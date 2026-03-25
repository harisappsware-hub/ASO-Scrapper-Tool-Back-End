"""
Database layer — SQLite for portability.
Stores app snapshots and enables change tracking.
"""
import sqlite3
import json
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Any

from keyword_engine import KeywordEngine

DB_PATH = "aso_tool.db"
kw_engine = KeywordEngine()


class Database:
    def __init__(self, path: str = DB_PATH):
        self.path = path
        self._init()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init(self):
        with self._conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS apps (
                    app_id      TEXT PRIMARY KEY,
                    title       TEXT,
                    developer   TEXT,
                    category    TEXT,
                    icon_url    TEXT,
                    first_seen  TEXT,
                    last_seen   TEXT
                );

                CREATE TABLE IF NOT EXISTS snapshots (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    app_id      TEXT NOT NULL,
                    version     TEXT,
                    title       TEXT,
                    short_desc  TEXT,
                    long_desc   TEXT,
                    rating      REAL,
                    installs    TEXT,
                    keywords_json TEXT,
                    content_hash  TEXT,
                    scraped_at    TEXT,
                    FOREIGN KEY (app_id) REFERENCES apps(app_id)
                );

                CREATE TABLE IF NOT EXISTS keyword_changes (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    app_id      TEXT,
                    from_snap   INTEGER,
                    to_snap     INTEGER,
                    change_type TEXT,
                    keyword     TEXT,
                    old_count   INTEGER,
                    new_count   INTEGER,
                    detected_at TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_snap_app ON snapshots(app_id);
                CREATE INDEX IF NOT EXISTS idx_changes_app ON keyword_changes(app_id);
            """)

    def save_and_compare(
        self,
        app_id: str,
        app_data: Dict,
        kw_data: Dict
    ) -> Dict:
        """Save snapshot and return changes vs previous."""
        now = datetime.utcnow().isoformat()

        # Build keyword freq map
        kw_freq = {
            item["keyword"]: item["count"]
            for item in kw_data.get("unigrams", [])
        }
        kw_json = json.dumps(kw_freq)

        # Content hash for change detection
        content = (
            (app_data.get("title") or "") +
            (app_data.get("long_description") or "")
        )
        content_hash = hashlib.md5(content.encode()).hexdigest()

        with self._conn() as conn:
            # Upsert app record
            conn.execute("""
                INSERT INTO apps (app_id, title, developer, category, icon_url, first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(app_id) DO UPDATE SET
                    title=excluded.title,
                    last_seen=excluded.last_seen
            """, (
                app_id,
                app_data.get("title", ""),
                app_data.get("developer", ""),
                app_data.get("category", ""),
                app_data.get("icon_url", ""),
                now, now
            ))

            # Get last snapshot
            row = conn.execute(
                "SELECT * FROM snapshots WHERE app_id=? ORDER BY id DESC LIMIT 1",
                (app_id,)
            ).fetchone()

            # Insert new snapshot
            cursor = conn.execute("""
                INSERT INTO snapshots
                    (app_id, version, title, short_desc, long_desc, rating, installs,
                     keywords_json, content_hash, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                app_id,
                app_data.get("version", ""),
                app_data.get("title", ""),
                app_data.get("short_description", ""),
                app_data.get("long_description", ""),
                app_data.get("rating"),
                app_data.get("installs", ""),
                kw_json,
                content_hash,
                now
            ))
            new_snap_id = cursor.lastrowid

            if not row:
                return {"is_first": True, "message": "First snapshot recorded."}

            # Compare
            if row["content_hash"] == content_hash:
                return {"is_first": False, "no_change": True, "message": "No changes detected."}

            old_kw_freq = json.loads(row["keywords_json"] or "{}")
            changes = kw_engine.compare_keyword_sets(old_kw_freq, kw_freq)

            # Persist changes
            for item in changes.get("added", []):
                conn.execute(
                    "INSERT INTO keyword_changes VALUES (NULL,?,?,?,?,?,?,?,?)",
                    (app_id, row["id"], new_snap_id, "added",
                     item["keyword"], 0, item["count"], now)
                )
            for item in changes.get("removed", []):
                conn.execute(
                    "INSERT INTO keyword_changes VALUES (NULL,?,?,?,?,?,?,?,?)",
                    (app_id, row["id"], new_snap_id, "removed",
                     item["keyword"], item["count"], 0, now)
                )
            for item in changes.get("changed", []):
                conn.execute(
                    "INSERT INTO keyword_changes VALUES (NULL,?,?,?,?,?,?,?,?)",
                    (app_id, row["id"], new_snap_id, "changed",
                     item["keyword"], item["old"], item["new"], now)
                )

            changes["from_version"] = row["version"]
            changes["from_date"]    = row["scraped_at"]
            changes["to_date"]      = now

            return changes

    def get_history(self, app_id: str) -> List[Dict]:
        """Get snapshot history with change summaries."""
        with self._conn() as conn:
            snaps = conn.execute(
                "SELECT id, version, title, rating, installs, scraped_at, content_hash "
                "FROM snapshots WHERE app_id=? ORDER BY id DESC LIMIT 20",
                (app_id,)
            ).fetchall()

            result = []
            for s in snaps:
                changes = conn.execute(
                    "SELECT change_type, COUNT(*) as cnt FROM keyword_changes "
                    "WHERE app_id=? AND to_snap=? GROUP BY change_type",
                    (app_id, s["id"])
                ).fetchall()

                change_summary = {r["change_type"]: r["cnt"] for r in changes}
                result.append({
                    "snapshot_id": s["id"],
                    "version":     s["version"],
                    "title":       s["title"],
                    "rating":      s["rating"],
                    "installs":    s["installs"],
                    "scraped_at":  s["scraped_at"],
                    "changes":     change_summary
                })

            return result

    def list_apps(self) -> List[Dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT a.*, COUNT(s.id) as snapshot_count "
                "FROM apps a LEFT JOIN snapshots s ON a.app_id=s.app_id "
                "GROUP BY a.app_id ORDER BY a.last_seen DESC"
            ).fetchall()
            return [dict(r) for r in rows]
