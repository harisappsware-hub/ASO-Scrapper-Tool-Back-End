"""
ASO Keyword Intelligence Tool - FastAPI Backend
"""
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, List, Dict
import asyncio
import json
import io
import csv
import time
from datetime import datetime

from scraper import GooglePlayScraper
from keyword_engine import KeywordEngine
from database import Database
from competitor import CompetitorAnalyzer

app = FastAPI(title="ASO Keyword Intelligence API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db = Database()
scraper = GooglePlayScraper()
keyword_engine = KeywordEngine()
competitor_analyzer = CompetitorAnalyzer()


# ─── MODELS ───────────────────────────────────────
class AnalyzeRequest(BaseModel):
    url: str
    country: str = "us"
    language: str = "en"
    include_competitors: bool = True
    track_changes: bool = True


class CompetitorRequest(BaseModel):
    app_id: str
    keywords: List[str]
    country: str = "us"


# ─── ENDPOINTS ────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


@app.post("/api/analyze")
async def analyze_app(req: AnalyzeRequest):
    """Full ASO analysis pipeline for a Google Play app."""
    try:
        # 1. Scrape app data
        app_data = await scraper.scrape_app(req.url, req.country)
        if not app_data:
            raise HTTPException(status_code=404, detail="Could not fetch app data. Check the URL.")

        app_id = app_data["app_id"]

        # 2. Extract keywords
        kw_data = keyword_engine.analyze(
            title=app_data.get("title", ""),
            short_desc=app_data.get("short_description", ""),
            long_desc=app_data.get("long_description", ""),
            language=req.language
        )

        # 3. Estimate rankings (top keywords only to avoid rate limiting)
        top_kws = [k["keyword"] for k in kw_data["unigrams"][:15]]
        rankings = await scraper.estimate_rankings(top_kws, app_id, req.country)

        # 4. Difficulty scores
        difficulty = await scraper.estimate_difficulty(top_kws, req.country)

        # 5. Merge ranking + difficulty into keyword data
        rank_map = {r["keyword"]: r for r in rankings}
        diff_map = {d["keyword"]: d for d in difficulty}

        for item in kw_data["unigrams"]:
            kw = item["keyword"]
            item["rank"]        = rank_map.get(kw, {}).get("rank", None)
            item["rank_status"] = rank_map.get(kw, {}).get("status", "Not Checked")
            item["difficulty"]  = diff_map.get(kw, {}).get("score", 0)
            item["competitor_count"] = diff_map.get(kw, {}).get("competitor_count", 0)

        # 6. Store in DB + detect changes
        changes = {}
        if req.track_changes:
            changes = db.save_and_compare(app_id, app_data, kw_data)

        # 7. Competitor analysis
        competitors = []
        if req.include_competitors and top_kws:
            competitors = await competitor_analyzer.analyze(
                main_app_id=app_id,
                keywords=top_kws[:5],
                main_keywords=set(k["keyword"] for k in kw_data["unigrams"]),
                country=req.country
            )

        return {
            "success": True,
            "app": app_data,
            "keywords": kw_data,
            "rankings": rankings,
            "difficulty": difficulty,
            "changes": changes,
            "competitors": competitors,
            "analyzed_at": datetime.utcnow().isoformat(),
            "country": req.country
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/history/{app_id}")
async def get_history(app_id: str):
    """Get keyword change history for an app."""
    history = db.get_history(app_id)
    return {"app_id": app_id, "history": history}


@app.get("/api/apps")
async def list_apps():
    """List all tracked apps."""
    apps = db.list_apps()
    return {"apps": apps}


@app.post("/api/export/csv")
async def export_csv(data: dict):
    """Export keyword data as CSV."""
    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow(["Keyword", "Type", "Frequency", "Density %", "Rank", "Difficulty", "Competitors"])

    keywords = data.get("keywords", {})
    for kw_type in ["unigrams", "bigrams", "trigrams"]:
        for item in keywords.get(kw_type, []):
            writer.writerow([
                item.get("keyword", ""),
                kw_type.rstrip("s"),
                item.get("count", 0),
                round(item.get("density", 0), 3),
                item.get("rank", "N/A"),
                item.get("difficulty", 0),
                item.get("competitor_count", 0)
            ])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=aso_report.csv"}
    )


@app.get("/api/suggestions/{app_id}")
async def keyword_suggestions(app_id: str, keyword: str):
    """Get semantic keyword suggestions."""
    suggestions = keyword_engine.get_suggestions(keyword)
    return {"keyword": keyword, "suggestions": suggestions}
