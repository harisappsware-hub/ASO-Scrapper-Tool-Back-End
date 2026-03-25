"""
Keyword Extraction Engine
Handles text cleaning, n-gram extraction, frequency/density analysis.
"""
import re
import math
from typing import Dict, List, Set, Tuple
from collections import Counter

# ─── STOPWORDS ─────────────────────────────────────────────
STOPWORDS: Set[str] = {
    "a","an","the","and","or","but","in","on","at","to","for",
    "of","with","by","from","is","are","was","were","be","been",
    "being","have","has","had","do","does","did","will","would",
    "could","should","may","might","can","this","that","these",
    "those","it","its","we","our","you","your","they","their",
    "he","his","she","her","i","my","me","us","all","any","each",
    "more","most","other","some","such","no","not","so","than",
    "then","there","what","which","who","how","when","where","why",
    "as","up","out","about","into","through","during","before",
    "after","above","below","between","app","apps","free","new",
    "get","just","now","use","using","used","make","made","also",
    "even","one","two","three","first","last","every","many","much",
    "own","same","too","very","well","way","also","only","both",
    "each","here","there","today","time","like","want","need",
    "support","please","help","download","available","version",
    "update","android","device","phone","tablet","mobile","google",
    "play","store","install","open","click","tap","press","go"
}

# ─── TEXT UTILITIES ────────────────────────────────────────

def clean_text(text: str) -> str:
    """Clean and normalize text."""
    if not text:
        return ""
    text = re.sub(r'<[^>]+>', ' ', text)          # remove HTML
    text = re.sub(r'[^\w\s\'-]', ' ', text)        # keep word chars, apostrophes, hyphens
    text = re.sub(r'\s+', ' ', text)               # normalize whitespace
    return text.lower().strip()


def tokenize(text: str) -> List[str]:
    """Tokenize cleaned text into words."""
    cleaned = clean_text(text)
    tokens = re.findall(r"\b[a-z][a-z0-9'-]*[a-z0-9]\b|\b[a-z]{2,}\b", cleaned)
    return tokens


def remove_stopwords(tokens: List[str]) -> List[str]:
    return [t for t in tokens if t not in STOPWORDS and len(t) >= 2]


def extract_ngrams(tokens: List[str], n: int) -> List[str]:
    """Extract n-grams from token list."""
    return [" ".join(tokens[i:i+n]) for i in range(len(tokens) - n + 1)]


def simple_stem(word: str) -> str:
    """Very lightweight stemmer (no NLTK dependency)."""
    suffixes = ["ing", "tion", "tions", "ness", "ment", "ments",
                "ly", "er", "ers", "est", "ed", "s"]
    for suffix in suffixes:
        if word.endswith(suffix) and len(word) - len(suffix) >= 3:
            return word[:-len(suffix)]
    return word


def keyword_relevance_score(kw: str, title: str, short_desc: str) -> float:
    """Score keyword by position/presence in high-value fields."""
    score = 0.0
    title_lower = title.lower()
    short_lower = short_desc.lower()
    kw_lower = kw.lower()

    if kw_lower in title_lower:
        score += 3.0
    if kw_lower in short_lower:
        score += 1.5
    # Longer keywords usually more specific / valuable
    score += len(kw.split()) * 0.5
    return score


# ─── MAIN ENGINE ──────────────────────────────────────────

class KeywordEngine:

    def analyze(
        self,
        title: str,
        short_desc: str,
        long_desc: str,
        language: str = "en"
    ) -> Dict:
        """Full keyword analysis pipeline."""

        # Combined text with field weights
        full_text = f"{title} {title} {title} {short_desc} {short_desc} {long_desc}"
        all_tokens_raw = tokenize(full_text)
        total_words = len(all_tokens_raw)

        # Individual field tokens (cleaned)
        title_tokens   = remove_stopwords(tokenize(title))
        short_tokens   = remove_stopwords(tokenize(short_desc))
        long_tokens    = remove_stopwords(tokenize(long_desc))
        clean_tokens   = remove_stopwords(all_tokens_raw)

        # ── Unigrams ──────────────────────────────────────
        unigram_counter = Counter(clean_tokens)
        unigrams = self._build_keyword_list(
            counter=unigram_counter,
            total=total_words,
            title=title,
            short_desc=short_desc,
            ngram=1,
            top_n=60
        )

        # ── Bigrams ───────────────────────────────────────
        bigram_tokens   = remove_stopwords(tokenize(long_desc + " " + short_desc + " " + title))
        bigram_counter  = Counter(extract_ngrams(bigram_tokens, 2))
        bigrams = self._build_keyword_list(
            counter=bigram_counter,
            total=total_words,
            title=title,
            short_desc=short_desc,
            ngram=2,
            top_n=40
        )

        # ── Trigrams ──────────────────────────────────────
        trigram_counter = Counter(extract_ngrams(bigram_tokens, 3))
        trigrams = self._build_keyword_list(
            counter=trigram_counter,
            total=total_words,
            title=title,
            short_desc=short_desc,
            ngram=3,
            top_n=25
        )

        # ── Stats ─────────────────────────────────────────
        unique_words = len(set(clean_tokens))
        avg_density  = sum(k["density"] for k in unigrams[:10]) / max(len(unigrams[:10]), 1)

        return {
            "unigrams":    unigrams,
            "bigrams":     bigrams,
            "trigrams":    trigrams,
            "stats": {
                "total_words":    total_words,
                "unique_words":   unique_words,
                "title_words":    len(title_tokens),
                "desc_words":     len(long_tokens),
                "avg_top10_density": round(avg_density, 3),
                "language":       language
            }
        }

    def _build_keyword_list(
        self,
        counter: Counter,
        total: int,
        title: str,
        short_desc: str,
        ngram: int,
        top_n: int = 50
    ) -> List[Dict]:
        """Build sorted keyword list with frequency and density."""
        results = []
        for kw, count in counter.most_common(top_n * 3):
            if count < 1:
                continue
            # Filter too-short bigrams/trigrams
            parts = kw.split()
            if any(len(p) < 2 for p in parts):
                continue

            density    = (count / max(total, 1)) * 100
            relevance  = keyword_relevance_score(kw, title, short_desc)
            stem       = simple_stem(kw) if ngram == 1 else kw

            results.append({
                "keyword":   kw,
                "stem":      stem,
                "count":     count,
                "density":   round(density, 4),
                "relevance": round(relevance, 2),
                "in_title":  kw.lower() in title.lower(),
                "in_short":  kw.lower() in short_desc.lower(),
                "ngram":     ngram,
            })

        # Sort by relevance then count
        results.sort(key=lambda x: (x["relevance"], x["count"]), reverse=True)
        return results[:top_n]

    def get_suggestions(self, keyword: str) -> List[str]:
        """Generate semantic keyword suggestions (rule-based expansion)."""
        kw = keyword.lower().strip()
        suggestions = []

        # Action prefixes
        actions = ["best", "free", "top", "how to", "easy", "fast", "smart",
                   "simple", "pro", "advanced", "ultimate"]
        # Modifier suffixes
        modifiers = ["app", "tool", "software", "manager", "tracker", "maker"]

        for action in actions:
            suggestions.append(f"{action} {kw}")
        for mod in modifiers:
            if mod not in kw:
                suggestions.append(f"{kw} {mod}")

        # Related terms (simple wordlist)
        related_map = {
            "task": ["to-do", "checklist", "planner", "schedule"],
            "photo": ["image", "picture", "camera", "gallery", "filter"],
            "music": ["audio", "song", "playlist", "player", "sound"],
            "fitness": ["workout", "exercise", "gym", "health", "calories"],
            "finance": ["budget", "money", "expense", "savings", "wallet"],
            "chat": ["messaging", "talk", "communicate", "social"],
            "video": ["movie", "stream", "watch", "clip", "player"],
            "map": ["navigation", "gps", "location", "directions", "travel"],
            "shop": ["store", "buy", "purchase", "market", "deals"],
            "note": ["notebook", "memo", "write", "journal", "diary"],
        }
        for base, related in related_map.items():
            if base in kw:
                suggestions.extend(related)

        # Deduplicate and exclude original
        seen = {kw}
        final = []
        for s in suggestions:
            if s not in seen:
                seen.add(s)
                final.append(s)

        return final[:20]

    def compare_keyword_sets(
        self,
        old_kws: Dict[str, int],
        new_kws: Dict[str, int]
    ) -> Dict:
        """Detect changes between two keyword snapshots."""
        old_set = set(old_kws.keys())
        new_set = set(new_kws.keys())

        added   = new_set - old_set
        removed = old_set - new_set
        common  = old_set & new_set

        changed = {}
        for kw in common:
            if old_kws[kw] != new_kws[kw]:
                changed[kw] = {
                    "old": old_kws[kw],
                    "new": new_kws[kw],
                    "delta": new_kws[kw] - old_kws[kw]
                }

        return {
            "added":   [{"keyword": k, "count": new_kws[k]} for k in sorted(added)],
            "removed": [{"keyword": k, "count": old_kws[k]} for k in sorted(removed)],
            "changed": [
                {"keyword": k, **v}
                for k, v in sorted(changed.items(), key=lambda x: abs(x[1]["delta"]), reverse=True)
            ],
            "added_count":   len(added),
            "removed_count": len(removed),
            "changed_count": len(changed),
        }
