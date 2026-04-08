import re


DOMAIN_CATEGORY_MAP = {
    "growth": {"foundations": 3, "life-guidance": 3},
    "career": {"life-guidance": 4},
    "relationships": {"life-guidance": 4},
    "timing": {"timing": 5},
    "money": {"life-guidance": 3},
    "inner_state": {"foundations": 2, "life-guidance": 2},
}

PLANET_KEYWORDS = {
    "jupiter": {"jupiter", "growth", "expansion"},
    "saturn": {"saturn", "timing", "pressure", "discipline"},
    "moon": {"moon", "emotional", "inner", "inner state"},
}

ANCHOR_KEYWORDS = {
    "growth": {"growth", "expansion", "development"},
    "career": {"career", "work", "vocation", "visibility"},
    "relationship": {"relationship", "connection", "partnership", "love"},
    "timing": {"timing", "period", "cycle", "dasha", "mahadasha"},
    "saturn": {"saturn", "pressure", "discipline", "structure"},
    "jupiter": {"jupiter", "growth", "meaning"},
    "moon": {"moon", "emotional", "inner", "feeling"},
}


def _tokenize(value):
    return {token for token in re.findall(r"[a-z0-9]+", str(value or "").lower()) if token}


def _article_text_tokens(article):
    return _tokenize(article.get("title", "")) | _tokenize(article.get("excerpt", "")) | _tokenize(article.get("category", ""))


def match_articles_to_result(prioritized_signals, anchors, domain_scores, articles):
    domain_scores = domain_scores or {}
    dominant_domain = max(domain_scores.items(), key=lambda item: item[1])[0] if domain_scores else None
    anchor_tokens = set()
    for anchor in anchors or []:
        anchor_tokens |= _tokenize(anchor.get("title", ""))
        for keyword, synonyms in ANCHOR_KEYWORDS.items():
            if keyword in str(anchor.get("title", "")).lower():
                anchor_tokens |= synonyms

    planet_tokens = set()
    for signal in prioritized_signals or []:
        planet = str(signal.get("planet") or "").strip().lower()
        if planet in PLANET_KEYWORDS:
            planet_tokens |= PLANET_KEYWORDS[planet]

    scored = []
    for article in articles or []:
        if not article.get("is_published", True):
            continue
        article_tokens = _article_text_tokens(article)
        category = str(article.get("category") or "").strip().lower()
        score = 0

        for domain, weight in sorted((domain_scores or {}).items(), key=lambda item: item[1], reverse=True):
            score += DOMAIN_CATEGORY_MAP.get(domain, {}).get(category, 0)
            if domain.replace("_", " ") in " ".join(article_tokens):
                score += 1
            if domain in article_tokens:
                score += 2

        if dominant_domain:
            score += DOMAIN_CATEGORY_MAP.get(dominant_domain, {}).get(category, 0)

        keyword_hits = len(article_tokens & anchor_tokens)
        score += keyword_hits * 2

        planet_hits = len(article_tokens & planet_tokens)
        score += planet_hits * 2

        if article.get("published_at"):
            score += 1

        if score <= 0:
            continue

        scored.append(
            {
                "title": article.get("title"),
                "slug": article.get("slug"),
                "excerpt": article.get("excerpt"),
                "category": category,
                "score": score,
            }
        )

    scored.sort(key=lambda item: (-item["score"], item["title"] or "", item["slug"] or ""))
    return scored[:3]
