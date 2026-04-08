"""Life-domain mapping for prioritized interpretation signals."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

TAG_TO_DOMAINS = {
    "career": ["career", "money"],
    "status": ["career"],
    "work": ["career", "inner_state"],
    "money": ["money"],
    "resources": ["money"],
    "relationships": ["relationships"],
    "partnerships": ["relationships"],
    "romance": ["relationships", "growth"],
    "inner_state": ["inner_state"],
    "security": ["inner_state", "relationships"],
    "spirituality": ["growth", "inner_state"],
    "growth": ["growth"],
    "wisdom": ["growth"],
    "transformation": ["growth", "inner_state"],
    "stress": ["inner_state", "career"],
    "pressure": ["career", "inner_state"],
}

HOUSE_TO_DOMAINS = {
    2: ["money"],
    4: ["inner_state"],
    7: ["relationships"],
    8: ["growth", "inner_state"],
    10: ["career"],
    11: ["money", "career"],
    12: ["growth", "inner_state"],
}

DEFAULT_DOMAINS = ["growth"]


def map_signals_to_domains(prioritized_signals: list[dict[str, Any]]) -> dict[str, Any]:
    domain_scores: dict[str, float] = defaultdict(float)
    domain_signals: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for signal in prioritized_signals:
        domains = _domains_for_signal(signal)
        for domain in domains[:2]:
            domain_scores[domain] += float(signal.get("score", 0.0))
            domain_signals[domain].append(signal)

    ordered_scores = sorted(domain_scores.items(), key=lambda item: item[1], reverse=True)
    return {
        "domain_scores": {domain: round(score, 4) for domain, score in ordered_scores},
        "domain_signals": {
            domain: sorted(signals, key=lambda item: item.get("score", 0.0), reverse=True)[:3]
            for domain, signals in domain_signals.items()
        },
    }


def _domains_for_signal(signal: dict[str, Any]) -> list[str]:
    domains = []
    for tag in signal.get("tags", []):
        for domain in TAG_TO_DOMAINS.get(tag, []):
            if domain not in domains:
                domains.append(domain)
    for domain in HOUSE_TO_DOMAINS.get(signal.get("house"), []):
        if domain not in domains:
            domains.append(domain)
    return domains or list(DEFAULT_DOMAINS)
