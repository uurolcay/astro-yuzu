import json
import math
import re
from hashlib import sha256


WORD_RE = re.compile(r"[A-Za-z0-9_ÇĞİÖŞÜçğıöşü'-]+", re.UNICODE)


def chunk_text(text, *, max_chars=520, overlap=80):
    raw = str(text or "").strip()
    if not raw:
        return []
    blocks = [part.strip() for part in re.split(r"\n\s*\n", raw) if part.strip()]
    chunks = []
    current = ""
    for block in blocks:
        candidate = f"{current}\n\n{block}".strip() if current else block
        if len(candidate) <= max_chars:
            current = candidate
            continue
        if current:
            chunks.append(current)
        if len(block) <= max_chars:
            current = block
            continue
        start = 0
        while start < len(block):
            end = min(start + max_chars, len(block))
            piece = block[start:end].strip()
            if piece:
                chunks.append(piece)
            if end >= len(block):
                break
            start = max(end - overlap, start + 1)
        current = ""
    if current:
        chunks.append(current)
    return chunks


def _tokens(text):
    return [token.lower() for token in WORD_RE.findall(str(text or ""))]


def generate_embedding(text, *, dims=48):
    vector = [0.0] * dims
    tokens = _tokens(text)
    if not tokens:
        return vector
    for token in tokens:
        digest = sha256(token.encode("utf-8")).digest()
        for index in range(dims):
            vector[index] += digest[index % len(digest)] / 255.0
    norm = math.sqrt(sum(value * value for value in vector)) or 1.0
    return [round(value / norm, 6) for value in vector]


def cosine_similarity(left, right):
    if not left or not right or len(left) != len(right):
        return 0.0
    dot = sum(a * b for a, b in zip(left, right))
    left_norm = math.sqrt(sum(a * a for a in left)) or 1.0
    right_norm = math.sqrt(sum(b * b for b in right)) or 1.0
    return dot / (left_norm * right_norm)


def serialize_embedding(vector):
    return json.dumps(list(vector or []), ensure_ascii=False)


def deserialize_embedding(value):
    if isinstance(value, list):
        return value
    try:
        return list(json.loads(value or "[]"))
    except Exception:
        return []
