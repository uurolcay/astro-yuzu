from pathlib import Path

from dotenv import load_dotenv

from services import retrieval_service
from agent_pipeline import (
    AIConfigurationError,
    AIServiceError,
    AgentResult,
    build_structured_payload,
    generate_interpretation as _pipeline_generate_interpretation,
    run_agent_pipeline as _pipeline_run_agent_pipeline,
)


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


def _augment_payload_with_knowledge(payload, db=None):
    if not isinstance(payload, dict):
        return payload
    try:
        knowledge_context = retrieval_service.build_prompt_knowledge_context(payload, db=db)
    except Exception:
        return payload
    if isinstance(knowledge_context, dict):
        payload["knowledge_context"] = knowledge_context
        chunk_ids = [cid for cid in (knowledge_context.get("chunk_ids") or []) if cid is not None]
        payload["_used_chunk_ids"] = chunk_ids
        payload["_knowledge_trace"] = {
            "used_chunk_ids": chunk_ids,
            "matched_entities": knowledge_context.get("matched_entities") or [],
            "missing_entities": knowledge_context.get("missing_entities") or [],
            "retrieval_queries": knowledge_context.get("retrieval_queries") or [],
            "missing_queries": knowledge_context.get("missing_queries") or [],
            "source_documents_used": knowledge_context.get("source_documents_used") or [],
            "source_coverage_score": knowledge_context.get("source_coverage_score"),
            "no_source_available": knowledge_context.get("no_source_available"),
        }
    return payload


def run_agent_pipeline(data: dict | None = None, **kwargs) -> dict:
    payload = _augment_payload_with_knowledge(dict(data or {}), db=kwargs.pop("db", None))
    return _pipeline_run_agent_pipeline(payload, **kwargs)


def generate_interpretation(data: dict | None = None, **kwargs) -> str:
    payload = _augment_payload_with_knowledge(dict(data or {}), db=kwargs.pop("db", None))
    return _pipeline_generate_interpretation(payload, **kwargs)


__all__ = [
    "AIConfigurationError",
    "AIServiceError",
    "AgentResult",
    "_augment_payload_with_knowledge",
    "build_structured_payload",
    "generate_interpretation",
    "run_agent_pipeline",
]
