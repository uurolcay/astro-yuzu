from pathlib import Path

from dotenv import load_dotenv

from services import retrieval_service
from agent_pipeline import (
    AIConfigurationError,
    AIServiceError,
    AgentResult,
    build_structured_payload,
    generate_interpretation,
    run_agent_pipeline,
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
    return payload


__all__ = [
    "AIConfigurationError",
    "AIServiceError",
    "AgentResult",
    "_augment_payload_with_knowledge",
    "build_structured_payload",
    "generate_interpretation",
    "run_agent_pipeline",
]
