import os
import logging
from datetime import datetime
from pathlib import Path

from sqlalchemy import Boolean, Column, DateTime, Float, ForeignKey, Integer, Numeric, String, Text, UniqueConstraint, create_engine, event, inspect, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_SQLITE_PATH = BASE_DIR / "astro_logic.db"
logger = logging.getLogger(__name__)

DB_DISCONNECT_PATTERNS = (
    "SSL error: decryption failed or bad record mac",
    "SSL SYSCALL",
    "server closed the connection unexpectedly",
    "connection already closed",
    "terminating connection",
    "EOF detected",
)


def _int_env(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return int(default)


def resolve_database_url(raw_value: str | None = None) -> str:
    raw = str(raw_value if raw_value is not None else os.getenv("DATABASE_URL", "")).strip()
    if raw.startswith("postgres://"):
        raw = "postgresql://" + raw[len("postgres://") :]
    if raw:
        return raw
    return f"sqlite:///{DEFAULT_SQLITE_PATH.as_posix()}"


SQLALCHEMY_DATABASE_URL = resolve_database_url()


def _is_postgresql_url(database_url: str) -> bool:
    return str(database_url or "").split(":", 1)[0].split("+", 1)[0].lower() == "postgresql"


def get_postgresql_pool_settings() -> dict:
    return {
        "pool_pre_ping": True,
        "pool_recycle": _int_env("DB_POOL_RECYCLE_SECONDS", "300"),
        "pool_timeout": _int_env("DB_POOL_TIMEOUT_SECONDS", "30"),
        "pool_size": _int_env("DB_POOL_SIZE", "5"),
        "max_overflow": _int_env("DB_MAX_OVERFLOW", "5"),
    }


def build_engine_kwargs(database_url: str) -> dict:
    if str(database_url or "").startswith("sqlite"):
        return {"connect_args": {"check_same_thread": False}}
    if _is_postgresql_url(database_url):
        return get_postgresql_pool_settings()
    return {}


def is_db_disconnect_error(exc) -> bool:
    message = str(exc or "")
    lowered = message.lower()
    return any(pattern.lower() in lowered for pattern in DB_DISCONNECT_PATTERNS)


def _database_url_uses_internal_hint(value: str) -> bool:
    raw = str(value or "").strip().lower()
    if not raw:
        return False
    host_part = raw.split("@", 1)[-1].split("/", 1)[0].split(":", 1)[0]
    return "internal" in host_part or ".svc" in host_part


_ENGINE_KWARGS = build_engine_kwargs(SQLALCHEMY_DATABASE_URL)
engine = create_engine(SQLALCHEMY_DATABASE_URL, **_ENGINE_KWARGS)


if _is_postgresql_url(SQLALCHEMY_DATABASE_URL):
    @event.listens_for(engine, "handle_error")
    def _mark_disconnect_errors(context):
        original_exception = getattr(context, "original_exception", None)
        sqlalchemy_exception = getattr(context, "sqlalchemy_exception", None)
        if is_db_disconnect_error(original_exception) or is_db_disconnect_error(sqlalchemy_exception):
            try:
                context.is_disconnect = True
            except Exception:
                pass
            logger.warning("Database disconnect detected; SQLAlchemy will recycle connection")


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def _mask_database_url(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "(missing)"
    if "://" not in raw:
        return raw
    scheme, rest = raw.split("://", 1)
    if "@" in rest:
        creds, tail = rest.rsplit("@", 1)
        if ":" in creds:
            user = creds.split(":", 1)[0]
            return f"{scheme}://{user}:***@{tail}"
        return f"{scheme}://***@{tail}"
    return raw


def get_engine_diagnostics():
    url_obj = engine.url
    dialect = str(getattr(url_obj, "drivername", "") or "").split("+", 1)[0]
    database_value = str(getattr(url_obj, "database", "") or "").strip()
    is_postgresql = dialect == "postgresql"
    pool_settings = get_postgresql_pool_settings()
    sqlite_file_path = ""
    sqlite_file_exists = False
    sqlite_file_size = 0
    if dialect == "sqlite" and database_value and database_value != ":memory:":
        sqlite_path = Path(database_value)
        if not sqlite_path.is_absolute():
            sqlite_path = (BASE_DIR / sqlite_path).resolve()
        sqlite_file_path = str(sqlite_path)
        sqlite_file_exists = sqlite_path.exists()
        sqlite_file_size = sqlite_path.stat().st_size if sqlite_file_exists else 0
    return {
        "database_url_masked": _mask_database_url(SQLALCHEMY_DATABASE_URL),
        "db_dialect": dialect or "unknown",
        "sqlite_file_path": sqlite_file_path or None,
        "sqlite_file_exists": sqlite_file_exists,
        "sqlite_file_size": sqlite_file_size,
        "database_url_missing": not bool(str(os.getenv("DATABASE_URL", "")).strip()),
        "is_in_memory": dialect == "sqlite" and database_value == ":memory:",
        "db_pool_pre_ping": bool(pool_settings["pool_pre_ping"]) if is_postgresql else False,
        "db_pool_recycle_seconds": pool_settings["pool_recycle"] if is_postgresql else None,
        "db_pool_size": pool_settings["pool_size"] if is_postgresql else None,
        "db_max_overflow": pool_settings["max_overflow"] if is_postgresql else None,
        "database_url_uses_internal_hint": _database_url_uses_internal_hint(str(os.getenv("DATABASE_URL", "") or SQLALCHEMY_DATABASE_URL)),
        "db_disconnect_patterns_enabled": True,
    }

class UserRecord(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    birth_date = Column(String, index=True)
    city = Column(String, index=True)
    raw_birth_place_input = Column(String, nullable=True)
    normalized_birth_place = Column(String, nullable=True)
    lat = Column(Float)
    lon = Column(Float)
    timezone = Column(String, nullable=True)
    geocode_provider = Column(String, nullable=True)
    geocode_confidence = Column(Float, nullable=True)
    natal_data_json = Column(Text)


class AppUser(Base):
    __tablename__ = "app_users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    password_hash = Column(String, nullable=False)
    name = Column(String, nullable=True)
    plan_code = Column(String, default="free", nullable=False)
    is_admin = Column(Boolean, default=False, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    subscription_status = Column(String, default="active", nullable=False)
    plan_started_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    plan_expires_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    profiles = relationship("UserProfile", back_populates="user", cascade="all, delete-orphan")
    reports = relationship("GeneratedReport", back_populates="user", cascade="all, delete-orphan")


class UserProfile(Base):
    __tablename__ = "user_profiles"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("app_users.id"), nullable=False, index=True)
    profile_name = Column(String, nullable=True)
    full_name = Column(String, nullable=True)
    birth_date = Column(String, nullable=False)
    birth_time = Column(String, nullable=False)
    birth_city = Column(String, nullable=False)
    birth_country = Column(String, nullable=True)
    raw_birth_place_input = Column(String, nullable=True)
    normalized_birth_place = Column(String, nullable=True)
    lat = Column(Float, nullable=True)
    lon = Column(Float, nullable=True)
    timezone = Column(String, nullable=True)
    geocode_provider = Column(String, nullable=True)
    geocode_confidence = Column(Float, nullable=True)
    natal_data_json = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    user = relationship("AppUser", back_populates="profiles")
    reports = relationship("GeneratedReport", back_populates="profile")


class GeneratedReport(Base):
    __tablename__ = "generated_reports"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("app_users.id"), nullable=False, index=True)
    profile_id = Column(Integer, ForeignKey("user_profiles.id"), nullable=True, index=True)
    report_type = Column(String, default="preview", nullable=False)
    title = Column(String, nullable=True)
    full_name = Column(String, nullable=True)
    birth_date = Column(String, nullable=True)
    birth_time = Column(String, nullable=True)
    birth_city = Column(String, nullable=True)
    birth_country = Column(String, nullable=True)
    raw_birth_place_input = Column(String, nullable=True)
    normalized_birth_place = Column(String, nullable=True)
    lat = Column(Float, nullable=True)
    lon = Column(Float, nullable=True)
    timezone = Column(String, nullable=True)
    geocode_provider = Column(String, nullable=True)
    geocode_confidence = Column(Float, nullable=True)
    calculation_metadata_json = Column(Text, nullable=True)
    interpretation_context_json = Column(Text, nullable=True)
    result_payload_json = Column(Text, nullable=True)
    ai_interpretation_text = Column(Text, nullable=True)
    access_state = Column(String, default="preview", nullable=False)
    is_paid = Column(Boolean, default=False, nullable=False)
    unlocked_at = Column(DateTime, nullable=True)
    payment_reference = Column(String, nullable=True)
    pdf_ready = Column(Boolean, default=False, nullable=False)
    delivered_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    user = relationship("AppUser", back_populates="reports")
    profile = relationship("UserProfile", back_populates="reports")


class ServiceOrder(Base):
    __tablename__ = "service_orders"

    id = Column(Integer, primary_key=True, index=True)
    order_token = Column(String, unique=True, nullable=False, index=True)
    public_token = Column(String, unique=True, nullable=True, index=True)
    service_type = Column(String, nullable=False, index=True)
    product_type = Column(String, nullable=False, index=True)
    bundle_type = Column(String, nullable=True, index=True)
    included_products_json = Column(Text, nullable=True)
    bundle_price = Column(Numeric(10, 2), nullable=True)
    status = Column(String, nullable=False, default="initiated", index=True)
    customer_name = Column(String, nullable=True)
    customer_email = Column(String, nullable=True, index=True)
    birth_date = Column(String, nullable=True)
    birth_time = Column(String, nullable=True)
    birth_place = Column(String, nullable=True)
    user_lang = Column(String, nullable=True, default="tr", index=True)
    optional_note = Column(Text, nullable=True)
    amount = Column(Numeric(10, 2), nullable=True)
    amount_label = Column(String, nullable=True)
    currency = Column(String, nullable=True, default="TRY")
    provider_name = Column(String, nullable=True, index=True)
    provider_token = Column(String, nullable=True, index=True)
    provider_payment_id = Column(String, unique=True, nullable=True, index=True)
    provider_transaction_id = Column(String, nullable=True, index=True)
    provider_conversation_id = Column(String, nullable=True, index=True)
    payment_provider = Column(String, nullable=True)
    payment_session_id = Column(String, nullable=True, index=True)
    payment_reference = Column(String, nullable=True, index=True)
    payment_verified_at = Column(DateTime, nullable=True)
    paid_at = Column(DateTime, nullable=True)
    fraud_status = Column(String, nullable=True)
    calendly_event_uri = Column(String, nullable=True)
    calendly_invitee_uri = Column(String, nullable=True, index=True)
    calendly_event_type_uri = Column(String, nullable=True)
    calendly_status = Column(String, nullable=True, index=True)
    calendly_canceled_at = Column(DateTime, nullable=True)
    booking_source = Column(String, nullable=True)
    scheduled_start = Column(DateTime, nullable=True)
    scheduled_end = Column(DateTime, nullable=True)
    payload_json = Column(Text, nullable=True)
    draft_status = Column(String, nullable=True)
    ai_draft_status = Column(String, nullable=True, index=True)
    draft_sent_at = Column(DateTime, nullable=True)
    ai_draft_text = Column(Text, nullable=True)
    ai_draft_created_at = Column(DateTime, nullable=True)
    ai_draft_version = Column(Integer, nullable=True, default=1)
    pdf_status = Column(String, nullable=True, index=True)
    final_pdf_path = Column(String, nullable=True)
    customer_confirmation_sent_at = Column(DateTime, nullable=True)
    last_task_error = Column(Text, nullable=True)
    internal_notes = Column(Text, nullable=True)
    admin_note = Column(Text, nullable=True)
    review_started_at = Column(DateTime, nullable=True)
    ready_to_send_at = Column(DateTime, nullable=True)
    delivered_at = Column(DateTime, nullable=True)
    confirmed_at = Column(DateTime, nullable=True)
    prepared_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    refund_status = Column(String, nullable=True, index=True)
    refund_amount = Column(Numeric(10, 2), nullable=True)
    refunded_at = Column(DateTime, nullable=True)
    refund_reason = Column(Text, nullable=True)
    provider_refund_id = Column(String, nullable=True, index=True)
    refund_provider_status = Column(String, nullable=True)
    cancellation_reason = Column(Text, nullable=True)
    cancelled_at = Column(DateTime, nullable=True)
    no_show_at = Column(DateTime, nullable=True)
    reconciliation_notes = Column(Text, nullable=True)
    is_gift = Column(Boolean, default=False, nullable=False)
    gift_recipient_name = Column(String, nullable=True)
    gift_recipient_email = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class InternalProfile(Base):
    __tablename__ = "internal_profiles"

    id = Column(Integer, primary_key=True, index=True)
    full_name = Column(String, nullable=True, index=True)
    gender = Column(String, nullable=True)
    birth_date = Column(String, nullable=False, index=True)
    birth_time = Column(String, nullable=False)
    birth_place_label = Column(String, nullable=False)
    birth_country = Column(String, nullable=True)
    birth_city = Column(String, nullable=True)
    birth_lat = Column(Float, nullable=True)
    birth_lng = Column(Float, nullable=True)
    birth_timezone = Column(String, nullable=True)
    notes = Column(Text, nullable=True)
    is_favorite = Column(Boolean, default=False, nullable=False, index=True)
    created_by_user_id = Column(Integer, ForeignKey("app_users.id"), nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    last_generated_at = Column(DateTime, nullable=True)

    created_by = relationship("AppUser")
    interpretations = relationship("InternalInterpretation", foreign_keys="InternalInterpretation.profile_id", back_populates="profile", cascade="all, delete-orphan")


class InternalInterpretation(Base):
    __tablename__ = "internal_interpretations"

    id = Column(Integer, primary_key=True, index=True)
    profile_id = Column(Integer, ForeignKey("internal_profiles.id"), nullable=True, index=True)
    secondary_profile_id = Column(Integer, ForeignKey("internal_profiles.id"), nullable=True, index=True)
    report_type = Column(String, nullable=False, index=True)
    input_payload_json = Column(Text, nullable=False)
    render_context_json = Column(Text, nullable=True)
    interpretation_text = Column(Text, nullable=False)
    pdf_path = Column(String, nullable=True)
    generation_mode = Column(String, default="quick", nullable=False, index=True)
    created_by_user_id = Column(Integer, ForeignKey("app_users.id"), nullable=True, index=True)
    prompt_version = Column(String, nullable=True)
    model_name = Column(String, nullable=True)
    pipeline_version = Column(String, nullable=True)
    generation_duration_s = Column(Float, nullable=True)
    signal_summary_json = Column(Text, nullable=True)
    used_chunk_ids_json = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    profile = relationship("InternalProfile", foreign_keys=[profile_id], back_populates="interpretations")
    secondary_profile = relationship("InternalProfile", foreign_keys=[secondary_profile_id])
    created_by = relationship("AppUser")


# Migration: interpretation_reviews, prompt_insights tables
# Added: 2026-04-29
# New nullable columns on internal_interpretations:
#   prompt_version, model_name, pipeline_version,
#   generation_duration_s, signal_summary_json
class InterpretationReview(Base):
    __tablename__ = "interpretation_reviews"

    id = Column(Integer, primary_key=True, index=True)
    interpretation_id = Column(Integer, ForeignKey("internal_interpretations.id"), nullable=False, index=True)
    rating_overall = Column(Integer, nullable=True)
    rating_clarity = Column(Integer, nullable=True)
    rating_depth = Column(Integer, nullable=True)
    rating_accuracy_feel = Column(Integer, nullable=True)
    rating_actionability = Column(Integer, nullable=True)
    rating_tone = Column(Integer, nullable=True)
    admin_feedback = Column(Text, nullable=True)
    strong_sections = Column(Text, nullable=True)
    weak_sections = Column(Text, nullable=True)
    improvement_notes = Column(Text, nullable=True)
    safety_flags = Column(Text, nullable=True)
    status = Column(String, default="draft", nullable=False, index=True)
    version_number = Column(Integer, default=1, nullable=False)
    parent_version_id = Column(Integer, nullable=True)
    quality_eval_json = Column(Text, nullable=True)
    missing_entities_json = Column(Text, nullable=True)
    section_coverage_json = Column(Text, nullable=True)
    reviewed_by_user_id = Column(Integer, ForeignKey("app_users.id"), nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    interpretation = relationship("InternalInterpretation", backref="reviews")
    reviewed_by = relationship("AppUser")


class PromptInsight(Base):
    __tablename__ = "prompt_insights"

    id = Column(Integer, primary_key=True, index=True)
    report_type = Column(String, nullable=True, index=True)
    insight_type = Column(String, nullable=False, index=True)
    title = Column(String, nullable=False)
    body = Column(Text, nullable=False)
    source_review_ids = Column(Text, nullable=True)
    prompt_version_ref = Column(String, nullable=True)
    created_by_user_id = Column(Integer, ForeignKey("app_users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    created_by = relationship("AppUser")


class InternalChatSession(Base):
    __tablename__ = "internal_chat_sessions"

    id = Column(Integer, primary_key=True, index=True)
    profile_id = Column(Integer, ForeignKey("internal_profiles.id"), nullable=True, index=True)
    secondary_profile_id = Column(Integer, ForeignKey("internal_profiles.id"), nullable=True, index=True)
    title = Column(String, nullable=True)
    report_type = Column(String, nullable=True, index=True)
    mode = Column(String, default="grounded", nullable=False, index=True)
    created_by_user_id = Column(Integer, ForeignKey("app_users.id"), nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    profile = relationship("InternalProfile", foreign_keys=[profile_id])
    secondary_profile = relationship("InternalProfile", foreign_keys=[secondary_profile_id])
    created_by = relationship("AppUser")
    messages = relationship("InternalChatMessage", back_populates="session", cascade="all, delete-orphan")


class InternalChatMessage(Base):
    __tablename__ = "internal_chat_messages"

    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(Integer, ForeignKey("internal_chat_sessions.id"), nullable=False, index=True)
    role = Column(String, nullable=False, index=True)
    message_text = Column(Text, nullable=False)
    tool_payload_json = Column(Text, nullable=True)
    context_snapshot_json = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    session = relationship("InternalChatSession", back_populates="messages")


class AiBehaviorRuleSet(Base):
    __tablename__ = "ai_behavior_rule_sets"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, index=True)
    description = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True, nullable=False, index=True)
    created_by_user_id = Column(Integer, ForeignKey("app_users.id"), nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    created_by = relationship("AppUser")
    rules = relationship("AiBehaviorRule", back_populates="rule_set", cascade="all, delete-orphan")


class AiBehaviorRule(Base):
    __tablename__ = "ai_behavior_rules"

    id = Column(Integer, primary_key=True, index=True)
    rule_set_id = Column(Integer, ForeignKey("ai_behavior_rule_sets.id"), nullable=False, index=True)
    category = Column(String, nullable=False, index=True)
    section = Column(String, nullable=True, index=True)
    code = Column(String, nullable=False, unique=True, index=True)
    title = Column(String, nullable=False)
    content = Column(Text, nullable=False)
    is_enabled = Column(Boolean, default=True, nullable=False, index=True)
    is_editable = Column(Boolean, default=True, nullable=False)
    sort_order = Column(Integer, default=0, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    rule_set = relationship("AiBehaviorRuleSet", back_populates="rules")


class Customer(Base):
    __tablename__ = "accounting_customers"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=True)
    email = Column(String, nullable=False, unique=True, index=True)
    tax_id = Column(String, nullable=True)
    customer_type = Column(String, nullable=True, default="individual")
    identity_number = Column(String, nullable=True)
    company_name = Column(String, nullable=True)
    tax_office = Column(String, nullable=True)
    billing_address = Column(Text, nullable=True)
    city = Column(String, nullable=True)
    country = Column(String, nullable=True, default="TR")
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class Transaction(Base):
    __tablename__ = "accounting_transactions"

    id = Column(Integer, primary_key=True, index=True)
    service_order_id = Column(Integer, ForeignKey("service_orders.id"), unique=True, nullable=True, index=True)
    customer_id = Column(Integer, ForeignKey("accounting_customers.id"), nullable=True, index=True)
    provider_payment_id = Column(String, nullable=True, index=True)
    product_type = Column(String, nullable=True, index=True)
    service_type = Column(String, nullable=True, index=True)
    currency = Column(String, nullable=True, default="TRY")
    gross_amount = Column(Numeric(12, 2), default=0, nullable=False)
    commission_amount = Column(Numeric(12, 2), default=0, nullable=False)
    refunded_amount = Column(Numeric(12, 2), default=0, nullable=False)
    net_amount = Column(Numeric(12, 2), default=0, nullable=False)
    payment_status = Column(String, default="paid", nullable=False, index=True)
    invoice_status = Column(String, default="uninvoiced", nullable=False, index=True)
    paid_at = Column(DateTime, nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    order = relationship("ServiceOrder")
    customer = relationship("Customer")


class Invoice(Base):
    __tablename__ = "accounting_invoices"

    id = Column(Integer, primary_key=True, index=True)
    invoice_number = Column(String, unique=True, nullable=True, index=True)
    transaction_id = Column(Integer, ForeignKey("accounting_transactions.id"), nullable=True, index=True)
    customer_id = Column(Integer, ForeignKey("accounting_customers.id"), nullable=True, index=True)
    status = Column(String, default="draft", nullable=False, index=True)
    issue_date = Column(DateTime, nullable=True, index=True)
    due_date = Column(DateTime, nullable=True)
    subtotal = Column(Numeric(12, 2), default=0, nullable=False)
    vat_amount = Column(Numeric(12, 2), default=0, nullable=False)
    total_amount = Column(Numeric(12, 2), default=0, nullable=False)
    currency = Column(String, nullable=True, default="TRY")
    notes = Column(Text, nullable=True)
    pdf_path = Column(String, nullable=True)
    pdf_status = Column(String, default="not_generated", nullable=False, index=True)
    pdf_generated_at = Column(DateTime, nullable=True)
    pdf_error_message = Column(Text, nullable=True)
    send_status = Column(String, default="not_sent", nullable=False, index=True)
    sent_at = Column(DateTime, nullable=True)
    sent_to_email = Column(String, nullable=True)
    send_error_message = Column(Text, nullable=True)
    last_send_attempt_at = Column(DateTime, nullable=True)
    cancelled_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    transaction = relationship("Transaction")
    customer = relationship("Customer")
    items = relationship("InvoiceItem", back_populates="invoice", cascade="all, delete-orphan")


class InvoiceItem(Base):
    __tablename__ = "accounting_invoice_items"

    id = Column(Integer, primary_key=True, index=True)
    invoice_id = Column(Integer, ForeignKey("accounting_invoices.id"), nullable=False, index=True)
    description = Column(String, nullable=False)
    quantity = Column(Numeric(10, 2), default=1, nullable=False)
    unit_price = Column(Numeric(12, 2), default=0, nullable=False)
    vat_rate = Column(Numeric(5, 2), default=20, nullable=False)
    line_total = Column(Numeric(12, 2), default=0, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    invoice = relationship("Invoice", back_populates="items")


class Expense(Base):
    __tablename__ = "accounting_expenses"

    id = Column(Integer, primary_key=True, index=True)
    expense_date = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    supplier = Column(String, nullable=True, index=True)
    category = Column(String, nullable=True, index=True)
    description = Column(Text, nullable=True)
    amount = Column(Numeric(12, 2), default=0, nullable=False)
    vat_amount = Column(Numeric(12, 2), default=0, nullable=False)
    currency = Column(String, nullable=True, default="TRY")
    receipt_path = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class TaxRate(Base):
    __tablename__ = "accounting_tax_rates"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, unique=True, index=True)
    rate_percent = Column(Numeric(5, 2), default=0, nullable=False)
    tax_type = Column(String, nullable=False, index=True)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class TaxPeriod(Base):
    __tablename__ = "accounting_tax_periods"

    id = Column(Integer, primary_key=True, index=True)
    period_key = Column(String, nullable=False, unique=True, index=True)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)
    estimated_vat = Column(Numeric(12, 2), default=0, nullable=False)
    estimated_tax = Column(Numeric(12, 2), default=0, nullable=False)
    status = Column(String, default="open", nullable=False, index=True)
    reviewed_at = Column(DateTime, nullable=True)
    reviewed_by = Column(String, nullable=True)
    notes = Column(Text, nullable=True)
    snapshot_json = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class Payout(Base):
    __tablename__ = "accounting_payouts"

    id = Column(Integer, primary_key=True, index=True)
    provider = Column(String, nullable=True, index=True)
    payout_reference = Column(String, nullable=True, unique=True, index=True)
    amount = Column(Numeric(12, 2), default=0, nullable=False)
    currency = Column(String, nullable=True, default="TRY")
    payout_date = Column(DateTime, nullable=True, index=True)
    status = Column(String, default="pending", nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class AccountingDocument(Base):
    __tablename__ = "accounting_documents"

    id = Column(Integer, primary_key=True, index=True)
    document_type = Column(String, nullable=False, index=True)
    related_type = Column(String, nullable=True, index=True)
    related_id = Column(Integer, nullable=True, index=True)
    file_path = Column(String, nullable=False)
    file_name = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class Reminder(Base):
    __tablename__ = "accounting_reminders"

    id = Column(Integer, primary_key=True, index=True)
    reminder_type = Column(String, nullable=False, index=True)
    title = Column(String, nullable=False)
    detail = Column(Text, nullable=True)
    due_date = Column(DateTime, nullable=True, index=True)
    status = Column(String, default="open", nullable=False, index=True)
    related_type = Column(String, nullable=True, index=True)
    related_id = Column(Integer, nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class AdminActionLog(Base):
    __tablename__ = "admin_action_logs"

    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(Integer, ForeignKey("service_orders.id"), nullable=False, index=True)
    action = Column(String, nullable=False, index=True)
    actor = Column(String, nullable=True)
    metadata_json = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)


class ActivityLog(Base):
    __tablename__ = "activity_log"

    id = Column(Integer, primary_key=True, index=True)
    user_email = Column(String, nullable=True, index=True)
    action = Column(String, nullable=False, index=True)
    target_type = Column(String, nullable=True, index=True)
    target_id = Column(String, nullable=True, index=True)
    detail = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)


class FAQItem(Base):
    __tablename__ = "faq_items"

    id = Column(Integer, primary_key=True, index=True)
    category = Column(String, nullable=False, index=True)
    question_tr = Column(Text, nullable=False)
    question_en = Column(Text, nullable=False)
    answer_tr = Column(Text, nullable=False)
    answer_en = Column(Text, nullable=False)
    sort_order = Column(Integer, default=0, nullable=False, index=True)
    is_published = Column(Boolean, default=True, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class SiteSetting(Base):
    __tablename__ = "site_settings"

    key = Column(String, primary_key=True, index=True)
    value = Column(Text, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class ContactMessage(Base):
    __tablename__ = "contact_messages"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=True)
    email = Column(String, nullable=True, index=True)
    subject = Column(String, nullable=True)
    message = Column(Text, nullable=True)
    is_read = Column(Boolean, default=False, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)


class Article(Base):
    __tablename__ = "articles"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False)
    slug = Column(String, unique=True, index=True, nullable=False)
    category = Column(String, nullable=False, index=True)
    excerpt = Column(Text, nullable=True)
    body = Column(Text, nullable=False)
    content = Column(Text, nullable=True)
    cover_image = Column(String, nullable=True)
    meta_title = Column(String, nullable=True)
    meta_description = Column(Text, nullable=True)
    is_published = Column(Boolean, default=True, nullable=False, index=True)
    published_at = Column(DateTime, nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    author_name = Column(String, nullable=True)
    reading_time = Column(Integer, nullable=True)
    language = Column(String, nullable=True, default="en", index=True)


class EmailLog(Base):
    __tablename__ = "email_logs"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("app_users.id"), nullable=True, index=True)
    email_type = Column(String, nullable=False, index=True)
    recipient_email = Column(String, nullable=False, index=True)
    subject = Column(String, nullable=False)
    status = Column(String, nullable=False, default="sent")
    provider_message_id = Column(String, nullable=True)
    related_event_type = Column(String, nullable=True, index=True)
    related_event_key = Column(String, nullable=True, index=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class EmailCapture(Base):
    __tablename__ = "email_captures"
    __table_args__ = (
        UniqueConstraint("email", "report_id", name="uq_email_capture_email_report"),
    )

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, nullable=False, index=True)
    report_id = Column(Integer, ForeignKey("generated_reports.id"), nullable=True, index=True)
    source = Column(String, nullable=False, default="result_page", index=True)
    is_converted = Column(Boolean, default=False, nullable=False, index=True)
    converted_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)


class FeedbackEntry(Base):
    __tablename__ = "feedback_entries"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("app_users.id"), nullable=True, index=True)
    report_id = Column(Integer, ForeignKey("generated_reports.id"), nullable=False, index=True)
    report_type = Column(String, nullable=False, index=True)
    stage = Column(String, nullable=False, index=True)
    rating = Column(String, nullable=False, index=True)
    comment = Column(Text, nullable=True)
    recommend_flag = Column(Boolean, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)


class BirthplaceEventLog(Base):
    __tablename__ = "birthplace_event_logs"

    id = Column(Integer, primary_key=True, index=True)
    event_name = Column(String, nullable=False, index=True)
    provider = Column(String, nullable=True, index=True)
    outcome = Column(String, nullable=True, index=True)
    location_source = Column(String, nullable=True, index=True)
    confidence = Column(Float, nullable=True)
    suggestion_count = Column(Integer, nullable=True)
    metadata_json = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)


class InterpretationFeedback(Base):
    __tablename__ = "interpretation_feedback"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("app_users.id"), nullable=True, index=True)
    report_id = Column(Integer, ForeignKey("generated_reports.id"), nullable=False, index=True)
    section_name = Column(String, nullable=True, index=True)
    anchor_rank = Column(Integer, nullable=True, index=True)
    anchor_title = Column(String, nullable=True)
    anchor_type = Column(String, nullable=True, index=True)
    domain = Column(String, nullable=True, index=True)
    user_rating = Column(Integer, nullable=False)
    feedback_label = Column(String, nullable=False, index=True)
    free_text_comment = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)


class RecommendationFeedback(Base):
    __tablename__ = "recommendation_feedback"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("app_users.id"), nullable=True, index=True)
    report_id = Column(Integer, ForeignKey("generated_reports.id"), nullable=False, index=True)
    recommendation_index = Column(Integer, nullable=False, index=True)
    recommendation_title = Column(String, nullable=True)
    recommendation_type = Column(String, nullable=True, index=True)
    domain = Column(String, nullable=True, index=True)
    user_feedback_label = Column(String, nullable=False, index=True)
    user_rating = Column(Integer, nullable=True)
    acted_on = Column(Boolean, nullable=True)
    saved_for_later = Column(Boolean, nullable=True)
    free_text_comment = Column(Text, nullable=True)
    feedback_source = Column(String, nullable=True, default="initial", index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)


class RecommendationFollowup(Base):
    __tablename__ = "recommendation_followups"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("app_users.id"), nullable=False, index=True)
    report_id = Column(Integer, ForeignKey("generated_reports.id"), nullable=False, index=True)
    recommendation_index = Column(Integer, nullable=False, index=True)
    recommendation_title = Column(String, nullable=True)
    scheduled_for = Column(DateTime, nullable=False, index=True)
    status = Column(String, nullable=False, default="pending", index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    completed_at = Column(DateTime, nullable=True)


# Migration: Knowledge Visibility & Coverage Layer — 2026-04-29
# Adds:
# - internal_interpretations.used_chunk_ids_json
# - knowledge_items.coverage_entities_json
# - knowledge_chunks.coverage_entities_json
class SourceDocument(Base):
    __tablename__ = "source_documents"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False, index=True)
    file_path = Column(String, nullable=True)
    document_type = Column(String, nullable=True, index=True)
    source_label = Column(String, nullable=True)
    source_uri = Column(String, nullable=True)
    language = Column(String, nullable=False, default="tr", index=True)
    content_text = Column(Text, nullable=True)
    metadata_json = Column(Text, nullable=True)
    created_by_user_id = Column(Integer, ForeignKey("app_users.id"), nullable=True, index=True)
    uploaded_at = Column(DateTime, default=datetime.utcnow, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    created_by = relationship("AppUser")
    knowledge_items = relationship("KnowledgeItem", back_populates="source_document", cascade="all, delete-orphan")


class KnowledgeItem(Base):
    __tablename__ = "knowledge_items"

    id = Column(Integer, primary_key=True, index=True)
    source_document_id = Column(Integer, ForeignKey("source_documents.id"), nullable=True, index=True)
    title = Column(String, nullable=False, index=True)
    item_type = Column(String, nullable=False, default="reference", index=True)
    language = Column(String, nullable=False, default="tr", index=True)
    summary_text = Column(Text, nullable=True)
    body_text = Column(Text, nullable=False)
    entities_json = Column(Text, nullable=True)
    coverage_entities_json = Column(Text, nullable=True)
    metadata_json = Column(Text, nullable=True)
    status = Column(String, nullable=False, default="active", index=True)
    created_by_user_id = Column(Integer, ForeignKey("app_users.id"), nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    source_document = relationship("SourceDocument", back_populates="knowledge_items")
    created_by = relationship("AppUser")
    chunks = relationship("KnowledgeChunk", back_populates="knowledge_item", cascade="all, delete-orphan")


class KnowledgeChunk(Base):
    __tablename__ = "knowledge_chunks"

    id = Column(Integer, primary_key=True, index=True)
    knowledge_item_id = Column(Integer, ForeignKey("knowledge_items.id"), nullable=False, index=True)
    chunk_index = Column(Integer, nullable=False, default=0, index=True)
    chunk_text = Column(Text, nullable=False)
    embedding_json = Column(Text, nullable=True)
    entities_json = Column(Text, nullable=True)
    coverage_entities_json = Column(Text, nullable=True)
    token_count = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    knowledge_item = relationship("KnowledgeItem", back_populates="chunks")


class EvaluationResult(Base):
    __tablename__ = "evaluation_results"

    id = Column(Integer, primary_key=True, index=True)
    interpretation_id = Column(Integer, ForeignKey("internal_interpretations.id"), nullable=True, index=True)
    report_type = Column(String, nullable=True, index=True)
    language = Column(String, nullable=False, default="tr", index=True)
    chart_data_json = Column(Text, nullable=True)
    output_text = Column(Text, nullable=False)
    accuracy_score = Column(Float, nullable=False, default=0.0)
    depth_score = Column(Float, nullable=False, default=0.0)
    safety_score = Column(Float, nullable=False, default=0.0)
    detected_issues_json = Column(Text, nullable=True)
    metadata_json = Column(Text, nullable=True)
    created_by_user_id = Column(Integer, ForeignKey("app_users.id"), nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    interpretation = relationship("InternalInterpretation")
    created_by = relationship("AppUser")
    knowledge_gaps = relationship("KnowledgeGap", back_populates="evaluation_result", cascade="all, delete-orphan")


class KnowledgeGap(Base):
    __tablename__ = "knowledge_gaps"

    id = Column(Integer, primary_key=True, index=True)
    evaluation_result_id = Column(Integer, ForeignKey("evaluation_results.id"), nullable=True, index=True)
    report_type = Column(String, nullable=True, index=True)
    language = Column(String, nullable=False, default="tr", index=True)
    missing_entities_json = Column(Text, nullable=True)
    missing_topics_json = Column(Text, nullable=True)
    context_json = Column(Text, nullable=True)
    status = Column(String, nullable=False, default="open", index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    evaluation_result = relationship("EvaluationResult", back_populates="knowledge_gaps")
    training_tasks = relationship("TrainingTask", back_populates="knowledge_gap", cascade="all, delete-orphan")


class TrainingTask(Base):
    __tablename__ = "training_tasks"

    id = Column(Integer, primary_key=True, index=True)
    knowledge_gap_id = Column(Integer, ForeignKey("knowledge_gaps.id"), nullable=True, index=True)
    task_type = Column(String, nullable=False, default="knowledge_gap", index=True)
    title = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    priority = Column(String, nullable=False, default="medium", index=True)
    status = Column(String, nullable=False, default="open", index=True)
    payload_json = Column(Text, nullable=True)
    created_by_user_id = Column(Integer, ForeignKey("app_users.id"), nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    knowledge_gap = relationship("KnowledgeGap", back_populates="training_tasks")
    created_by = relationship("AppUser")


def _add_missing_columns():
    inspector = inspect(engine)
    table_columns = {
        table_name: {column["name"] for column in inspector.get_columns(table_name)}
        for table_name in inspector.get_table_names()
    }
    with engine.begin() as connection:
        def add_col(table, column, ddl):
            if table in table_columns and column not in table_columns[table]:
                connection.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}"))

        if "app_users" in table_columns and "is_admin" not in table_columns["app_users"]:
            connection.execute(text("ALTER TABLE app_users ADD COLUMN is_admin BOOLEAN DEFAULT 0 NOT NULL"))
        add_col("accounting_customers", "customer_type", "VARCHAR")
        add_col("accounting_customers", "identity_number", "VARCHAR")
        add_col("accounting_customers", "tax_office", "VARCHAR")
        add_col("accounting_customers", "city", "VARCHAR")
        add_col("source_documents", "file_path", "VARCHAR")
        add_col("source_documents", "uploaded_at", "DATETIME")
        add_col("accounting_invoices", "pdf_status", "VARCHAR DEFAULT 'not_generated'")
        add_col("accounting_invoices", "pdf_generated_at", "DATETIME")
        add_col("accounting_invoices", "pdf_error_message", "TEXT")
        add_col("accounting_invoices", "send_status", "VARCHAR DEFAULT 'not_sent'")
        add_col("accounting_invoices", "sent_to_email", "VARCHAR")
        add_col("accounting_invoices", "send_error_message", "TEXT")
        add_col("accounting_invoices", "last_send_attempt_at", "DATETIME")
        add_col("accounting_tax_periods", "reviewed_at", "DATETIME")
        add_col("accounting_tax_periods", "reviewed_by", "VARCHAR")
        add_col("accounting_tax_periods", "notes", "TEXT")
        add_col("accounting_tax_periods", "snapshot_json", "TEXT")
        if "users" in table_columns:
            if "raw_birth_place_input" not in table_columns["users"]:
                connection.execute(text("ALTER TABLE users ADD COLUMN raw_birth_place_input VARCHAR"))
            if "normalized_birth_place" not in table_columns["users"]:
                connection.execute(text("ALTER TABLE users ADD COLUMN normalized_birth_place VARCHAR"))
            if "timezone" not in table_columns["users"]:
                connection.execute(text("ALTER TABLE users ADD COLUMN timezone VARCHAR"))
            if "geocode_provider" not in table_columns["users"]:
                connection.execute(text("ALTER TABLE users ADD COLUMN geocode_provider VARCHAR"))
            if "geocode_confidence" not in table_columns["users"]:
                connection.execute(text("ALTER TABLE users ADD COLUMN geocode_confidence FLOAT"))
        if "user_profiles" in table_columns:
            if "raw_birth_place_input" not in table_columns["user_profiles"]:
                connection.execute(text("ALTER TABLE user_profiles ADD COLUMN raw_birth_place_input VARCHAR"))
            if "normalized_birth_place" not in table_columns["user_profiles"]:
                connection.execute(text("ALTER TABLE user_profiles ADD COLUMN normalized_birth_place VARCHAR"))
            if "timezone" not in table_columns["user_profiles"]:
                connection.execute(text("ALTER TABLE user_profiles ADD COLUMN timezone VARCHAR"))
            if "geocode_provider" not in table_columns["user_profiles"]:
                connection.execute(text("ALTER TABLE user_profiles ADD COLUMN geocode_provider VARCHAR"))
            if "geocode_confidence" not in table_columns["user_profiles"]:
                connection.execute(text("ALTER TABLE user_profiles ADD COLUMN geocode_confidence FLOAT"))
        if "generated_reports" in table_columns:
            if "raw_birth_place_input" not in table_columns["generated_reports"]:
                connection.execute(text("ALTER TABLE generated_reports ADD COLUMN raw_birth_place_input VARCHAR"))
            if "normalized_birth_place" not in table_columns["generated_reports"]:
                connection.execute(text("ALTER TABLE generated_reports ADD COLUMN normalized_birth_place VARCHAR"))
            if "lat" not in table_columns["generated_reports"]:
                connection.execute(text("ALTER TABLE generated_reports ADD COLUMN lat FLOAT"))
            if "lon" not in table_columns["generated_reports"]:
                connection.execute(text("ALTER TABLE generated_reports ADD COLUMN lon FLOAT"))
            if "timezone" not in table_columns["generated_reports"]:
                connection.execute(text("ALTER TABLE generated_reports ADD COLUMN timezone VARCHAR"))
            if "geocode_provider" not in table_columns["generated_reports"]:
                connection.execute(text("ALTER TABLE generated_reports ADD COLUMN geocode_provider VARCHAR"))
            if "geocode_confidence" not in table_columns["generated_reports"]:
                connection.execute(text("ALTER TABLE generated_reports ADD COLUMN geocode_confidence FLOAT"))
            if "calculation_metadata_json" not in table_columns["generated_reports"]:
                connection.execute(text("ALTER TABLE generated_reports ADD COLUMN calculation_metadata_json TEXT"))
            if "access_state" not in table_columns["generated_reports"]:
                connection.execute(text("ALTER TABLE generated_reports ADD COLUMN access_state VARCHAR DEFAULT 'preview' NOT NULL"))
            if "is_paid" not in table_columns["generated_reports"]:
                connection.execute(text("ALTER TABLE generated_reports ADD COLUMN is_paid BOOLEAN DEFAULT 0 NOT NULL"))
            if "unlocked_at" not in table_columns["generated_reports"]:
                connection.execute(text("ALTER TABLE generated_reports ADD COLUMN unlocked_at DATETIME"))
            if "payment_reference" not in table_columns["generated_reports"]:
                connection.execute(text("ALTER TABLE generated_reports ADD COLUMN payment_reference VARCHAR"))
            if "pdf_ready" not in table_columns["generated_reports"]:
                connection.execute(text("ALTER TABLE generated_reports ADD COLUMN pdf_ready BOOLEAN DEFAULT 0 NOT NULL"))
            if "delivered_at" not in table_columns["generated_reports"]:
                connection.execute(text("ALTER TABLE generated_reports ADD COLUMN delivered_at DATETIME"))
        if "service_orders" in table_columns:
            service_order_columns = {
                "public_token": "VARCHAR",
                "amount": "NUMERIC(10, 2)",
                "provider_name": "VARCHAR",
                "provider_token": "VARCHAR",
                "provider_payment_id": "VARCHAR",
                "provider_transaction_id": "VARCHAR",
                "bundle_type": "VARCHAR",
                "included_products_json": "TEXT",
                "bundle_price": "NUMERIC(10, 2)",
                "provider_conversation_id": "VARCHAR",
                "payment_verified_at": "DATETIME",
                "paid_at": "DATETIME",
                "fraud_status": "VARCHAR",
                "calendly_event_uri": "VARCHAR",
                "calendly_invitee_uri": "VARCHAR",
                "calendly_event_type_uri": "VARCHAR",
                "calendly_status": "VARCHAR",
                "calendly_canceled_at": "DATETIME",
                "booking_source": "VARCHAR",
                "scheduled_start": "DATETIME",
                "scheduled_end": "DATETIME",
                "ai_draft_text": "TEXT",
                "ai_draft_status": "VARCHAR",
                "ai_draft_created_at": "DATETIME",
                "ai_draft_version": "INTEGER",
                "pdf_status": "VARCHAR",
                "final_pdf_path": "VARCHAR",
                "customer_confirmation_sent_at": "DATETIME",
                "last_task_error": "TEXT",
                "internal_notes": "TEXT",
                "admin_note": "TEXT",
                "review_started_at": "DATETIME",
                "ready_to_send_at": "DATETIME",
                "delivered_at": "DATETIME",
                "confirmed_at": "DATETIME",
                "prepared_at": "DATETIME",
                "completed_at": "DATETIME",
                "refund_status": "VARCHAR",
                "refund_amount": "NUMERIC(10, 2)",
                "refunded_at": "DATETIME",
                "refund_reason": "TEXT",
                "provider_refund_id": "VARCHAR",
                "refund_provider_status": "VARCHAR",
                "cancellation_reason": "TEXT",
                "cancelled_at": "DATETIME",
                "no_show_at": "DATETIME",
                "reconciliation_notes": "TEXT",
                "is_gift": "BOOLEAN DEFAULT 0 NOT NULL",
                "gift_recipient_name": "VARCHAR",
                "gift_recipient_email": "VARCHAR",
                "user_lang": "VARCHAR DEFAULT 'tr'",
            }
            for column_name, column_type in service_order_columns.items():
                if column_name not in table_columns["service_orders"]:
                    connection.execute(text(f"ALTER TABLE service_orders ADD COLUMN {column_name} {column_type}"))
        if "internal_profiles" in table_columns:
            internal_profile_columns = {
                "gender": "VARCHAR",
                "birth_country": "VARCHAR",
                "birth_city": "VARCHAR",
                "birth_lat": "FLOAT",
                "birth_lng": "FLOAT",
                "birth_timezone": "VARCHAR",
                "notes": "TEXT",
                "is_favorite": "BOOLEAN DEFAULT 0 NOT NULL",
                "created_by_user_id": "INTEGER",
                "last_generated_at": "DATETIME",
            }
            for column_name, column_type in internal_profile_columns.items():
                if column_name not in table_columns["internal_profiles"]:
                    connection.execute(text(f"ALTER TABLE internal_profiles ADD COLUMN {column_name} {column_type}"))
        if "internal_interpretations" in table_columns:
            internal_interpretation_columns = {
                "secondary_profile_id": "INTEGER",
                "render_context_json": "TEXT",
                "pdf_path": "VARCHAR",
                "generation_mode": "VARCHAR DEFAULT 'quick' NOT NULL",
                "created_by_user_id": "INTEGER",
                "prompt_version": "VARCHAR",
                "model_name": "VARCHAR",
                "pipeline_version": "VARCHAR",
                "generation_duration_s": "FLOAT",
                "signal_summary_json": "TEXT",
                "used_chunk_ids_json": "TEXT",
            }
            for column_name, column_type in internal_interpretation_columns.items():
                if column_name not in table_columns["internal_interpretations"]:
                    connection.execute(text(f"ALTER TABLE internal_interpretations ADD COLUMN {column_name} {column_type}"))
        add_col("knowledge_items", "coverage_entities_json", "TEXT")
        add_col("knowledge_chunks", "coverage_entities_json", "TEXT")
        if "internal_chat_sessions" in table_columns:
            internal_chat_session_columns = {
                "profile_id": "INTEGER",
                "secondary_profile_id": "INTEGER",
                "title": "VARCHAR",
                "report_type": "VARCHAR",
                "mode": "VARCHAR DEFAULT 'grounded' NOT NULL",
                "created_by_user_id": "INTEGER",
            }
            for column_name, column_type in internal_chat_session_columns.items():
                if column_name not in table_columns["internal_chat_sessions"]:
                    connection.execute(text(f"ALTER TABLE internal_chat_sessions ADD COLUMN {column_name} {column_type}"))
        if "internal_chat_messages" in table_columns:
            internal_chat_message_columns = {
                "tool_payload_json": "TEXT",
                "context_snapshot_json": "TEXT",
            }
            for column_name, column_type in internal_chat_message_columns.items():
                if column_name not in table_columns["internal_chat_messages"]:
                    connection.execute(text(f"ALTER TABLE internal_chat_messages ADD COLUMN {column_name} {column_type}"))
        if "ai_behavior_rule_sets" in table_columns:
            ai_behavior_rule_set_columns = {
                "description": "TEXT",
                "is_active": "BOOLEAN DEFAULT 1 NOT NULL",
                "created_by_user_id": "INTEGER",
            }
            for column_name, column_type in ai_behavior_rule_set_columns.items():
                if column_name not in table_columns["ai_behavior_rule_sets"]:
                    connection.execute(text(f"ALTER TABLE ai_behavior_rule_sets ADD COLUMN {column_name} {column_type}"))
        if "ai_behavior_rules" in table_columns:
            ai_behavior_rule_columns = {
                "section": "VARCHAR",
                "is_enabled": "BOOLEAN DEFAULT 1 NOT NULL",
                "is_editable": "BOOLEAN DEFAULT 1 NOT NULL",
                "sort_order": "INTEGER DEFAULT 0 NOT NULL",
            }
            for column_name, column_type in ai_behavior_rule_columns.items():
                if column_name not in table_columns["ai_behavior_rules"]:
                    connection.execute(text(f"ALTER TABLE ai_behavior_rules ADD COLUMN {column_name} {column_type}"))
        if "articles" in table_columns:
            if "meta_title" not in table_columns["articles"]:
                connection.execute(text("ALTER TABLE articles ADD COLUMN meta_title VARCHAR"))
            if "meta_description" not in table_columns["articles"]:
                connection.execute(text("ALTER TABLE articles ADD COLUMN meta_description TEXT"))
            if "is_published" not in table_columns["articles"]:
                connection.execute(text("ALTER TABLE articles ADD COLUMN is_published INTEGER DEFAULT 0"))
            if "content" not in table_columns["articles"]:
                connection.execute(text("ALTER TABLE articles ADD COLUMN content TEXT"))
        if "report_orders" in table_columns:
            if "admin_note" not in table_columns["report_orders"]:
                connection.execute(text("ALTER TABLE report_orders ADD COLUMN admin_note TEXT"))
        if "email_captures" in table_columns:
            if "source" not in table_columns["email_captures"]:
                connection.execute(text("ALTER TABLE email_captures ADD COLUMN source VARCHAR DEFAULT 'result_page'"))
            if "is_converted" not in table_columns["email_captures"]:
                connection.execute(text("ALTER TABLE email_captures ADD COLUMN is_converted BOOLEAN DEFAULT 0 NOT NULL"))
            if "converted_at" not in table_columns["email_captures"]:
                connection.execute(text("ALTER TABLE email_captures ADD COLUMN converted_at DATETIME"))
        if "birthplace_event_logs" in table_columns:
            if "provider" not in table_columns["birthplace_event_logs"]:
                connection.execute(text("ALTER TABLE birthplace_event_logs ADD COLUMN provider VARCHAR"))
            if "outcome" not in table_columns["birthplace_event_logs"]:
                connection.execute(text("ALTER TABLE birthplace_event_logs ADD COLUMN outcome VARCHAR"))
            if "location_source" not in table_columns["birthplace_event_logs"]:
                connection.execute(text("ALTER TABLE birthplace_event_logs ADD COLUMN location_source VARCHAR"))
            if "confidence" not in table_columns["birthplace_event_logs"]:
                connection.execute(text("ALTER TABLE birthplace_event_logs ADD COLUMN confidence FLOAT"))
            if "suggestion_count" not in table_columns["birthplace_event_logs"]:
                connection.execute(text("ALTER TABLE birthplace_event_logs ADD COLUMN suggestion_count INTEGER"))
            if "metadata_json" not in table_columns["birthplace_event_logs"]:
                connection.execute(text("ALTER TABLE birthplace_event_logs ADD COLUMN metadata_json TEXT"))
        if "recommendation_feedback" in table_columns:
            if "recommendation_title" not in table_columns["recommendation_feedback"]:
                connection.execute(text("ALTER TABLE recommendation_feedback ADD COLUMN recommendation_title VARCHAR"))
            if "recommendation_type" not in table_columns["recommendation_feedback"]:
                connection.execute(text("ALTER TABLE recommendation_feedback ADD COLUMN recommendation_type VARCHAR"))
            if "domain" not in table_columns["recommendation_feedback"]:
                connection.execute(text("ALTER TABLE recommendation_feedback ADD COLUMN domain VARCHAR"))
            if "user_rating" not in table_columns["recommendation_feedback"]:
                connection.execute(text("ALTER TABLE recommendation_feedback ADD COLUMN user_rating INTEGER"))
            if "acted_on" not in table_columns["recommendation_feedback"]:
                connection.execute(text("ALTER TABLE recommendation_feedback ADD COLUMN acted_on BOOLEAN"))
            if "saved_for_later" not in table_columns["recommendation_feedback"]:
                connection.execute(text("ALTER TABLE recommendation_feedback ADD COLUMN saved_for_later BOOLEAN"))
            if "free_text_comment" not in table_columns["recommendation_feedback"]:
                connection.execute(text("ALTER TABLE recommendation_feedback ADD COLUMN free_text_comment TEXT"))
            if "feedback_source" not in table_columns["recommendation_feedback"]:
                connection.execute(text("ALTER TABLE recommendation_feedback ADD COLUMN feedback_source VARCHAR DEFAULT 'initial'"))


def init_db():
    Base.metadata.create_all(bind=engine)
    _add_missing_columns()
