from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Float, ForeignKey, Integer, Numeric, String, Text, UniqueConstraint, create_engine, inspect, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

SQLALCHEMY_DATABASE_URL = "sqlite:///./astro_logic.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

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


class Article(Base):
    __tablename__ = "articles"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False)
    slug = Column(String, unique=True, index=True, nullable=False)
    category = Column(String, nullable=False, index=True)
    excerpt = Column(Text, nullable=True)
    body = Column(Text, nullable=False)
    cover_image = Column(String, nullable=True)
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


def _add_missing_columns():
    inspector = inspect(engine)
    table_columns = {
        table_name: {column["name"] for column in inspector.get_columns(table_name)}
        for table_name in inspector.get_table_names()
    }
    with engine.begin() as connection:
        if "app_users" in table_columns and "is_admin" not in table_columns["app_users"]:
            connection.execute(text("ALTER TABLE app_users ADD COLUMN is_admin BOOLEAN DEFAULT 0 NOT NULL"))
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
            }
            for column_name, column_type in service_order_columns.items():
                if column_name not in table_columns["service_orders"]:
                    connection.execute(text(f"ALTER TABLE service_orders ADD COLUMN {column_name} {column_type}"))
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
