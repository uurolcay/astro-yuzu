import csv
import copy
import base64
import html as html_lib
import hmac
import hashlib
import json
import logging
import os
import re
import sys
import time
import secrets
from collections import defaultdict
from functools import wraps
from io import BytesIO, StringIO
from datetime import timedelta
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from unicodedata import normalize
from datetime import date, datetime
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlencode

from dotenv import load_dotenv
import pytz
import uvicorn
from jinja2 import Environment, FileSystemLoader, pass_context
from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.requests import Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, inspect as sa_inspect, or_, text as sa_text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from starlette.middleware.sessions import SessionMiddleware
from itsdangerous import BadSignature, TimestampSigner
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from weasyprint import HTML as WeasyHTML
from werkzeug.security import check_password_hash, generate_password_hash

import ai_interpreter as ai_logic
import database as db_mod
import email_utils
from services.accounting import exports as accounting_exports
from services.accounting import document_service, invoice_service, month_close_service, reminders_service, tax_service, transaction_service
from services.accounting.calculations import collection_ratio, invoice_coverage_ratio
import utils
from translations import (
    TRANSLATIONS,
    get_preferred_language,
    t as translate_text,
    translation_namespace,
)
from config.astro_config import ASTRO_CONFIG, ASTRO_DEBUG, ASTRO_ENGINE_VERSION
from core.ayanamsa import get_ayanamsa_trace
from core.calculation_context import CalculationContext
from core.feedback import load_feedback_history, save_interpretation_feedback
from core.interpretation import build_interpretation_layer
from core.article_matching import match_articles_to_result
from core.dual_chart import build_parent_child_ai_summary, build_parent_child_interpretation
from core.metadata import build_calculation_metadata_snapshot
from core.recommendations import (
    VALID_RECOMMENDATION_FEEDBACK_LABELS,
    VALID_RECOMMENDATION_FOLLOWUP_LABELS,
    compute_recommendation_feedback_summary,
    derive_followup_time,
)
from services.admin_api_contracts import (
    get_admin_api_schema_version,
    build_admin_api_docs_payload,
    build_admin_segments_export_metadata_payload,
    json_admin_error,
    json_ok,
)
from services.admin_segments import (
    SEGMENT_GROUPS,
    build_admin_segments_api_payload,
    build_campaign_export_row,
    build_export_columns_for_view,
    generate_campaign_ready_segments,
    generate_lifecycle_segments,
    _apply_segment_context_filters,
    _resolve_segment_filters,
    _segment_export_rows,
)
from services import admin_astro_workspace as astro_workspace
from services import admin_astro_chat as astro_chat
from services import ai_behavior_rules
from services import astro_signal_enrichment
from services import evaluation_service
from services import gap_detector
from services import interpretation_quality_service as quality_svc
from services import knowledge_coverage_service as coverage_svc
from services import knowledge_import_service
from services import knowledge_service
from services import embedding_service
from services import report_structure_v3
from services import retrieval_service
from services import training_service
from services import document_parser, document_chunker
from services import nakshatra_extraction_service
from services.geocoding import BirthPlaceResolutionError, search_birth_places
from services import payments
from engines import engines_dasha, engines_eclipses, engines_natal, engines_navamsa, engines_transits
from engines.life_area_impact_engine import analyze_life_area_impact
from engines.narrative_compression_engine import (
    compress_ai_narratives,
    localize_narrative_analysis,
    localize_narrative_text,
)
from engines.psychological_theme_engine import extract_psychological_themes
from engines.timing_intelligence_engine import build_timing_intelligence

try:
    import engines_fullmoons
except ImportError:
    engines_fullmoons = None


load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

def get_bool_env(key, default):
    return os.getenv(key, str(default)).lower() == "true"


LAUNCH_MODE = get_bool_env("LAUNCH_MODE", True)
ENABLE_PAYMENTS = get_bool_env("ENABLE_PAYMENTS", False)
ENABLE_FREE_CALCULATOR = get_bool_env("ENABLE_FREE_CALCULATOR", False)
ENABLE_AI_INTERPRETATION = get_bool_env("ENABLE_AI_INTERPRETATION", False)
ENABLE_CONSULTATION_BOOKING = get_bool_env("ENABLE_CONSULTATION_BOOKING", False)
PORT = int(os.getenv("PORT", 8000))


def _env_flag_value(name, default=False):
    return get_bool_env(name, default)


def launch_mode_enabled():
    return _env_flag_value("LAUNCH_MODE", default=True)


def launch_payments_enabled():
    return _env_flag_value("ENABLE_PAYMENTS", default=False)


def launch_free_calculator_enabled():
    return _env_flag_value("ENABLE_FREE_CALCULATOR", default=False)


def launch_ai_interpretation_enabled():
    return _env_flag_value("ENABLE_AI_INTERPRETATION", default=False)


def launch_consultation_booking_enabled():
    return _env_flag_value("ENABLE_CONSULTATION_BOOKING", default=False)


def _launch_mode_flags():
    return {
        "launch_mode": launch_mode_enabled(),
        "enable_payments": launch_payments_enabled(),
        "enable_free_calculator": launch_free_calculator_enabled(),
        "enable_ai_interpretation": launch_ai_interpretation_enabled(),
        "enable_consultation_booking": launch_consultation_booking_enabled(),
    }


TRUST_PROXY = _env_flag_value("TRUST_PROXY", default=False)
SESSION_SECRET_KEY = (
    os.getenv("SECRET_KEY")
    or os.getenv("APP_SECRET_KEY")
    or "jyotish-dev-secret-change-me"
)


def _is_production_runtime():
    env_name = str(
        os.getenv("APP_ENV")
        or os.getenv("ENVIRONMENT")
        or os.getenv("ENV")
        or os.getenv("FASTAPI_ENV")
        or ""
    ).strip().lower()
    if env_name in {"prod", "production"}:
        return True
    return any(str(os.getenv(key, "")).strip() for key in ("RENDER", "RENDER_SERVICE_ID", "RENDER_EXTERNAL_URL"))


def _configured_admin_emails():
    values = [
        str(os.getenv("ADMIN_EMAIL", "")).strip().lower(),
        str(os.getenv("ADMIN_USERNAME", "")).strip().lower(),
    ]
    values.extend(item.strip().lower() for item in str(os.getenv("ADMIN_EMAILS", "")).split(","))
    return {value for value in values if value}


def _admin_password_configured():
    return bool(str(os.getenv("ADMIN_PASSWORD", "") or "").strip())


def _session_secret_configured():
    return bool(str(os.getenv("SECRET_KEY", "") or os.getenv("APP_SECRET_KEY", "")).strip())


def _using_development_session_secret():
    return SESSION_SECRET_KEY == "jyotish-dev-secret-change-me"


app = FastAPI(title="Astro-Yuzu Intelligence Core", version="5.3")
templates = Jinja2Templates(directory="templates")

print("LAUNCH_MODE:", LAUNCH_MODE)
print("ENABLE_PAYMENTS:", ENABLE_PAYMENTS)


@pass_context
def _template_translate(context, key, lang=None, **kwargs):
    request = context.get("request")
    resolved_lang = (
        lang
        or context.get("lang")
        or context.get("language")
        or (getattr(getattr(request, "state", None), "lang", None) if request else None)
    )
    return translate_text(key, resolved_lang, **kwargs)


templates.env.globals["t"] = _template_translate
templates.env.globals["translation_namespace"] = translation_namespace
app.mount("/static", StaticFiles(directory="static"), name="static")
app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET_KEY, max_age=int(timedelta(days=30).total_seconds()))

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

db_mod.init_db()
os.makedirs("static/reports", exist_ok=True)
_pdf_env = Environment(loader=FileSystemLoader("templates/pdf"))


def get_setting(db: Session, key: str, default: str = "") -> str:
    row = db.query(db_mod.SiteSetting).filter(db_mod.SiteSetting.key == key).first()
    return row.value if row and row.value is not None else default


def parse_price_amount(value) -> Decimal:
    raw = str(value or "").strip()
    raw = raw.replace("₺", "").replace("â‚º", "").replace("TRY", "").replace("TL", "").replace(" ", "")
    if "," in raw and "." in raw:
        if raw.rfind(",") > raw.rfind("."):
            raw = raw.replace(".", "").replace(",", ".")
        else:
            raw = raw.replace(",", "")
    elif "," in raw:
        raw = raw.replace(".", "").replace(",", ".")
    elif "." in raw:
        parts = raw.split(".")
        if len(parts[-1]) == 3:
            raw = "".join(parts)
    try:
        amount = Decimal(raw)
    except (InvalidOperation, ValueError):
        amount = Decimal("0")
    return amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _format_try_amount(amount: Decimal, language: str = "tr") -> str:
    whole = int(amount.quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    separator = "," if language == "en" else "."
    formatted = f"{whole:,}".replace(",", separator)
    return f"₺{formatted} TL" if language == "en" else f"₺{formatted}"


def parse_usd_try_rate(value) -> Decimal | None:
    raw = str(value or "").strip().replace(",", ".")
    try:
        rate = Decimal(raw)
    except (InvalidOperation, ValueError):
        return None
    if rate <= 0:
        return None
    return rate


def get_usd_try_rate(db: Session) -> Decimal | None:
    return parse_usd_try_rate(get_setting(db, "site_usd_try_rate", ""))


def format_price_html(value, language: str = "tr", usd_try_rate: Decimal | None = None) -> str:
    lang = "en" if language == "en" else "tr"
    amount = parse_price_amount(value)
    label = _format_try_amount(amount, lang)
    if lang == "en" and usd_try_rate:
        usd_amount = (amount / usd_try_rate).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        label = f'{label} <span class="price-approx">(≈ ${int(usd_amount):,})</span>'
    return label


def render_email(template_name: str, **kwargs) -> str:
    return email_utils.render_email(template_name, **kwargs)


def send_email(
    to_email: str,
    subject: str,
    html_body: str,
    plain_body: str = "",
    attachment_path: str | None = None,
    attachment_filename: str | None = None,
) -> bool:
    return email_utils.send_email(
        to_email=to_email,
        subject=subject,
        html_body=html_body,
        plain_body=plain_body,
        attachment_path=attachment_path,
        attachment_filename=attachment_filename,
    )


def get_astrologer_name(db: Session) -> str:
    return get_setting(db, "astrologer_name", "Focus Astrology") or "Focus Astrology"


def generate_report_pdf(order_id: int, context: dict) -> str:
    """Render the PDF template and write to static/reports/{order_id}.pdf"""
    html_content = _pdf_env.get_template("report_base.html").render(**context)
    output_path = f"static/reports/{order_id}.pdf"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    WeasyHTML(string=html_content, base_url=".").write_pdf(output_path)
    return output_path


def _send_logged_email(
    db: Session,
    *,
    email_type: str,
    to_email: str,
    subject: str,
    html_body: str,
    event_type: str | None = None,
    event_key: str | None = None,
    user_id: int | None = None,
    plain_body: str = "",
    attachment_path: str | None = None,
    attachment_filename: str | None = None,
) -> bool:
    if not to_email:
        return False
    if _find_existing_email_log(db, email_type, to_email, event_key):
        logger.info("Duplicate transactional email suppressed email_type=%s recipient=%s event_key=%s", email_type, to_email, event_key)
        return True
    ok = send_email(
        to_email=to_email,
        subject=subject,
        html_body=html_body,
        plain_body=plain_body,
        attachment_path=attachment_path,
        attachment_filename=attachment_filename,
    )
    _create_email_log(
        db,
        user_id=user_id,
        email_type=email_type,
        recipient_email=to_email,
        subject=subject,
        status="sent" if ok else "failed",
        related_event_type=event_type,
        related_event_key=event_key,
        error_message=None if ok else "send_email_failed",
    )
    return ok


def base_context(request: Request, db: Session, **kwargs) -> dict:
    usd_try_rate = get_usd_try_rate(db)
    return _auth_template_context(
        request,
        instagram_url=get_setting(db, "site_instagram_url", "https://www.instagram.com/feruze.olcay/"),
        usd_try_rate=usd_try_rate,
        format_price=lambda value, language="tr": format_price_html(value, language, usd_try_rate),
        **kwargs,
    )


def seed_faq_if_empty(db: Session):
    if db.query(db_mod.FAQItem).count() > 0:
        return
    seed_data = [
        db_mod.FAQItem(category="Genel", question_tr="Vedik astroloji nedir, Batı astrolojisinden farkı ne?", answer_tr="Vedik astroloji (Jyotisha), Hindistan'ın kadim Vedik geleneğinden gelen ve beş binden fazla yıllık birikimi olan bir sistemdir. Batı astrolojisinden en temel farkı hesaplama yöntemidir: Vedik sistem sabit yıldızları referans alır (sidereal zodiac), Batı sistemi ise Güneş'in ilkbahar noktasını (tropical zodiac).", question_en="What is Vedic astrology and how does it differ from Western astrology?", answer_en="Vedic astrology (Jyotisha) is an ancient system from India's Vedic tradition with over five thousand years of history. Its main difference from Western astrology is the calculation method: Vedic uses the sidereal zodiac (fixed stars), while Western uses the tropical zodiac (Sun's spring equinox point).", sort_order=1, is_published=True),
        db_mod.FAQItem(category="Genel", question_tr="Harita hesaplaması ücretsiz mi?", answer_tr="Evet, doğum haritanızı hesaplamak tamamen ücretsizdir. Haritayı yorumlamak için ücretli raporlarımızdan veya birebir danışmanlık seansından yararlanabilirsiniz.", question_en="Is the birth chart calculation free?", answer_en="Yes, calculating your birth chart is completely free. To interpret the chart, you can use our paid reports or a one-on-one consultation session.", sort_order=2, is_published=True),
        db_mod.FAQItem(category="Genel", question_tr="Doğum saatimi bilmiyorum, yine de analiz yaptırabilir miyim?", answer_tr="Doğum saati, doğru bir harita için en kritik bilgidir. Saatinizi kesin olarak bilmiyorsanız bize not ekleyebilirsiniz; bazı durumlarda saati daraltan teknikler uygulanabilir.", question_en="I don't know my birth time - can I still get an analysis?", answer_en="Birth time is the most critical piece of information for an accurate chart. If you don't know your exact time, you can add a note and we may apply rectification techniques in some cases.", sort_order=3, is_published=True),
        db_mod.FAQItem(category="Raporlar", question_tr="Raporlar nasıl hazırlanıyor, ne zaman teslim ediliyor?", answer_tr="Doğum bilgilerinizle AI destekli bir taslak hazırlanır ve uzman incelemesinden geçirilir. Rapor en geç 7 gün içinde e-posta ile teslim edilir.", question_en="How are reports prepared and when are they delivered?", answer_en="An AI-assisted draft is created from your birth data and reviewed by an expert. The report is delivered by email within 7 days.", sort_order=1, is_published=True),
        db_mod.FAQItem(category="Raporlar", question_tr="Hangi raporu seçmeliyim?", answer_tr="Genel yaşam temalarını anlamak için Doğum Haritası, dönem ve zamanlama için Yıllık Transit, kariyer kararları için Kariyer raporu önerilir. Birden fazla konu aynı anda etkili oluyorsa danışmanlık daha kapsamlı bir çerçeve sunar.", question_en="Which report should I choose?", answer_en="For understanding general life themes, choose Natal Chart. For timing and upcoming periods, Annual Transit. For career decisions, Career report. If multiple themes are active simultaneously, a consultation offers a more comprehensive framework.", sort_order=2, is_published=True),
        db_mod.FAQItem(category="Raporlar", question_tr="Rapor satın aldıktan sonra iade alabilir miyim?", answer_tr="Dijital ürün niteliğindeki raporlarda, hazırlık süreci başladıktan sonra iade yapılamamaktadır. Detaylar için Satış Koşulları sayfamızı inceleyebilirsiniz.", question_en="Can I get a refund after purchasing a report?", answer_en="Once the preparation process has started, refunds are not available for digital products. Please review our Sales Terms page for full details.", sort_order=3, is_published=True),
        db_mod.FAQItem(category="Danışmanlık", question_tr="Birebir danışmanlık seansı nasıl işliyor?", answer_tr="Seans başvurusu yaptıktan sonra doğum bilgilerinizi paylaşırsınız. Görüşme öncesinde haritanız incelenir. 60 dakikalık seans online olarak gerçekleştirilmektedir.", question_en="How does a one-on-one consultation session work?", answer_en="After applying, you share your birth information. Your chart is reviewed before the session. The 60-minute session takes place online.", sort_order=1, is_published=True),
        db_mod.FAQItem(category="Danışmanlık", question_tr="Randevumu iptal etmek veya ertelemek istiyorum.", answer_tr="Randevunuzu planlanan saatten en az 24 saat önce ücretsiz olarak iptal edebilir veya erteleyebilirsiniz. 24 saatten kısa süre kala yapılan iptallerde ücret iadesi yapılamamaktadır.", question_en="I want to cancel or reschedule my appointment.", answer_en="You may cancel or reschedule free of charge at least 24 hours before your appointment. Cancellations made less than 24 hours in advance are non-refundable.", sort_order=2, is_published=True),
        db_mod.FAQItem(category="Danışmanlık", question_tr="Başkası adına seans veya rapor alabilir miyim?", answer_tr="Evet, sevdiğiniz biri için rapor veya danışmanlık seansı satın alabilirsiniz. Sipariş formunda alıcının adını ve e-posta adresini belirtmeniz yeterlidir.", question_en="Can I buy a session or report for someone else?", answer_en="Yes, you can purchase a report or consultation session as a gift. Simply provide the recipient's name and email address in the order form.", sort_order=3, is_published=True),
    ]
    for item in seed_data:
        db.add(item)
    db.commit()


_seed_db = db_mod.SessionLocal()
try:
    seed_faq_if_empty(_seed_db)
finally:
    _seed_db.close()

logger = logging.getLogger(__name__)
if _using_development_session_secret():
    logger.warning("Using development session secret. Set SECRET_KEY or APP_SECRET_KEY in production.")
if _is_production_runtime() and not _session_secret_configured():
    logger.warning("Production runtime detected without SECRET_KEY/APP_SECRET_KEY; session cookies are using the development fallback.")
if _is_production_runtime() and _using_development_session_secret():
    logger.warning("Production is using development session secret; login/CSRF may be unstable.")
if not _configured_admin_emails():
    logger.warning("ADMIN_EMAIL/ADMIN_USERNAME/ADMIN_EMAILS is not configured; admin login bootstrap may be unavailable.")
if not _admin_password_configured():
    logger.warning("ADMIN_PASSWORD is not configured; env-based admin login/bootstrap password is not configured.")
if payments.payments_enabled() and payments.payment_provider() == "iyzico":
    missing_iyzico_config = [
        name
        for name in ("IYZICO_API_KEY", "IYZICO_SECRET_KEY")
        if not str(os.getenv(name, "")).strip()
    ]
    if missing_iyzico_config:
        logger.warning("iyzico payments are enabled but missing config keys=%s", ",".join(missing_iyzico_config))
if not (str(os.getenv("GEMINI_API_KEY", "")).strip() or str(os.getenv("OPENAI_API_KEY", "")).strip()):
    logger.info("No AI provider key detected; interpretation endpoints depend on provider environment configuration.")


def _bootstrap_admin_user_from_env():
    admin_email = (
        str(os.getenv("ADMIN_EMAIL", "")).strip().lower()
        or str(os.getenv("ADMIN_USERNAME", "")).strip().lower()
    )
    admin_password = str(os.getenv("ADMIN_PASSWORD", "") or "")
    if not admin_email:
        return {"status": "skipped", "reason": "missing_env"}
    if "@" not in admin_email:
        logger.warning("ADMIN_EMAIL/ADMIN_USERNAME must be an email address; admin bootstrap skipped.")
        return {"status": "skipped", "reason": "invalid_email"}

    db = db_mod.SessionLocal()
    try:
        user = db.query(db_mod.AppUser).filter(db_mod.AppUser.email == admin_email).first()
        created = False
        if not user:
            if len(admin_password) < 8:
                logger.warning("ADMIN_PASSWORD must be at least 8 characters; admin bootstrap skipped.")
                return {"status": "skipped", "reason": "weak_password"}
            user = db_mod.AppUser(
                email=admin_email,
                password_hash=generate_password_hash(admin_password),
                name=admin_email.split("@")[0],
                plan_code="free",
                is_admin=True,
                is_active=True,
            )
            db.add(user)
            created = True
        else:
            user.is_admin = True
            user.is_active = True
            if _env_flag_value("ADMIN_RESET_PASSWORD", default=False):
                user.password_hash = generate_password_hash(admin_password)
        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            user = db.query(db_mod.AppUser).filter(db_mod.AppUser.email == admin_email).first()
            if not user:
                raise
            user.is_admin = True
            user.is_active = True
            if _env_flag_value("ADMIN_RESET_PASSWORD", default=False):
                user.password_hash = generate_password_hash(admin_password)
            db.commit()
            created = False
        logger.info("Admin bootstrap %s email=%s", "created" if created else "verified", admin_email)
        return {"status": "created" if created else "verified", "email": admin_email}
    finally:
        db.close()


_bootstrap_admin_user_from_env()
BASE_DIR = Path(__file__).resolve().parent
GTK_RUNTIME_DIR = Path(r"C:\Program Files\GTK3-Runtime Win64")
GTK_BIN_DIR = GTK_RUNTIME_DIR / "bin"
GTK_ETC_DIR = GTK_RUNTIME_DIR / "etc"
GTK_FONTS_DIR = GTK_ETC_DIR / "fonts"
GTK_FONTS_FILE = GTK_FONTS_DIR / "fonts.conf"
GTK_SHARE_DIR = GTK_RUNTIME_DIR / "share"
WINDOWS_FONTS_DIR = Path(os.environ.get("WINDIR", r"C:\Windows")) / "Fonts"
RUNTIME_CACHE_DIR = BASE_DIR / ".runtime_cache"
FONTCONFIG_CACHE_DIR = RUNTIME_CACHE_DIR / "fontconfig"
RUNTIME_TMP_DIR = RUNTIME_CACHE_DIR / "tmp"
FONTCONFIG_RUNTIME_FILE = RUNTIME_CACHE_DIR / "fonts.runtime.conf"
_PDF_RUNTIME_CONFIGURED = False
_PDF_DLL_DIRECTORY_HANDLE = None
UPLOADS_DIR = Path(os.getenv("UPLOAD_DIR", str(BASE_DIR / "uploads"))).resolve()


def get_upload_dir():
    configured = str(os.getenv("UPLOAD_DIR", str(UPLOADS_DIR))).strip()
    upload_dir = Path(configured).resolve()
    upload_dir.mkdir(parents=True, exist_ok=True)
    return upload_dir


def _is_render_env():
    return any(
        str(os.getenv(key, "")).strip()
        for key in ("RENDER", "RENDER_SERVICE_ID", "RENDER_EXTERNAL_URL")
    )


def _storage_warnings(engine_meta, uploads_dir, *, is_render_env=False):
    warnings = []
    if engine_meta.get("database_url_missing"):
        warnings.append("DATABASE_URL missing")
    if is_render_env and engine_meta.get("db_dialect") == "sqlite":
        warnings.append("Production SQLite detected")
    normalized_upload_dir = str(uploads_dir).replace("\\", "/").rstrip("/")
    if is_render_env and not normalized_upload_dir.startswith("/var/data"):
        warnings.append("Upload dir not under /var/data while Render detected")
        warnings.append("Upload dir may be ephemeral")
    return warnings


def _storage_diagnostics(db: Session):
    engine_meta = db_mod.get_engine_diagnostics()
    uploads_dir = get_upload_dir()
    uploads_dir_exists = uploads_dir.exists()
    is_render_env = _is_render_env()
    warnings = _storage_warnings(engine_meta, uploads_dir, is_render_env=is_render_env)
    source_documents_count = db.query(db_mod.SourceDocument).count()
    knowledge_items_count = db.query(db_mod.KnowledgeItem).count()
    knowledge_chunks_count = db.query(db_mod.KnowledgeChunk).count()
    review_required_count = db.query(db_mod.KnowledgeItem).filter(db_mod.KnowledgeItem.status == "review_required").count()
    published_count = db.query(db_mod.KnowledgeItem).filter(db_mod.KnowledgeItem.status.in_(["published", "active"])).count()
    return {
        **engine_meta,
        "source_documents_count": source_documents_count,
        "knowledge_items_count": knowledge_items_count,
        "knowledge_chunks_count": knowledge_chunks_count,
        "review_required_count": review_required_count,
        "published_count": published_count,
        "uploads_dir_path": str(uploads_dir),
        "uploads_dir_exists": uploads_dir_exists,
        "uploads_file_count": len([item for item in uploads_dir.iterdir() if item.is_file()]) if uploads_dir_exists else 0,
        "is_render_env": is_render_env,
        "warnings": warnings,
        "session_config": {
            "secret_configured": _session_secret_configured(),
            "using_dev_secret": _using_development_session_secret(),
        },
    }


_startup_storage_meta = db_mod.get_engine_diagnostics()
_startup_render_env = _is_render_env()
_startup_upload_dir = get_upload_dir()
_startup_warnings = _storage_warnings(_startup_storage_meta, _startup_upload_dir, is_render_env=_startup_render_env)
logger.info(
    "Storage startup db_dialect=%s db_url=%s upload_dir=%s render_env=%s warnings=%s",
    _startup_storage_meta.get("db_dialect"),
    _startup_storage_meta.get("database_url_masked"),
    str(_startup_upload_dir),
    _startup_render_env,
    ",".join(_startup_warnings) if _startup_warnings else "-",
)
REPORT_TYPES = {
    "preview": {
        "label": "Preview",
        "include_pdf": False,
        "include_scores": False,
        "include_lunations": False,
        "include_timing": False,
        "include_action_guidance": False,
    },
    "basic": {
        "label": "Basic",
        "include_pdf": True,
        "include_scores": False,
        "include_lunations": False,
        "include_timing": True,
        "include_action_guidance": False,
    },
    "premium": {
        "label": "Premium",
        "include_pdf": True,
        "include_scores": True,
        "include_lunations": True,
        "include_timing": True,
        "include_action_guidance": True,
    },
    "elite": {
        "label": "Elite",
        "include_pdf": True,
        "include_scores": True,
        "include_lunations": True,
        "include_timing": True,
        "include_action_guidance": True,
    },
    "parent_child": {
        "label": "Parent-Child",
        "include_pdf": True,
        "include_scores": True,
        "include_lunations": False,
        "include_timing": True,
        "include_action_guidance": True,
    },
}
REPORT_ORDER_PRODUCTS = {
    "birth_chart_karma": {
        "title": "Doğum Haritası Karma’sı",
        "label": "Temel harita",
        "price": "₺1.900",
        "summary": "Yaşam temalarınızı, doğal güçlü yönlerinizi ve tekrar eden karmik örüntüleri daha bütünlüklü anlamak için temel yazılı analiz.",
        "draft_focus": "foundational birth chart, karmic patterns, strengths, challenges, broader life direction",
    },
    "annual_transit": {
        "title": "Yıllık Transit",
        "label": "Zamanlama",
        "price": "₺1.490",
        "summary": "Önümüzdeki dönemin ana vurgularını, fırsat pencerelerini ve dikkat isteyen zamanlarını daha bilinçli planlamak için odak raporu.",
        "draft_focus": "annual timing, transitions, upcoming periods, opportunity and pressure windows",
    },
    "career": {
        "title": "Kariyer",
        "label": "Kariyer yönü",
        "price": "₺1.690",
        "summary": "Doğal çalışma biçiminizi, mesleki potansiyelinizi ve uzun vadeli büyüme yönünüzü anlamak için stratejik kariyer analizi.",
        "draft_focus": "career direction, professional strengths, work rhythm, vocational decisions",
    },
    "parent_child": {
        "title": "Ebeveyn-Çocuk",
        "label": "Aile dinamiği",
        "price": "₺1.790",
        "summary": "Çocuğun doğasını, ebeveyn-çocuk iletişimini ve daha bilinçli destek biçimlerini anlamaya yönelik hassas analiz.",
        "draft_focus": "parent-child relationship, temperament, emotional needs, communication dynamics",
    },
}
CONSULTATION_PRODUCT = {
    "service_type": "consultation",
    "product_type": "consultation_60_min",
    "title": "60 dk Birebir Astroloji Danışmanlığı",
    "label": "Birebir danışmanlık",
    "price": "₺4.900",
    "summary": "Haritanızı kişisel sorularınızla birlikte ele alan, doğrudan yanıt ve stratejik sentez odaklı 60 dakikalık birebir danışmanlık.",
}
REPORT_BUNDLE_PRODUCTS = {
    "life_path_bundle": {
        "bundle_type": "life_path_bundle",
        "title": "Life Path Bundle",
        "label": "Yaşam yönü paketi",
        "price": "₺3.290",
        "summary": "Doğum Haritası Karma’sı ve Kariyer raporlarını birlikte ele alarak kişisel yapı, doğal yetenekler ve uzun vadeli yön arasında daha net bağ kurar.",
        "included_products": ["birth_chart_karma", "career"],
        "draft_focus": "life path synthesis, birth chart karma, career direction, natural strengths",
    },
    "full_year_insight_bundle": {
        "bundle_type": "full_year_insight_bundle",
        "title": "Full Year Insight Bundle",
        "label": "Yıl ve kariyer yönü paketi",
        "price": "₺2.890",
        "summary": "Yıllık Transit ve Kariyer raporlarını birleştirerek profesyonel kararları dönemsel zamanlama ile birlikte okur.",
        "included_products": ["annual_transit", "career"],
        "draft_focus": "annual timing, career direction, transition windows, professional planning",
    },
    "deep_family_insight_bundle": {
        "bundle_type": "deep_family_insight_bundle",
        "title": "Deep Family Insight",
        "label": "Aile içgörüsü paketi",
        "price": "₺3.390",
        "summary": "Ebeveyn-Çocuk ve Doğum Haritası Karma’sı perspektifini birlikte düşünerek ilişki dinamiğini daha geniş bir kişisel yapı içinde değerlendirir.",
        "included_products": ["parent_child", "birth_chart_karma"],
        "draft_focus": "parent-child relationship, birth chart foundation, family dynamics, conscious support",
    },
    "astrology_deep_dive": {
        "bundle_type": "astrology_deep_dive",
        "title": "Astrology Deep Dive",
        "label": "En bütünlüklü deneyim",
        "price": "₺7.900",
        "summary": "İki odak raporu ve 60 dk birebir danışmanlığı birleştiren, yaşam yönünüzü en kapsamlı biçimde anlamaya yönelik premium çalışma.",
        "included_products": ["birth_chart_karma", "career", "consultation_60_min"],
        "draft_focus": "complete chart synthesis, life direction, career path, personal consultation preparation",
        "includes_consultation": True,
    },
}
ARTICLE_CATEGORY_LABELS = {
    "foundations": "Foundations",
    "timing": "Timing",
    "chart-reading": "Chart Reading",
    "life-guidance": "Life Guidance",
}
LEGACY_ARTICLE_SEED_TITLES = {
    "What Is Vedic Astrology",
    "Understanding Mahadasha Timing",
    "Jupiter in the First House",
    "How to Read Career Patterns in a Chart",
    "Saturn Periods and Life Pressure",
    "Timing vs Free Will in Vedic Astrology",
    "Venus: Iliskiler, Cekim ve Deger Algisi",
    "Merkur: Zihin, Iletisim ve Ogrenme Dili",
    "Jupiter Transiti: Acele Etme",
    "Venüs Transiti",
    "Merkür Transiti",
}
ARTICLE_SEED_CONTENT = [
    {
        "title": "Venüs Transiti: Değişim Kaçınılmaz",
        "legacy_titles": ["Venüs Transiti", "Venus: Iliskiler, Cekim ve Deger Algisi"],
        "category": "life-guidance",
        "excerpt": "Kova burcundaki Venüs transiti; ilişkilerde, sevgide ve değer verdiğimiz alanlarda özgürlük ihtiyacını daha görünür hale getiriyor.",
        "body": "VENÜS TRANSİTİ\n\nKova burcu 5 Şubat'ta Venüs transitine ev sahipliği yapacak. İlişkilere, paraya ve sevgiye bakış açımızı değiştirdiğimiz; değişimden korkmayacağımız bir ay bizi bekliyor.\n\nDEĞİŞİM KAÇINILMAZ\n\nVenüs bu ay Kova burcunda ilerlerken ilişkilerde, iletişimde ve keyif aldığımız alanlarda yeni bir düzen arayışını öne çıkarıyor. Maddi manevi değer verdiğimiz her şeyi sorgularken şu sorular gündeme geliyor: İçinde olduğumuz ilişkilerde gerçekten kendimiz olabilir miyiz? Kariyerimizde ve toplum önünde daha özgün bir biçimde var olabilir miyiz? Değişime ihtiyacımız var mı?\n\nKova burcunun tabiatı ile uyumlu ilerleyen Venüs, radikal ama özgün çıkış yolları sunuyor. Değişimden korkmadan; sevdiğimiz ve değer verdiğimiz her şeyin hayatımızdaki karşılığını daha net görüp yeni adımlar atabileceğimiz bir dönem.\n\nYÜKSELEN BURÇLARINIZA GÖRE ŞUBAT AYINDA VENÜS TRANSİTİ\n\nKOÇ\n\nBu ay özel ve iş ilişkilerinde tutumunuz özgürlük ve hayalleriniz yönünde olacak. Kariyerinizle ilgili parladığınız bu dönemde kazançlarınızı artırma ihtimali var. Gelirlerinizi nasıl harcayacağınızla ilgili planlar yapabilirsiniz. Sosyal ortamlarda yeni arkadaşlıklar kazanabilir, yeni bir ilişki içindeyseniz ilişkinin değerlerini ve yaşanma biçimini sorgulayabilirsiniz.\n\nBOĞA\n\nVenüs transiti kariyer evinizi etkiliyor. Nasıl göründüğünüzle daha çok ilgileneceğiniz bir dönemdesiniz. Fiziksel görünümünüzde değişiklikler yapabilir, özellikle iş ortamındaki ilişkilerinizi gözden geçirebilirsiniz. Günlük rutinlerinizi kendinizi merkeze alarak yeniden düzenlemek ve hedeflerinize daha yüksek motivasyonla ilerlemek mümkün.\n\nİKİZLER\n\nBu ay odağınız hem gezmekte hem öğrenmekte. Enerjiniz yüksek. Eğer öğretmenseniz öğrencileriniz hızınıza yetişmekte zorlanabilir. Aynı anda birçok şeyi yapmak isteyebilirsiniz. Uzak ülkelere seyahat planları gündeme gelebilir. Romantik ilişkilerde denge kurmak ve sosyalleşme ihtiyacınızı hobilerle desteklemek önemli olacak. Yaratıcılık gerektiren bir mesleğiniz varsa ilhamınız daha güçlü akabilir.\n\nYENGEÇ\n\nHayallerinizi ve sizi mutlu eden şeyleri tekrar hatırlayacağınız bu ay, ertelediğiniz istekler bir kriz veya içsel farkındalıkla yeniden gündeme gelebilir. Hep hayalini kurduğunuz bir gelişmenin gerçekleşmesi olası. Maddi tarafta ani bir kazanım yaşanabilir. Bu transit, hayatta sizi mutlu eden ve yoran şeyleri daha görünür kılıyor.\n\nASLAN\n\nBu ay ikili ilişkiler ön planda. Özel hayatınızda ve iş hayatınızda dikkatleri üzerinize çekebilirsiniz. Venüs Kova'da ilerlediği için ilişkilerde özgürlük teması baskın. Sosyal ortamlarda yaratıcı ve gösterişli tarzınızla öne çıkabilirsiniz. Partnerinize hediyeler almak veya mutluluğunuzu görünür kılmak isteyebilirsiniz; ancak ölçüyü kaçırmamaya dikkat edin.\n\nBAŞAK\n\nGünlük rutinleriniz, düzeniniz ve sağlığınız ön planda. Hem iş akışınızı hem de bedeninizin verdiği sinyalleri daha dikkatle dinlemeniz gereken bir dönem. Küçük ama düzenli değişiklikler daha iyi hissettirebilir.\n\nTERAZİ\n\nVenüs'ün Kova transiti aşk, romantik ilişkiler ve hobileri öne çıkarıyor. Daha çok eğlenmek isteyeceğiniz bu dönemde ilişkilerde bunalmış hissediyorsanız biraz alan açma ihtiyacı duyabilirsiniz. Kendinize ne kadar zaman ayırdığınız, kazancınızı nereye harcadığınız ve keyif kavramını nasıl yaşadığınız içsel muhasebe konusu olabilir. Çocuklarınız varsa onların isteklerine ve hobilerine daha çok önem verebilirsiniz.\n\nAKREP\n\nBu ay Venüs transiti içsel huzurunuza, zihninize ve yaşadığınız yere dair temaları yoğunlaştırıyor. Aileyle ilgili eski konular yeniden gündeme gelebilir. Kalabalık ortamlara girmek yerine kendinizle baş başa kalmak isteyebilirsiniz. İç dünyanızı dinlemek ve eski duyguları fark etmek önemli olabilir.\n\nYAY\n\nYakın çevrenizle kısa yolculuklar ve sosyal planlar gündeme gelebilir. Neşenizin arttığı bu dönemde yakın çevrenizden biriyle ilişkiniz farklı bir boyut kazanabilir. Hayallerinizi anlatırken daha heyecanlı ve motive hissedebilirsiniz. İş arkadaşlarıyla yapılan paylaşımlar yeni fırsatlar getirebilir.\n\nOĞLAK\n\nGelir-gider dengenizin öne çıktığı bu ay, kariyeriniz ve kazançlarınız gündemde. Kazançlarınızı artırmakla ilgili ciddi planlar yapabilirsiniz. Hobilerden ya da sosyal çevreden gelen değer artışı mümkün. Dengeyi iyi kurmak ve savurganlıktan kaçınmak önemli.\n\nKOVA\n\nVenüs bu ay sizin burcunuzdan transit ediyor ve dikkat çekiciliğinizi yükseltiyor. Özellikle ailenizle olan bağlarınızı, mutluluğunuzu ve özgürlük ihtiyacınızı yeniden sorgulayabilirsiniz. Fiziksel görünümünüzde ya da yaşadığınız yerde değişiklik yapmak isteyebilirsiniz. Yurtdışı bağlantılı işlerde veya görünürlük gerektiren alanlarda artış olabilir.\n\nBALIK\n\nKendinizle kalmak isteyeceğiniz, bazı içsel sıkışmaları çözmek için çaba göstereceğiniz bir dönem. Yakın çevrenizden saklanan bir konuyu öğrenebilirsiniz. Venüs transitinin daha içsel ve sorgulayıcı çalıştığı bu süreçte rüyalar ve sezgiler daha güçlü mesajlar taşıyabilir.",
        "author_name": "Focus Astrology",
        "reading_time": 9,
        "language": "tr",
        "published_at": datetime(2026, 2, 1),
    },
    {
        "title": "Merkür Transiti: Planla ve Harekete Geç",
        "legacy_titles": ["Merkür Transiti", "Merkur: Zihin, Iletisim ve Ogrenme Dili"],
        "category": "chart-reading",
        "excerpt": "Merkür bu yıl düşünme biçimimizi, iletişim dilimizi ve karar alma hızımızı sık sık yeniden düzenlemeye çağırıyor.",
        "body": "MERKÜR TRANSİTİ\n\nOcak ayının ilk günlerinde kendimizi daha çok hayal kurarken, düşünürken ve konuşurken bulabiliriz. Gökyüzünün enerjisi zihinsel olarak hepimize mesaj taşıyan tohumlar ekiyor. Bu yıl Merkür transitinde bizi neler bekliyor?\n\nPLANLA VE HAREKETE GEÇ\n\nYılın ilk 7 gününde Yay burcunda transit eden Merkür bizi daha derin düşüncelere itiyor. Ardından Oğlak burcuna geçerek ayaklarımızın yere daha sağlam basmasını istiyor. Oğlak doğası gereği sorumluluk alacağımız bu dönem; hayattan ne istediğimizi, neyi düşündüğümüz halde yapamadığımızı ve hangi planların sonuç vermediğini daha net görmemizi sağlıyor.\n\nMerkür bu yıl öyle etkiler bırakacak ki olmayanı oldurmak, başaramadığımızı farklı yollarla denemek ve yeni fırsatları daha akılcı değerlendirmek isteyeceğiz. Mantık, pratik düşünme ve akılcı yaklaşım; kafamıza koyduğumuzu gerçekleştirmek için daha güçlü bir zemin sunuyor.\n\nMERKÜR RETRO NELER YAŞATACAK?\n\nBir yıl boyunca her burçta transit edecek olan Merkür, 4 defa retro hareket yapacak. İlk retro 26 Şubat'ta Kova burcunda başlıyor. Bu dönem teknolojik aksaklıklar, internet ve sosyal medya kaynaklı kopukluklar yaratabilir.\n\n29 Haziran'da Yengeç burcundaki retro Merkür; aile, yaşadığınız yer ve geçmiş konuları yeniden gündeme getirebilir. Duyguların kontrolden çıkması, yanlış anlaşılmalar ve tepkisel cevaplar artabilir. Ayrıca elektronik aletlerde ya da araçlarda teknik sorunlar yaşanabilir.\n\n7 Temmuz'da İkizler burcundaki retro Merkür en güçlü etkilerden biri. İletişimde acele, yanlış anlaşılma, yanlış kişiye giden mesajlar, adres karışıklıkları ve bilgi kirliliği öne çıkabilir. Telefon, tablet ve bilgisayar gibi iletişim araçları da daha hassas çalışabilir.\n\n24 Eylül'de Terazi burcundaki retro Merkür ise adalet, ortaklıklar, ilişkiler, hukuki ve diplomatik süreçlerde gecikmeler getirebilir. Bu dönem ortak hesaplaşmalar ve hassas yazışmalar daha dikkatli yürütülmeli.\n\nBU AY MERKÜR TRANSİTİNİN YÜKSELEN BURCUNUZA ETKİLERİ\n\nKOÇ\n\nYılın ilk günleri eğitim ve inançlarla ilgili merakınızı artırabilir. Daha çok şey öğrenmek ya da öğretmek isteyebilirsiniz. Yurtdışı bağlantılı eğitim konularında netleşmeler olabilir. Ayın sonlarına doğru fikirleriniz netleşirken kariyerinizle ilgili daha sağlam adımlar atabilirsiniz. Özellikle iş ortamındaki konuşmalarda kelimelerinizi özenle seçmek faydalı olur.\n\nBOĞA\n\nMaddi kaynaklarınızı artırmakla ilgili düşünceler zihninizi meşgul edebilir. Ayın sonuna doğru uzun süredir kafanızda dönen planlar daha netleşir. Ani borçlanmalar yerine uzun vadeli ve düşük riskli kararlar vermek daha sağlıklı olur.\n\nİKİZLER\n\nMerkür sizin yönetici gezegeniniz olduğu için bu transiti çok güçlü hissedeceğiniz bir dönem. İlişkilerinizle ilgili içsel hesaplaşmalar yaşayabilirsiniz. İş ortaklarınız ya da partnerinizle yapılacak konuşmalarda acele karar vermemek önemli.\n\nYENGEÇ\n\nGünlük hayat temponuz, çalışma düzeniniz ve sağlık konuları zihninizi meşgul ederken ilişkilerinizde daha ciddi kararlar almak isteyebilirsiniz. Bu süreçte sınırlarınızı yeniden tanımlayıp kendinizi daha net ifade etmeniz mümkün.\n\nASLAN\n\nYılın ilk günlerinde romantik ilişkiler, hobiler ve keyif alanları öne çıkıyor. Ay ortasından sonra hayatınızdaki sorumluluklar daha fazla görünür olabilir. Bu ay bitmeden kendinize yeni bir rutin belirlemek iyi gelebilir.\n\nBAŞAK\n\nEv, içsel huzur ve aile temaları ön planda. Çocuklarınız ya da romantik ilişkileriniz gündemde olabilir. Kendinizi daha rahat ifade etmeye başladığınız bu süreçte yaratıcı projelere yönelmek verimli olur.\n\nTERAZİ\n\nKardeşleriniz ve yakın çevrenizle iletişiminiz artıyor. Kısa yolculuk planları gündeme gelebilir. Ay ortasından sonra annenizle, ailenizle ya da yaşadığınız yerle ilgili yeni kararlar alabilirsiniz.\n\nAKREP\n\nYılın ilk günlerinde para ve değer konularında hareketlilik olabilir. Maddi ve manevi değerlere bakışınız değişiyor. Harcamalarda daha disiplinli davranmak ve kaynakları dikkatle yönetmek öncelik kazanıyor.\n\nYAY\n\nMerkür sizin burcunuzda olduğu için fikirlerinizi anlatma isteğiniz çok yüksek. Ay ortasından itibaren para kazanma ve harcama biçiminizle ilgili daha emin adımlar atabilirsiniz.\n\nOĞLAK\n\nAyın başında zihniniz biraz dağınık olabilir. Merkür kısa süre sonra sizin burcunuza geçerek kendinizle ilgili daha net kararlar almanızı sağlar. Öncesinde kısa bir dinlenme molası, zihinsel toparlanmaya yardımcı olabilir.\n\nKOVA\n\nArkadaşlarınız, hayalleriniz ve kazançlarınızla ilgili bir süreçten geçiyorsunuz. Kafanız karışmış hissedebilirsiniz. Karar almadan önce biraz yalnız kalmak ve kendi iç sesinizi dinlemek daha doğru olur.\n\nBALIK\n\nYılın ilk günlerinde iş ve kariyer odaklı düşünceler çok yoğun olabilir. Belki de yabancı ülkelerde kazanç sağlama hayalleri kuruyorsunuz. Ayın sonlarına doğru hayalleri daha somut adımlara dönüştürmek mümkün olacak.",
        "author_name": "Focus Astrology",
        "reading_time": 10,
        "language": "tr",
        "published_at": datetime(2026, 1, 3),
    },
    {
        "title": "Jüpiter Transiti: Jüpiter \"Acele Etme\" Diyor",
        "legacy_titles": ["Jupiter Transiti: Acele Etme"],
        "category": "timing",
        "excerpt": "İkizler burcundaki Jüpiter retrosu, hızdan çok gözlem ve yeniden değerlendirme çağrısı yapıyor.",
        "body": "JÜPİTER TRANSİTİ: JÜPİTER \"ACELE ETME\" DİYOR\n\nGökyüzünün en iyicil gezegeni olan Jüpiter 5 Aralık'ta retro hareketine başlıyor. İkizler burcunda retro olan Jüpiter 11 Mart'a kadar bu hareketini sürdürüyor. Peki Jüpiter'in İkizler burcundaki ziyareti bize ne anlatmak istiyor?\n\nRETRO VE PUNARVASU ETKİLERİ\n\nRetro Jüpiter İkizler burcundayken geçmişten gelen kişiler, olaylar ve gündemler tekrar karşımıza çıkabilir. Bu süreçte olabildiğince gözlemci kalmak yararımıza olacaktır. Jüpiter'in ilk bakışta fırsat gibi görünen bazı vaatleri, acele edildiğinde yanıltıcı olabilir. Gezegen retro konumda gücünü içe çeker; ödül etkisi ise daha çok retro çıkışında belirginleşir.\n\nİkizler burcunun takıntılı ve zihinsel olarak dağılmaya açık yapısı da bu süreçte devrede olabilir. Punarvasu etkisi ise yeniden doğuşu, affetmeyi ve hayatımızda yeni bir düzen kurmayı anlatır. Bu yüzden bazen fırtınanın dinmesini beklemek, en doğru büyüme stratejisi olur.\n\nJÜPİTER'İN YÜKSELEN BURÇLARA GÖRE ETKİLERİ\n\nKOÇ\n\nYurtdışı, kısa-uzun yolculuklar ve yabancılarla bağlantılı konular gündeme gelebilir. Planlarınızı tekrar gözden geçirmek isteyebilirsiniz. Eğitim konusu öne çıkabilir. İletişimde daha dikkatli olmanız gereken bir süreç olabilir.\n\nBOĞA\n\nGelir kaynaklarınızla ilgili sıkıntılar veya yeniden yapılandırma ihtiyacı oluşabilir. Nasıl para kazanacağınız sorusu daha çok gündeme gelir. Maddi gelirlerde gecikmeler, beklenmedik ödemeler ya da kredi-vergi başlıkları zihni meşgul edebilir. Sosyal çevrenizden destek almak çözüm sağlayabilir.\n\nİKİZLER\n\nJüpiter sizin birinci evinizde retro yaparken kendinizle ilgili önemli farkındalıklar yaşayabilirsiniz. Fiziksel görünümünüzde değişiklik yapma isteği doğabilir. Kendinize zaman ayırmanız gereken bir dönem.\n\nYENGEÇ\n\nİçe dönme, yalnız kalma ve kendinizle baş başa kalma ihtiyacı hissedebilirsiniz. Geçmişten gelen bazı konular yeniden gündeme gelebilir. Ruhsal çalışmalar için uygun bir dönem.\n\nASLAN\n\nYaratıcılık, aşk, çocuklar ve sosyal çevreyle ilgili konular öne çıkabilir. Eski arkadaşlarla karşılaşmalar ve geçmişten gelen ilişkiler gündeme gelebilir. Geçmişte sevdiğiniz bir hobiye yeniden dönmek de mümkün.\n\nBAŞAK\n\nKariyer, aile hayatı ve ilişkilerde geçmiş gündemler önünüze geliyor. Beklediğiniz bir teklif gecikebilir ya da eski işinizle ilgili yeni bir değerlendirme alanı doğabilir.\n\nTERAZİ\n\nYurtdışı bağlantılı tatil, eğitim veya kazanç planlarında gecikmeler olabilir. İçsel enerjinizi ve inançlarınızı daha çok sorgulayabilirsiniz. Eğer evliyseniz eşinizle ilgili gündemler de öne çıkabilir.\n\nAKREP\n\nİlk çocukluk anıları, maddi-manevi değerler ve bilinçaltı temalar gündeme gelebilir. Özellikle bu dönemde sizi zorlayan meselelerle yüzleşmek gerekebilir.\n\nYAY\n\nJüpiter retrosu size ihmal ettiğiniz kişisel sorunları çözmenizi hatırlatıyor. Yakın çevre, kardeşler ve iletişim başlıkları öne çıkabilir. İlişkilerde ve ortaklıklarda eski meseleler yeniden gündeme gelebilir.\n\nOĞLAK\n\nGünlük hayat düzeniniz, iş temponuz, sağlığınız ve varsa evcil hayvanlarınız daha fazla dikkat isteyebilir. Yaşam düzenini sadeleştirmek iyi gelebilir.\n\nKOVA\n\nYatırım yapmak istediğiniz bir proje varsa retro döneminde acele adım atmak yerine planı biraz daha geliştirmek iyi olur. Sosyal çevreyle şekillenen projelerde tekrar değerlendirme gerekli olabilir.\n\nBALIK\n\nJüpiter'in dördüncü evinizdeki retro hareketi aile, ev ve geçmişle ilgili konuları yeniden hatırlatabilir. Aileyle ilgili gelişmeler ya da ev içi meseleler tekrar gündeme gelebilir.",
        "author_name": "Focus Astrology",
        "reading_time": 9,
        "language": "tr",
        "published_at": datetime(2025, 12, 3),
    },
]

ARTICLE_LOCALIZED_CONTENT = {
    "venus-transiti-degisim-kacinilmaz": {
        "en": {
            "title": "Venus Transit: Change Is Unavoidable",
            "excerpt": "Venus moving through Aquarius brings freedom, honesty, and a new relationship with what you truly value.",
            "body": """VENUS TRANSIT

Aquarius becomes the host of Venus on February 5. It is a month that asks us to change the way we relate to love, money, beauty, and emotional value without being afraid of what must evolve.

CHANGE IS UNAVOIDABLE

As Venus travels through Aquarius, relationships, communication, and the parts of life we enjoy begin asking for a new order. We may question what we value emotionally and materially: can we really be ourselves in the relationships we are in, and can we show up more honestly in our public and professional life? This transit invites change where authenticity has been missing.

Aquarius gives Venus a more independent and future-facing tone. It is not change for the sake of disruption. It is change that helps you see more clearly what still has value, what needs distance, and what is ready for a more truthful form.

VENUS TRANSIT IN FEBRUARY BY RISING SIGN

ARIES

This month, both personal and professional relationships may revolve around freedom and long-term hopes. Career visibility can rise, and you may begin making plans about how to use growing income more wisely. Social circles can bring new connections, and if you are in a new relationship, you may start questioning the real values that hold it together.

TAURUS

Venus activates your career house. You may care more about how you are perceived and may want to refresh your appearance or public image. Work relationships can come under review. Reorganizing daily routines around your own priorities can help you move toward goals with stronger motivation.

GEMINI

This month your attention turns toward travel, learning, and curiosity. Energy is high, and you may want to do many things at once. Long-distance plans can become more exciting. In relationships, balance matters; hobbies and social life can help you stay centered. If your work is creative, inspiration may flow more easily.

CANCER

This is a month of remembering what truly makes you happy. Desires that were postponed may return through a crisis or awakening. A long-imagined development may finally begin to move. Financially, sudden support or gain is possible. Venus helps you see more clearly both what nourishes you and what drains you.

LEO

Partnerships are emphasized. In both love and work, you may draw attention easily. Since Venus is moving through Aquarius, freedom becomes a central relationship theme. In social settings your creative and expressive side can shine, but avoid overdoing spending or dramatic gestures just to prove affection.

VIRGO

Daily routines, order, and well-being come into focus. It is a period to listen more carefully to both workflow and the signals of your body. Small but steady changes can improve how you feel. Better structure brings more ease than intense effort.

LIBRA

Venus in Aquarius highlights romance, creativity, pleasure, and hobbies. You may want more joy and more room to breathe. If relationships have begun to feel confining, you may need distance in order to feel sincere again. Questions about where your money, time, and pleasure go can become more visible.

SCORPIO

This transit intensifies themes around inner peace, the home, and emotional foundations. Old family topics may return. Instead of entering crowded or noisy environments, you may prefer privacy and reflection. Listening to your inner world can reveal what still needs tenderness or release.

SAGITTARIUS

Short trips, nearby connections, and social plans become more active. Your mood may feel lighter, and someone from your close environment could begin to matter in a new way. Conversations about your dreams can be more inspiring now, and exchanges with colleagues may open practical opportunities.

CAPRICORN

Income, spending, and self-worth come into focus. Career and financial planning can become more serious. There may be chances to increase what you earn, especially through social networks or personal talents. The key is balance: grow your resources, but stay clear of unnecessary excess.

AQUARIUS

Venus moves through your sign this month, increasing your magnetism and visibility. You may question happiness, freedom, family ties, and the life shape that really suits you. Changes to your appearance or living environment may appeal to you. International or highly visible work can gain momentum.

PISCES

This is a more inward period. You may want time alone to process emotional tension or quiet realizations. Something hidden in your environment may become clear. Dreams, intuition, and subtle emotional signals can carry stronger messages now if you slow down enough to hear them.""",
        }
    },
    "merkur-transiti-planla-ve-harekete-gec": {
        "en": {
            "title": "Mercury Transit: Plan and Take Action",
            "excerpt": "Mercury asks us to rethink how we think, speak, organize, and act so our plans can finally become real.",
            "body": """MERCURY TRANSIT

In the first days of January, many of us may find ourselves thinking, imagining, and speaking more than usual. The sky is planting mental seeds that will shape how we plan, communicate, and move through the year. So what does Mercury's journey ask from us now?

PLAN AND TAKE ACTION

Mercury begins the month in Sagittarius, encouraging broader thought, meaning, and perspective. Soon after, it moves into Capricorn and asks us to become more grounded. This shift helps us see what we truly want, what we have postponed for too long, and which plans have not delivered real results.

Mercury this year encourages practical intelligence. We may want to make the impossible more manageable, try again with better methods, and approach opportunities with more reason and structure. Logic, timing, and realistic execution become the strongest allies.

WHAT WILL MERCURY RETRO BRING?

Mercury will move through every sign over the course of the year and will turn retrograde four times. The first retrograde begins on February 26 in Aquarius. This period may bring technological issues, internet disruptions, and communication gaps in social or digital spaces.

On June 29, Mercury retrograde in Cancer can reactivate family matters, home concerns, or the emotional past. Feelings may become harder to regulate, misunderstandings may increase, and technical problems involving vehicles or electronics are more likely.

On July 7, Mercury retrograde in Gemini becomes one of the most noticeable periods: rushed speech, mixed messages, confusion of addresses or information, and communication overload can all rise. Phones, tablets, and computers may also feel more fragile.

On September 24, Mercury retrograde in Libra may delay justice-related matters, partnerships, negotiations, and delicate written exchanges. This is a time to move carefully in agreements and not rush diplomatic or legal decisions.

THIS MONTH'S MERCURY TRANSIT BY RISING SIGN

ARIES

The first days of the year may increase your interest in education, belief systems, and expanding your horizon. International or academic matters can become clearer. By the end of the month, your ideas may settle into stronger career decisions. Choose your words carefully in professional settings.

TAURUS

Thoughts around money, security, and how to improve your resources can take up more mental space. By the end of the month, plans that have been circling in your mind may begin to take clearer form. Avoid impulsive debt or risky financial moves; steady thinking serves you better.

GEMINI

Because Mercury is your ruling planet, you may feel this transit strongly. Relationship dynamics may bring internal questioning. Conversations with a partner or business ally require patience. The right words matter more than quick conclusions.

CANCER

Your schedule, work flow, and health concerns may occupy your mind more than usual. At the same time, relationships may require more mature decisions. This is a useful period for redefining boundaries and expressing yourself with more clarity.

LEO

Romance, pleasure, hobbies, and personal joy stand out at the beginning of the month. Later on, responsibilities may grow more visible. Before the month ends, setting a new routine can help you channel your energy more effectively.

VIRGO

Home, family, and emotional foundations become central themes. Children or romantic concerns may also draw attention. As self-expression becomes easier, creative projects can benefit from your renewed clarity and order.

LIBRA

Communication with siblings, neighbors, and your close environment may increase. Short trips and quick plans can arise. Later in the month, you may make fresh decisions regarding family, home, or the place you live.

SCORPIO

Money, value, and self-worth may feel more active in the first days of the year. Your relationship with material and emotional resources is changing. Greater discipline around spending and clearer resource management become important.

SAGITTARIUS

With Mercury moving through your sign early in the month, your urge to speak, explain, and define your point of view is strong. From mid-month onward, you may begin making more confident decisions around income, value, and how you use your energy.

CAPRICORN

At the beginning of the month, the mind may feel slightly scattered. Soon Mercury enters your sign and helps you think more clearly about yourself and your direction. Before that, a short pause for rest and mental reset can be especially helpful.

AQUARIUS

You may be moving through a period focused on friends, long-term hopes, and income. Confusion is possible. Before forcing decisions, it may be wiser to spend time alone and listen for your own inner signal rather than everyone else's noise.

PISCES

Work and career thoughts may dominate the first days of the year. You may be imagining opportunities connected to international income or a wider audience. By the end of the month, it becomes easier to turn those dreams into practical steps.""",
        }
    },
    "jupiter-transiti-jupiter-acele-etme-diyor": {
        "en": {
            "title": "Jupiter Transit: Jupiter Says \"Don't Rush\"",
            "excerpt": "Jupiter retrograde in Gemini asks for observation, review, and wiser timing instead of immediate expansion.",
            "body": """JUPITER TRANSIT: JUPITER SAYS "DON'T RUSH"

Jupiter, the most benefic planet in the sky, begins its retrograde motion on December 5. It remains retrograde in Gemini until March 11. So what is Jupiter trying to teach us through this slower, more reflective passage?

RETROGRADE AND PUNARVASU THEMES

While Jupiter is retrograde in Gemini, people, events, and unfinished questions from the past may return. It is wiser to stay observant than reactive. Some promises that look like opportunities at first glance may prove misleading if handled too quickly. In retrograde, Jupiter pulls its strength inward; the reward often becomes more visible after the retrograde has completed.

Gemini can scatter the mind or create over-analysis, and that quality may be amplified now. Punarvasu, however, carries the symbolism of renewal, forgiveness, and rebuilding life with more integrity. Sometimes the most intelligent growth strategy is to wait for the storm to settle before acting.

JUPITER'S EFFECTS BY RISING SIGN

ARIES

Topics related to travel, foreign connections, and both short and long journeys can become active. You may want to revisit plans or educational choices. Communication deserves more care during this period, especially when expectations are moving faster than facts.

TAURUS

Questions around income, stability, and financial restructuring may become more urgent. Delays, unexpected expenses, taxes, or credit matters may demand attention. Support from friends or your wider network can be part of the solution, but patience remains essential.

GEMINI

With Jupiter retrograding through your first house, important personal realizations may arise. You may want to change your appearance or the way you present yourself. This is a period to spend more time on your own development instead of trying to prove forward movement too quickly.

CANCER

You may feel the need to withdraw, reflect, and spend time with yourself. Topics from the past can return for emotional review. This is a supportive period for spiritual practices, rest, and deeper inner listening.

LEO

Creativity, romance, children, and social connections can become more visible. Encounters with old friends or past relationships are possible. You may also return to a hobby or passion that once brought genuine joy.

VIRGO

Career matters, family matters, and relationship dynamics may bring old themes back to the surface. A proposal you were waiting for could be delayed, or a previous professional issue may need to be reassessed. Retrograde motion asks for evaluation before expansion.

LIBRA

Travel, international plans, education, and long-range goals may slow down or require revision. You may question your beliefs or your inner motivation more deeply. If you are married or closely partnered, your partner's situation may also become part of the story.

SCORPIO

Early life memories, shared resources, deeper emotional material, and unconscious patterns may become more visible. This period can require honest confrontation with what has been avoided, especially in matters of trust and control.

SAGITTARIUS

Jupiter retrograde reminds you to address personal issues you may have postponed. Relationships, communication, siblings, or close-environment matters can move back to the foreground. Old topics in partnership may return for resolution rather than repetition.

CAPRICORN

Your daily schedule, work pace, health habits, and even responsibilities involving pets may require more care. Simplifying your routine can be more effective now than taking on additional complexity.

AQUARIUS

If there is a project or investment you want to launch, it may be wiser to refine the plan than to rush the move while Jupiter is retrograde. Projects shaped by social circles or group dynamics may need reevaluation before they can grow well.

PISCES

Jupiter retrograde in your fourth house can bring attention back to home, family, and the emotional past. Family developments or domestic matters may reappear. The lesson is not to force resolution, but to understand what your inner foundation truly needs.""",
        }
    },
}
ALLOWED_BIRTHPLACE_EVENTS = {
    "suggestion_results_returned",
    "suggestion_selected",
    "submit_with_selected_suggestion",
    "submit_without_selected_suggestion",
    "ambiguous_or_low_confidence_birthplace",
    "stale_resolved_payload_discarded",
    "resolved_birthplace_success",
    "resolved_birthplace_failure",
}
PLAN_FEATURES = {
    "free": {
        "label": "Free",
        "allowed_report_types": ["preview", "parent_child"],
        "pdf_export": False,
        "max_saved_reports": 5,
        "advanced_history": False,
        "elite_guidance": False,
    },
    "basic": {
        "label": "Basic",
        "allowed_report_types": ["preview", "basic", "parent_child"],
        "pdf_export": True,
        "max_saved_reports": 20,
        "advanced_history": True,
        "elite_guidance": False,
    },
    "premium": {
        "label": "Premium",
        "allowed_report_types": ["preview", "basic", "premium", "parent_child"],
        "pdf_export": True,
        "max_saved_reports": 100,
        "advanced_history": True,
        "elite_guidance": False,
    },
    "elite": {
        "label": "Elite",
        "allowed_report_types": ["preview", "basic", "premium", "elite", "parent_child"],
        "pdf_export": True,
        "max_saved_reports": 500,
        "advanced_history": True,
        "elite_guidance": True,
    },
}
PLAN_ORDER = {"free": 0, "basic": 1, "premium": 2, "elite": 3}
REPORT_ACCESS_STATES = ("preview", "unlocked", "purchased", "delivered")
FULL_REPORT_ACCESS_STATES = {"unlocked", "purchased", "delivered"}
PDF_ALLOWED_ACCESS_STATES = {"unlocked", "purchased", "delivered"}
EMAIL_CAPTURE_SOURCES = {"result_page", "preview_gate", "bottom_cta"}
FEEDBACK_STAGES = {"preview", "full"}
PREVIEW_FEEDBACK_RATINGS = {"very_accurate", "somewhat", "not_really"}
FULL_FEEDBACK_RATINGS = {"very_helpful", "somewhat_helpful", "not_helpful"}
PREVIEW_CONTENT_LIMITS = {
    "anchors": 1,
    "recommendations": 1,
    "ai_sections": 1,
    "ai_blocks_per_section": 2,
}
_ADMIN_ALLOWLIST_CACHE = None
_ADMIN_ALLOWLIST_LOGGED = False
ARTICLE_SLUG_CHAR_MAP = str.maketrans(
    {
        "\u00e7": "c",
        "\u00c7": "c",
        "\u011f": "g",
        "\u011e": "g",
        "\u0131": "i",
        "\u0130": "i",
        "\u00f6": "o",
        "\u00d6": "o",
        "\u015f": "s",
        "\u015e": "s",
        "\u00fc": "u",
        "\u00dc": "u",
    }
)


def get_db():
    db = db_mod.SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _safe_model_attr(instance, attr_name, default=None):
    if instance is None:
        return default
    try:
        state = getattr(instance, "__dict__", {})
        if attr_name in state:
            return state.get(attr_name, default)
    except Exception:
        pass
    if attr_name == "id":
        try:
            identity = sa_inspect(instance).identity
            if identity:
                return identity[0]
        except Exception:
            pass
    try:
        return getattr(instance, attr_name)
    except Exception:
        return default


def _user_id(user):
    return _safe_model_attr(user, "id")


def _public_user_view(user):
    if not user:
        return None
    email = _safe_model_attr(user, "email", "") or ""
    name = _safe_model_attr(user, "name", None)
    return {
        "id": _user_id(user),
        "email": email,
        "name": name or email.split("@")[0],
        "plan_code": _safe_model_attr(user, "plan_code", "free"),
        "is_admin": is_admin_user(user) if "is_admin_user" in globals() else bool(getattr(user, "is_admin", False)),
        "is_active": bool(_safe_model_attr(user, "is_active", False)),
    }


def normalize_plan_code(plan_code):
    normalized = str(plan_code or "").strip().lower()
    return normalized if normalized in PLAN_FEATURES else "free"


def get_user_plan(user):
    if not user or not bool(_safe_model_attr(user, "is_active", False)):
        return "free"
    return normalize_plan_code(_safe_model_attr(user, "plan_code", "free"))


def get_plan_features(user_or_plan):
    plan_code = normalize_plan_code(user_or_plan if isinstance(user_or_plan, str) else get_user_plan(user_or_plan))
    return dict(PLAN_FEATURES[plan_code])


def get_request_user(request, db):
    user_id = request.session.get("user_id") if "session" in request.scope else None
    if not user_id:
        return None
    user = db.query(db_mod.AppUser).filter(db_mod.AppUser.id == user_id, db_mod.AppUser.is_active.is_(True)).first()
    return user


def _request_user_from_signed_cookie(request, db):
    raw_cookie = request.cookies.get("session")
    if not raw_cookie:
        return None
    try:
        signer = TimestampSigner(str(SESSION_SECRET_KEY))
        unsigned = signer.unsign(raw_cookie.encode("utf-8"), max_age=int(timedelta(days=30).total_seconds()))
        payload = json.loads(base64.b64decode(unsigned))
        user_id = payload.get("user_id")
    except (BadSignature, ValueError, TypeError, json.JSONDecodeError):
        return None
    if not user_id:
        return None
    return db.query(db_mod.AppUser).filter(db_mod.AppUser.id == user_id, db_mod.AppUser.is_active.is_(True)).first()


def _admin_email_allowlist():
    global _ADMIN_ALLOWLIST_CACHE, _ADMIN_ALLOWLIST_LOGGED

    raw = os.getenv("ADMIN_EMAILS", "")
    normalized_entries = tuple(sorted({item.strip().lower() for item in raw.split(",") if item.strip()}))
    if _ADMIN_ALLOWLIST_CACHE != normalized_entries:
        _ADMIN_ALLOWLIST_CACHE = normalized_entries
        _ADMIN_ALLOWLIST_LOGGED = False
    if not _ADMIN_ALLOWLIST_LOGGED:
        logger.info("Admin allowlist loaded count=%s", len(_ADMIN_ALLOWLIST_CACHE))
        if _ADMIN_ALLOWLIST_CACHE:
            logger.debug("Admin allowlist entries=%s", list(_ADMIN_ALLOWLIST_CACHE))
        _ADMIN_ALLOWLIST_LOGGED = True
    return set(_ADMIN_ALLOWLIST_CACHE)


def _safe_next_path(value, default="/dashboard"):
    candidate = str(value or "").strip()
    if not candidate:
        return default
    if not candidate.startswith("/") or candidate.startswith("//"):
        return default
    if "://" in candidate:
        return default
    return candidate


def _client_ip(request):
    try:
        if TRUST_PROXY:
            cf_ip = request.headers.get("cf-connecting-ip", "").strip()
            if cf_ip:
                return cf_ip
            forwarded = request.headers.get("x-forwarded-for", "").strip()
            if forwarded:
                return forwarded.split(",")[0].strip()
            real_ip = request.headers.get("x-real-ip", "").strip()
            if real_ip:
                return real_ip
        if request.client and getattr(request.client, "host", None):
            return request.client.host
    except Exception:
        pass
    return "-"


_RATE_LIMIT_BUCKETS = {}
CSRF_SESSION_KEY = "_csrf_token"
CSRF_FORM_FIELD = "csrf_token"
LOGIN_CSRF_ERROR_MESSAGE = "Güvenlik oturumu süresi doldu. Lütfen sayfayı yenileyip tekrar deneyin."
APP_VERSION = "csrf-v4-raw"
LOGIN_TEMPLATE_VERSION = "csrf-v4-raw"


def enforce_rate_limit(request, scope, limit=10, window_seconds=600):
    """Small per-process abuse guard for expensive MVP routes; not a distributed quota."""
    now = time.monotonic()
    client_ip = _client_ip(request)
    key = (str(scope or "default"), client_ip)
    window_start = now - max(int(window_seconds or 1), 1)
    timestamps = [ts for ts in _RATE_LIMIT_BUCKETS.get(key, []) if ts >= window_start]
    if len(timestamps) >= int(limit):
        logger.warning("Rate limit exceeded scope=%s client_ip=%s limit=%s window=%s", scope, client_ip, limit, window_seconds)
        raise HTTPException(status_code=429, detail="Too many requests. Please try again shortly.")
    timestamps.append(now)
    _RATE_LIMIT_BUCKETS[key] = timestamps

    if len(_RATE_LIMIT_BUCKETS) > 1000:
        for bucket_key, bucket_values in list(_RATE_LIMIT_BUCKETS.items()):
            recent_values = [ts for ts in bucket_values if ts >= window_start]
            if recent_values:
                _RATE_LIMIT_BUCKETS[bucket_key] = recent_values
            else:
                _RATE_LIMIT_BUCKETS.pop(bucket_key, None)


def check_rate_limit(key: str, max_calls: int, window_seconds: int) -> bool:
    now = time.monotonic()
    window_start = now - max(int(window_seconds or 1), 1)
    calls = [ts for ts in _RATE_LIMIT_BUCKETS.get(key, []) if ts >= window_start]
    _RATE_LIMIT_BUCKETS[key] = calls
    if len(calls) >= int(max_calls):
        return False
    calls.append(now)
    _RATE_LIMIT_BUCKETS[key] = calls
    return True


def ensure_csrf_token(request):
    if "session" not in getattr(request, "scope", {}):
        return ""
    token = request.session.get(CSRF_SESSION_KEY)
    if not token:
        token = secrets.token_urlsafe(32)
        request.session[CSRF_SESSION_KEY] = token
    return token


def get_csrf_token(request: Request) -> str:
    return ensure_csrf_token(request)


def verify_csrf_token(request: Request, form_token: str) -> bool:
    session_token = request.session.get(CSRF_SESSION_KEY, "") if "session" in getattr(request, "scope", {}) else ""
    if not session_token:
        raw_cookie = request.cookies.get("session")
        try:
            if raw_cookie:
                signer = TimestampSigner(str(SESSION_SECRET_KEY))
                unsigned = signer.unsign(raw_cookie.encode("utf-8"), max_age=int(timedelta(days=30).total_seconds()))
                payload = json.loads(base64.b64decode(unsigned))
                session_token = payload.get(CSRF_SESSION_KEY, "")
        except (BadSignature, ValueError, TypeError, json.JSONDecodeError):
            session_token = ""
    return bool(session_token) and secrets.compare_digest(str(session_token), str(form_token or ""))


def _csrf_token_from_body(request: Request, body: bytes) -> str:
    content_type = str(request.headers.get("content-type", "")).lower()
    if "application/x-www-form-urlencoded" in content_type:
        parsed = parse_qs(body.decode("utf-8", errors="ignore"), keep_blank_values=True)
        values = parsed.get(CSRF_FORM_FIELD) or []
        return str(values[0] if values else "")
    if "multipart/form-data" in content_type:
        match = re.search(
            rb'name=["\']csrf_token["\'][^\r\n]*(?:\r?\n){2}([^\r\n]*)',
            body,
            flags=re.IGNORECASE,
        )
        return match.group(1).decode("utf-8", errors="ignore").strip() if match else ""
    return str(request.headers.get("x-csrf-token") or request.headers.get("x-focus-csrf-token") or "")


async def validate_csrf_token(request, submitted_token=None):
    expected = ensure_csrf_token(request)
    candidate = (
        str(submitted_token or "").strip()
        or str(request.headers.get("x-csrf-token", "")).strip()
        or str(request.headers.get("x-focus-csrf-token", "")).strip()
    )
    if not candidate:
        content_type = str(request.headers.get("content-type", "")).lower()
        if "application/x-www-form-urlencoded" in content_type or "multipart/form-data" in content_type:
            try:
                form = await request.form()
                candidate = str(form.get(CSRF_FORM_FIELD, "") or "").strip()
            except Exception:
                candidate = ""
    if not expected or not candidate or not hmac.compare_digest(str(expected), str(candidate)):
        logger.warning("CSRF validation failed path=%s client_ip=%s", getattr(getattr(request, "url", None), "path", "-"), _client_ip(request))
        raise HTTPException(status_code=403, detail="Invalid form token.")
    return True


templates.env.globals["csrf_token_for"] = ensure_csrf_token


def set_flash(request: Request, message: str, level: str = "success"):
    if "session" in getattr(request, "scope", {}):
        request.session[f"flash_{level}"] = message


def get_flash(request: Request) -> dict:
    if "session" not in getattr(request, "scope", {}):
        return {"flash_success": None, "flash_error": None}
    return {
        "flash_success": request.session.pop("flash_success", None),
        "flash_error": request.session.pop("flash_error", None),
    }


def _unread_contact_count():
    db = db_mod.SessionLocal()
    try:
        return db.query(db_mod.ContactMessage).filter(db_mod.ContactMessage.is_read.is_(False)).count()
    except Exception:
        return 0
    finally:
        db.close()


def _app_base_url(request=None):
    configured = str(os.getenv("APP_BASE_URL", "")).strip().rstrip("/")
    if configured:
        return configured
    if request is not None:
        try:
            return str(request.base_url).rstrip("/")
        except Exception:
            pass
    return "http://127.0.0.1:8000"


def _checkout_redirect_url(path, **query):
    base = _app_base_url()
    suffix = f"?{urlencode(query)}" if query else ""
    return f"{base}{path}{suffix}"


def _generate_order_token(prefix):
    return f"{prefix}_{secrets.token_urlsafe(18)}"


def _amount_decimal(value):
    raw = str(value or "").strip()
    raw = raw.replace("₺", "").replace("â‚º", "").replace("TRY", "").replace("TL", "").replace(" ", "")
    if "," in raw:
        raw = raw.replace(".", "").replace(",", ".")
    elif "." in raw and len(raw.rsplit(".", 1)[-1]) == 3:
        raw = raw.replace(".", "")
    else:
        raw = raw.replace(",", "")
    try:
        amount = Decimal(raw)
    except (InvalidOperation, ValueError):
        amount = Decimal("0")
    return amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _order_amount(order):
    amount = getattr(order, "amount", None)
    if amount not in (None, ""):
        return _amount_decimal(amount)
    return _amount_decimal(getattr(order, "amount_label", ""))


def _order_public_token(order):
    return getattr(order, "public_token", None) or getattr(order, "order_token", "")


def can_use_beta_free_unlock(user):
    return payments.can_use_beta_free_unlock(user)


def is_admin_user(user):
    if not user:
        return False
    if bool(_safe_model_attr(user, "is_admin", False)):
        return True
    return str(_safe_model_attr(user, "email", "")).strip().lower() in _admin_email_allowlist()


def can_access_report_type(user, report_type):
    features = get_plan_features(user)
    return normalize_report_type(report_type) in features.get("allowed_report_types", [])


def can_export_pdf(user, report_type):
    features = get_plan_features(user)
    return bool(features.get("pdf_export")) and bool(get_report_type_config(report_type)[1].get("include_pdf"))


def can_save_more_reports(user, db):
    if not user:
        return False
    features = get_plan_features(user)
    current_count = db.query(db_mod.GeneratedReport).filter(db_mod.GeneratedReport.user_id == user.id).count()
    return current_count < int(features.get("max_saved_reports", 0))


def _highest_allowed_report_type(user):
    allowed = get_plan_features(user).get("allowed_report_types", ["preview"])
    tier_order = ["preview", "basic", "premium", "elite"]
    allowed_tiers = [report_type for report_type in tier_order if report_type in allowed]
    return normalize_report_type(allowed_tiers[-1] if allowed_tiers else "preview")


def resolve_report_type_for_user(user, requested_report_type):
    requested = normalize_report_type(requested_report_type)
    if can_access_report_type(user, requested):
        return requested, None
    effective = _highest_allowed_report_type(user)
    plan_label = get_plan_features(user).get("label", "Free")
    notice = f"Planiniz {plan_label} oldugu icin rapor {effective.title()} seviyesine ayarlandi."
    return effective, notice


def normalize_report_access_state(access_state):
    normalized = str(access_state or "").strip().lower()
    return normalized if normalized in REPORT_ACCESS_STATES else "preview"


def get_report_access_state(report):
    if not report:
        return "preview"
    normalized = normalize_report_access_state(getattr(report, "access_state", None))
    if normalized != "preview":
        return normalized
    if bool(getattr(report, "is_paid", False)):
        return "purchased"
    if getattr(report, "delivered_at", None):
        return "delivered"
    if getattr(report, "unlocked_at", None):
        return "unlocked"
    return "preview"


def can_view_full_report(report):
    return get_report_access_state(report) in FULL_REPORT_ACCESS_STATES


def can_download_pdf(report):
    return get_report_access_state(report) in PDF_ALLOWED_ACCESS_STATES and bool(getattr(report, "pdf_ready", False))


def _normalize_capture_email(email):
    return str(email or "").strip().lower()


def _is_valid_capture_email(email):
    normalized = _normalize_capture_email(email)
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", normalized))


def capture_email_lead(db, *, email, report_id=None, source="result_page"):
    normalized_email = _normalize_capture_email(email)
    if not _is_valid_capture_email(normalized_email):
        raise ValueError("invalid_email")
    normalized_source = str(source or "result_page").strip().lower() or "result_page"
    if normalized_source not in EMAIL_CAPTURE_SOURCES:
        normalized_source = "result_page"
    report_id_value = int(report_id) if str(report_id or "").strip().isdigit() else None
    capture = db.query(db_mod.EmailCapture).filter(
        db_mod.EmailCapture.email == normalized_email,
        db_mod.EmailCapture.report_id == report_id_value,
    ).first()
    if capture:
        if normalized_source and not getattr(capture, "source", None):
            capture.source = normalized_source
        return capture, False
    capture = db_mod.EmailCapture(
        email=normalized_email,
        report_id=report_id_value,
        source=normalized_source,
    )
    db.add(capture)
    db.flush()
    return capture, True


def mark_email_capture_converted(db, *, report=None, email=None):
    normalized_email = _normalize_capture_email(email or getattr(getattr(report, "user", None), "email", None))
    report_id = getattr(report, "id", None)
    if not normalized_email:
        return 0
    query = db.query(db_mod.EmailCapture).filter(
        db_mod.EmailCapture.email == normalized_email,
        db_mod.EmailCapture.is_converted.is_(False),
    )
    if report_id:
        query = query.filter(db_mod.EmailCapture.report_id == report_id)
    captures = query.all()
    if not captures:
        return 0
    now = datetime.utcnow()
    for capture in captures:
        capture.is_converted = True
        capture.converted_at = capture.converted_at or now
    return len(captures)


def get_abandoned_unlocks(db, days=1):
    cutoff = datetime.utcnow() - timedelta(days=max(int(days or 1), 0))
    captures = (
        db.query(db_mod.EmailCapture)
        .filter(
            db_mod.EmailCapture.created_at <= cutoff,
            db_mod.EmailCapture.is_converted.is_(False),
        )
        .order_by(db_mod.EmailCapture.created_at.asc())
        .all()
    )
    return [
        {
            "email": capture.email,
            "report_id": capture.report_id,
            "created_at": capture.created_at,
            "source": capture.source,
        }
        for capture in captures
    ]


def _normalize_feedback_stage(stage):
    normalized = str(stage or "").strip().lower()
    if normalized not in FEEDBACK_STAGES:
        raise ValueError("invalid_feedback_stage")
    return normalized


def _normalize_feedback_rating(stage, rating):
    normalized = str(rating or "").strip().lower()
    allowed = PREVIEW_FEEDBACK_RATINGS if stage == "preview" else FULL_FEEDBACK_RATINGS
    if normalized not in allowed:
        raise ValueError("invalid_feedback_rating")
    return normalized


def save_feedback_entry(db, payload, *, user=None):
    report_id = payload.get("report_id")
    try:
        report_id = int(report_id)
    except (TypeError, ValueError):
        raise ValueError("report_id_required") from None

    report = db.query(db_mod.GeneratedReport).filter(db_mod.GeneratedReport.id == report_id).first()
    if not report:
        raise LookupError("report_not_found")

    stage = _normalize_feedback_stage(payload.get("stage"))
    rating = _normalize_feedback_rating(stage, payload.get("rating"))
    comment = str(payload.get("comment") or "").strip() or None
    recommend_flag = _normalize_optional_bool(payload.get("recommend_flag"))

    entry = db_mod.FeedbackEntry(
        user_id=getattr(user, "id", None),
        report_id=report.id,
        report_type=str(getattr(report, "report_type", None) or payload.get("report_type") or "premium").strip(),
        stage=stage,
        rating=rating,
        comment=comment,
        recommend_flag=recommend_flag if stage == "full" else None,
    )
    db.add(entry)
    db.flush()
    return entry


def build_recovery_email_content(report_context):
    context = dict(report_context or {})
    summary = str(context.get("summary") or context.get("primary_focus") or "your current reading").strip()
    focus_title = str(context.get("recommendation_title") or context.get("anchor_title") or summary).strip()
    full_name = str(context.get("full_name") or "there").strip()
    subject = "Your reading is still waiting"
    preview_text = "Return to unlock the rest of your chart-based guidance."
    body = (
        f"{full_name}, your reading is still waiting.\n\n"
        f"We prepared this guidance around {focus_title} based on your birth chart and current timing.\n"
        "Your current planetary period makes these insights especially relevant now.\n\n"
        "Come back to unlock your full reading and downloadable report."
    )
    return {"subject": subject, "preview_text": preview_text, "body": body}


def mark_report_as_unlocked(report, payment_reference=None):
    now = datetime.utcnow()
    report.access_state = "unlocked"
    report.unlocked_at = getattr(report, "unlocked_at", None) or now
    if payment_reference:
        report.payment_reference = payment_reference
    report.pdf_ready = True
    return report


def mark_report_as_paid(report, payment_reference=None):
    mark_report_as_unlocked(report, payment_reference=payment_reference)
    report.access_state = "purchased"
    report.is_paid = True
    return report


def mark_report_as_delivered(report):
    if not can_view_full_report(report):
        mark_report_as_unlocked(report)
    report.access_state = "delivered"
    report.pdf_ready = True
    report.delivered_at = datetime.utcnow()
    return report


def _report_access_label(access_state):
    labels = {
        "preview": "Preview",
        "unlocked": "Unlocked",
        "purchased": "Purchased",
        "delivered": "Delivered",
    }
    return labels.get(normalize_report_access_state(access_state), "Preview")


def _truncate_ai_sections_for_preview(ai_sections):
    preview_sections = []
    for section in (ai_sections or [])[: PREVIEW_CONTENT_LIMITS["ai_sections"]]:
        blocks = list(section.get("blocks") or [])[: PREVIEW_CONTENT_LIMITS["ai_blocks_per_section"]]
        preview_sections.append({**section, "blocks": blocks})
    return preview_sections


def _build_report_access_context(report, *, current_user=None, unlock_success=False):
    access_state = get_report_access_state(report)
    is_preview = access_state == "preview"
    can_unlock_here = bool(report and current_user and getattr(report, "user_id", None) == _user_id(current_user))
    beta_eligible = can_use_beta_free_unlock(current_user)
    payments_live = payments.payments_enabled()
    return {
        "access_state": access_state,
        "access_label": _report_access_label(access_state),
        "is_preview": is_preview,
        "is_unlocked": can_view_full_report(report),
        "is_paid": bool(getattr(report, "is_paid", False)),
        "is_delivered": access_state == "delivered",
        "unlocked_at": getattr(report, "unlocked_at", None),
        "payment_reference": getattr(report, "payment_reference", None),
        "pdf_ready": bool(getattr(report, "pdf_ready", False)),
        "can_view_full_report": can_view_full_report(report),
        "can_download_pdf": can_download_pdf(report),
        "show_unlock_cta": is_preview and bool(report) and (payments_live or beta_eligible),
        "can_unlock_here": can_unlock_here,
        "unlock_success": bool(unlock_success),
        "show_login_hint": is_preview and not bool(report),
        "payments_enabled": payments_live,
        "beta_free_unlock_enabled": payments.beta_free_unlock_enabled(),
        "beta_unlock_eligible": beta_eligible,
        "purchase_cta_label": "Unlock beta access" if beta_eligible else "Unlock full reading",
        "checkout_mode": "beta" if beta_eligible else "payment",
    }


def _apply_report_access_context(context, report, *, current_user=None, unlock_success=False):
    enriched = dict(context or {})
    access = _build_report_access_context(report, current_user=current_user, unlock_success=unlock_success)
    recommendation_layer = enriched.get("recommendation_layer") or {}
    top_anchors = list(enriched.get("top_anchors") or [])
    top_recommendations = list(recommendation_layer.get("top_recommendations") or [])
    visible_top_anchors = top_anchors if access["can_view_full_report"] else top_anchors[: PREVIEW_CONTENT_LIMITS["anchors"]]
    visible_top_recommendations = (
        top_recommendations if access["can_view_full_report"] else top_recommendations[: PREVIEW_CONTENT_LIMITS["recommendations"]]
    )
    visible_ai_sections = enriched.get("ai_sections") or []
    if not access["can_view_full_report"]:
        visible_ai_sections = _truncate_ai_sections_for_preview(visible_ai_sections)
    enriched["generated_report_id"] = getattr(report, "id", None) or enriched.get("generated_report_id")
    enriched["report_access"] = access
    enriched["visible_top_anchors"] = visible_top_anchors
    enriched["visible_top_recommendations"] = visible_top_recommendations
    enriched["visible_opportunity_windows"] = recommendation_layer.get("opportunity_windows") or []
    enriched["visible_risk_windows"] = recommendation_layer.get("risk_windows") or []
    if not access["can_view_full_report"]:
        enriched["visible_opportunity_windows"] = []
        enriched["visible_risk_windows"] = []
    enriched["has_locked_anchors"] = len(top_anchors) > len(visible_top_anchors)
    enriched["has_locked_recommendations"] = len(top_recommendations) > len(visible_top_recommendations)
    enriched["show_locked_windows"] = not access["can_view_full_report"]
    enriched["show_pdf_download"] = bool(enriched.get("show_pdf_download")) and access["can_download_pdf"]
    enriched["show_ai_interpretation"] = access["can_view_full_report"]
    enriched["ai_sections"] = visible_ai_sections
    enriched["preview_teaser_count"] = max(len(top_anchors) - len(visible_top_anchors), 0) + max(len(top_recommendations) - len(visible_top_recommendations), 0)
    return enriched


def _owned_report_or_404(db, user, report_id):
    report = db.query(db_mod.GeneratedReport).filter(
        db_mod.GeneratedReport.id == report_id,
        db_mod.GeneratedReport.user_id == _user_id(user),
    ).first()
    if not report:
        _public_error("Rapor bulunamadi.", 404)
    return report


def _owned_report_from_payload(db, user, payload_data):
    report_id = payload_data.get("generated_report_id")
    if not report_id or not user:
        return None
    return db.query(db_mod.GeneratedReport).filter(
        db_mod.GeneratedReport.id == report_id,
        db_mod.GeneratedReport.user_id == _user_id(user),
    ).first()


def _report_checkout_urls(report, request=None):
    success_url = _checkout_redirect_url("/checkout/success", report_id=report.id, session_id="{CHECKOUT_SESSION_ID}")
    cancel_url = _checkout_redirect_url("/checkout/cancel", report_id=report.id)
    if request is not None:
        base = _app_base_url(request)
        success_url = f"{base}/checkout/success?report_id={report.id}&session_id={{CHECKOUT_SESSION_ID}}"
        cancel_url = f"{base}/checkout/cancel?report_id={report.id}"
    return success_url, cancel_url


def _service_order_by_token_or_404(db, order_token):
    order = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.order_token == str(order_token or "")).first()
    if not order:
        _public_error("Sipariş bulunamadı.", 404)
    return order


def _service_order_payload(order):
    payload = _safe_json_loads(getattr(order, "payload_json", None), {})
    return payload if isinstance(payload, dict) else {}


def _service_order_product(order):
    if getattr(order, "service_type", "") == "consultation":
        return dict(CONSULTATION_PRODUCT)
    bundle_type = normalize_report_bundle_type(getattr(order, "bundle_type", "") or getattr(order, "product_type", ""))
    if bundle_type:
        product = dict(REPORT_BUNDLE_PRODUCTS[bundle_type])
        product["is_bundle"] = True
        return product
    product_type = normalize_report_order_type(getattr(order, "product_type", ""))
    if product_type:
        return dict(REPORT_ORDER_PRODUCTS[product_type])
    return {"title": getattr(order, "product_type", "Sipariş"), "price": getattr(order, "amount_label", ""), "summary": ""}


def _checkout_upsell_context(order=None, service_kind=None):
    service_type = service_kind or getattr(order, "service_type", "")
    product_type = getattr(order, "product_type", "")
    bundle_type = normalize_report_bundle_type(getattr(order, "bundle_type", "") or product_type)
    if service_type == "consultation":
        return {
            "eyebrow": "Seansa hazırlık",
            "title": "Doğum Haritası Karma’sı raporunu seans öncesi ekleyebilirsiniz.",
            "text": "Temel harita raporu, birebir görüşmede sorularınızı daha hızlı ve derin bir zeminde ele almaya yardımcı olur.",
            "primary_label": "Bu Raporu Al",
            "primary_href": "/reports/order/birth_chart_karma",
        }
    if bundle_type:
        return {
            "eyebrow": "Sipariş kapsamı",
            "title": "Bu paket birden fazla içgörüyü tek siparişte toplar.",
            "text": "Ödeme sonrası paket içeriği birlikte hazırlanır; AI destekli taslak yine yalnızca yönetici incelemesine gider ve teslim insan değerlendirmesi sonrası yapılır.",
            "primary_label": "Paketleri Karşılaştır",
            "primary_href": "/reports#paketler",
        }
    if product_type == "birth_chart_karma":
        return {
            "eyebrow": "Zamanlama katmanı",
            "title": "Yıllık Transit ile bu temel haritayı zamana yerleştirin.",
            "text": "Doğum Haritası Karma’sı kişisel yapıyı gösterir; Yıllık Transit ise önünüzdeki dönemlerde bu yapının nasıl çalışabileceğini netleştirir.",
            "primary_label": "Bu Analizle Devam Et",
            "primary_href": "/reports/order/annual_transit",
            "secondary_label": "Life Path Bundle",
            "secondary_href": "/reports/order/bundle/life_path_bundle",
        }
    if product_type == "career":
        return {
            "eyebrow": "Daha derin kariyer yönü",
            "title": "Kariyer kararlarını birebir danışmanlıkla derinleştirin.",
            "text": "Kariyer raporu yönü açar; danışmanlık ise bu yönü gerçek kararlarınız ve zamanlama sorularınızla birlikte ele alır.",
            "primary_label": "Danışmanlıkla Derinleştir",
            "primary_href": "/personal-consultation",
            "secondary_label": "Full Year Insight Bundle",
            "secondary_href": "/reports/order/bundle/full_year_insight_bundle",
        }
    if product_type == "parent_child":
        return {
            "eyebrow": "Aile içgörüsü",
            "title": "İkinci çocuk veya aile dinamiği için kapsamı genişletebilirsiniz.",
            "text": "Ebeveyn-Çocuk raporu tek ilişkiyi hassas biçimde okur; daha geniş aile bağlamı için ek rapor veya danışmanlık daha doğru olabilir.",
            "primary_label": "Danışmanlıkla Derinleştir",
            "primary_href": "/personal-consultation",
        }
    return {
        "eyebrow": "Zamanlama katmanı",
        "title": "Yıllık Transit ile bu analizi dönemsel bağlama taşıyın.",
        "text": "Odak raporunuz netlik sağlar; yıllık zamanlama katmanı, hangi dönemde nasıl ilerleyeceğinizi daha görünür kılar.",
        "primary_label": "Bu Analizle Devam Et",
        "primary_href": "/reports/order/annual_transit",
    }


def _service_checkout_urls(order, request=None):
    if order.service_type == "consultation":
        success_path = "/checkout/consultation/success"
        cancel_path = "/checkout/consultation/cancel"
    else:
        success_path = f"/checkout/report/{order.order_token}/success"
        cancel_path = f"/checkout/report/{order.order_token}/cancel"
    base = _app_base_url(request)
    success_url = f"{base}{success_path}?order_token={order.order_token}&session_id={{CHECKOUT_SESSION_ID}}"
    cancel_url = f"{base}{cancel_path}?order_token={order.order_token}"
    return success_url, cancel_url


def _iyzico_callback_url(order, request=None):
    callback_path = (
        "/payments/iyzico/callback/consultation"
        if order.service_type == "consultation"
        else "/payments/iyzico/callback/report"
    )
    return f"{_app_base_url(request)}{callback_path}"


def initialize_payment_for_order(order, request=None):
    provider = payments.get_payment_provider()
    if getattr(provider, "provider_name", "") != "iyzico":
        raise payments.PaymentConfigurationError("Service orders currently require iyzico checkout form.")
    if not hasattr(provider, "initialize_payment_for_order"):
        raise payments.PaymentConfigurationError("Iyzico checkout form initialization is not configured.")
    callback_url = _iyzico_callback_url(order, request=request)
    session = provider.initialize_payment_for_order(order, callback_url)
    order.provider_name = "iyzico"
    order.payment_provider = "iyzico"
    order.provider_token = session.get("provider_token") or session.get("session_id")
    order.payment_session_id = order.provider_token
    order.provider_conversation_id = session.get("provider_conversation_id") or _order_public_token(order)
    return session


def _create_service_checkout_session(order, request=None):
    if not payments.payments_enabled():
        raise payments.PaymentConfigurationError("Online ödeme şu anda aktif değil.")
    return initialize_payment_for_order(order, request=request)


def create_report_payment_session(order, request=None):
    return _create_service_checkout_session(order, request=request)


def create_consultation_payment_session(order, request=None):
    return _create_service_checkout_session(order, request=request)


def _calendly_signature_header(headers):
    return (
        headers.get("calendly-webhook-signature")
        or headers.get("Calendly-Webhook-Signature")
        or headers.get("x-calendly-webhook-signature")
        or headers.get("X-Calendly-Webhook-Signature")
        or ""
    )


def verify_calendly_webhook_signature(raw_body, headers):
    secret = str(os.getenv("CALENDLY_WEBHOOK_SIGNING_KEY", "") or "").strip()
    if not secret:
        return True
    signature_header = str(_calendly_signature_header(headers) or "").strip()
    if not signature_header:
        return False
    parts = {}
    for chunk in signature_header.split(","):
        if "=" in chunk:
            key, value = chunk.split("=", 1)
            parts[key.strip()] = value.strip()
    timestamp = parts.get("t")
    expected_signature = parts.get("v1") or signature_header
    signed_payload = raw_body if not timestamp else f"{timestamp}.{raw_body.decode('utf-8')}".encode("utf-8")
    digest = hmac.new(secret.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
    return hmac.compare_digest(digest, expected_signature)


def _parse_calendly_datetime(value):
    if not value:
        return None
    raw = str(value).strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(pytz.UTC).replace(tzinfo=None)
    return parsed


def _calendly_scheduled_event(payload):
    scheduled = payload.get("scheduled_event")
    if isinstance(scheduled, Mapping):
        return scheduled
    return {}


def _calendly_event_uri(payload):
    scheduled = _calendly_scheduled_event(payload)
    event_value = payload.get("event")
    if isinstance(event_value, Mapping):
        return event_value.get("uri") or scheduled.get("uri")
    return scheduled.get("uri") or event_value or payload.get("event_uri")


def _calendly_invitee_uri(payload):
    invitee = payload.get("invitee")
    if isinstance(invitee, Mapping):
        return invitee.get("uri")
    return payload.get("uri") or payload.get("invitee_uri")


def _calendly_invitee_email(payload):
    invitee = payload.get("invitee")
    if isinstance(invitee, Mapping) and invitee.get("email"):
        return str(invitee.get("email")).strip().lower()
    return str(payload.get("email") or "").strip().lower()


def _calendly_start_end(payload):
    scheduled = _calendly_scheduled_event(payload)
    return (
        _parse_calendly_datetime(scheduled.get("start_time") or payload.get("start_time")),
        _parse_calendly_datetime(scheduled.get("end_time") or payload.get("end_time")),
    )


def _calendly_event_type_uri(payload):
    scheduled = _calendly_scheduled_event(payload)
    event_type = scheduled.get("event_type") or payload.get("event_type")
    if isinstance(event_type, Mapping):
        return event_type.get("uri")
    return event_type


def _consultation_payload_json_from_calendly(payload, order=None):
    scheduled_start = getattr(order, "scheduled_start", None) if order else None
    scheduled_end = getattr(order, "scheduled_end", None) if order else None
    return json.dumps(
        {
            "service_type": "consultation",
            "product_type": CONSULTATION_PRODUCT["product_type"],
            "submitted_at": datetime.now(pytz.UTC).isoformat(),
            "booking_source": "calendly",
            "calendly": {
                "event_uri": getattr(order, "calendly_event_uri", None) if order else _calendly_event_uri(payload),
                "invitee_uri": getattr(order, "calendly_invitee_uri", None) if order else _calendly_invitee_uri(payload),
                "event_type_uri": getattr(order, "calendly_event_type_uri", None) if order else _calendly_event_type_uri(payload),
                "scheduled_start": scheduled_start.isoformat() if scheduled_start else None,
                "scheduled_end": scheduled_end.isoformat() if scheduled_end else None,
            },
            "service_model": {
                "duration": "60 dakika",
                "sequence": "Calendly randevu seçimi sonrası iyzico ödeme adımı",
                "cancellation": "Randevular, planlanan saatten en az 24 saat önce ücretsiz olarak iptal edilebilir veya yeniden planlanabilir.",
            },
        },
        ensure_ascii=False,
    )


def _find_calendly_consultation_order(db, event_uri=None, invitee_uri=None, email=None, scheduled_start=None):
    query = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.service_type == "consultation")
    if invitee_uri:
        order = query.filter(db_mod.ServiceOrder.calendly_invitee_uri == invitee_uri).first()
        if order:
            return order
    if event_uri:
        order = query.filter(db_mod.ServiceOrder.calendly_event_uri == event_uri).first()
        if order:
            return order
    if email and scheduled_start:
        return query.filter(
            db_mod.ServiceOrder.customer_email == email,
            db_mod.ServiceOrder.scheduled_start == scheduled_start,
            db_mod.ServiceOrder.status.in_({"booking_pending_payment", "paid", "confirmed", "prepared"}),
        ).first()
    return None


def sync_calendly_invitee_created(db, payload):
    event_uri = _calendly_event_uri(payload)
    invitee_uri = _calendly_invitee_uri(payload)
    email = _calendly_invitee_email(payload)
    name = str(payload.get("name") or payload.get("first_name") or "").strip()
    scheduled_start, scheduled_end = _calendly_start_end(payload)
    event_type_uri = _calendly_event_type_uri(payload)
    order = _find_calendly_consultation_order(db, event_uri, invitee_uri, email, scheduled_start)
    action = "consultation_booking_updated" if order else "consultation_booking_created"
    if not order:
        order = db_mod.ServiceOrder(
            order_token=_generate_order_token("consult"),
            service_type="consultation",
            product_type=CONSULTATION_PRODUCT["product_type"],
            status="booking_pending_payment",
            amount=_amount_decimal(CONSULTATION_PRODUCT["price"]),
            amount_label=CONSULTATION_PRODUCT["price"],
            currency="TRY",
        )
        order.public_token = order.order_token
        db.add(order)
        db.flush()
    elif order.status in {"initiated", "booking_expired", "awaiting_payment"} and not order.paid_at:
        order.status = "booking_pending_payment"
    order.customer_name = name or order.customer_name
    order.customer_email = email or order.customer_email
    order.calendly_event_uri = event_uri or order.calendly_event_uri
    order.calendly_invitee_uri = invitee_uri or order.calendly_invitee_uri
    order.calendly_event_type_uri = event_type_uri or order.calendly_event_type_uri
    order.calendly_status = "created"
    order.booking_source = "calendly"
    order.scheduled_start = scheduled_start or order.scheduled_start
    order.scheduled_end = scheduled_end or order.scheduled_end
    order.payload_json = _consultation_payload_json_from_calendly(payload, order=order)
    log_admin_action(
        db,
        order,
        action,
        actor="calendly",
        metadata={
            "calendly_event_uri": order.calendly_event_uri,
            "calendly_invitee_uri": order.calendly_invitee_uri,
            "invitee_email": order.customer_email,
            "scheduled_start": order.scheduled_start.isoformat() if order.scheduled_start else None,
            "checkout_url": f"/checkout/consultation/{order.order_token}",
        },
    )
    db.commit()
    db.refresh(order)
    return {"order": order, "action": action, "checkout_url": f"/checkout/consultation/{order.order_token}"}


def sync_calendly_invitee_canceled(db, payload):
    event_uri = _calendly_event_uri(payload)
    invitee_uri = _calendly_invitee_uri(payload)
    email = _calendly_invitee_email(payload)
    scheduled_start, _scheduled_end = _calendly_start_end(payload)
    order = _find_calendly_consultation_order(db, event_uri, invitee_uri, email, scheduled_start)
    if not order:
        return {"order": None, "action": "ignored"}
    canceled_at = _parse_calendly_datetime(payload.get("canceled_at")) or datetime.utcnow()
    order.calendly_status = "canceled"
    order.calendly_canceled_at = canceled_at
    order.cancelled_at = order.cancelled_at or canceled_at
    reason = payload.get("cancellation", {}).get("reason") if isinstance(payload.get("cancellation"), Mapping) else payload.get("cancel_reason")
    if reason:
        order.cancellation_reason = str(reason)
    if order.status == "booking_pending_payment" and not order.paid_at:
        order.status = "booking_expired"
    elif order.paid_at:
        note = f"Calendly cancellation received at {canceled_at.isoformat()}; admin review required before any refund."
        order.internal_notes = ((order.internal_notes or "").rstrip() + "\n" + note).strip()
    log_admin_action(
        db,
        order,
        "consultation_booking_canceled",
        actor="calendly",
        metadata={
            "calendly_event_uri": event_uri,
            "calendly_invitee_uri": invitee_uri,
            "invitee_email": email,
            "scheduled_start": scheduled_start.isoformat() if scheduled_start else None,
            "paid": bool(order.paid_at),
        },
    )
    db.commit()
    db.refresh(order)
    return {"order": order, "action": "consultation_booking_canceled"}


def process_calendly_webhook_event(db, event_type, payload):
    if event_type == "invitee.created":
        return sync_calendly_invitee_created(db, payload)
    if event_type == "invitee.canceled":
        return sync_calendly_invitee_canceled(db, payload)
    return {"order": None, "action": "ignored"}


def _finalize_report_purchase(report, payment_data):
    provider = payments.get_payment_provider()
    changed = provider.finalize_purchase(report, payment_data)
    report.access_state = "purchased"
    report.is_paid = True
    report.pdf_ready = True
    return changed


def _recovery_report_context(report):
    interpretation_context = _safe_json_loads(getattr(report, "interpretation_context_json", None), {})
    recommendation_layer = interpretation_context.get("recommendation_layer") or interpretation_context.get("signal_layer", {}).get("recommendation_layer") or {}
    top_recommendations = recommendation_layer.get("top_recommendations") or []
    top_anchors = (interpretation_context.get("signal_layer") or {}).get("top_anchors") or []
    return {
        "full_name": getattr(report, "full_name", None),
        "primary_focus": interpretation_context.get("primary_focus"),
        "summary": _report_summary_text(report),
        "recommendation_title": (top_recommendations[0] or {}).get("title") if top_recommendations else None,
        "anchor_title": (top_anchors[0] or {}).get("title") if top_anchors else None,
    }


def _require_authenticated_user(request, db):
    user = get_request_user(request, db)
    if user:
        return user
    return None


def _safe_model_id(instance):
    if instance is None:
        return None
    try:
        identity = sa_inspect(instance).identity
        if identity:
            return identity[0]
    except Exception:
        pass
    try:
        return getattr(instance, "id", None)
    except Exception:
        return None


def _require_admin_user(request, db):
    user = _require_authenticated_user(request, db)
    if not user:
        request_path = _safe_next_path(getattr(request.url, "path", "/admin"), default="/admin")
        if request_path in {"/admin", "/admin/dashboard"}:
            return None, RedirectResponse(url=f"/login?{urlencode({'next': request_path})}", status_code=303)
        return None, RedirectResponse(url="/login", status_code=303)
    if not is_admin_user(user):
        logger.warning(
            "Admin access denied ip=%s user_id=%s email=%s path=%s",
            _client_ip(request),
            _safe_model_id(user),
            getattr(user, "email", None),
            request.url.path,
        )
        return user, HTMLResponse("Admin access denied.", status_code=403)
    return user, None


def admin_required(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        request = kwargs.get("request")
        db = kwargs.get("db")
        if request is None or db is None:
            raise RuntimeError("admin_required expects request and db keyword arguments.")
        admin_user, denied_response = _require_admin_user(request, db)
        if denied_response:
            return denied_response
        request.state.admin_user = _public_user_view(admin_user)
        logger.info("Admin page accessed admin_id=%s path=%s", _safe_model_id(admin_user), request.url.path)
        return await func(*args, **kwargs)

    return wrapper


async def require_admin(request: Request, db: Session = Depends(get_db)):
    current_user = get_request_user(request, db)
    if not current_user or not current_user.is_admin:
        raise HTTPException(status_code=403, detail="Forbidden")
    request.state.admin_user = _public_user_view(current_user)
    return current_user


def admin_api_required(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        request = kwargs.get("request")
        db = kwargs.get("db")
        if request is None or db is None:
            raise RuntimeError("admin_api_required expects request and db keyword arguments.")
        user = get_request_user(request, db)
        if not user:
            logger.warning("Admin API access denied ip=%s user_id=%s email=%s path=%s", _client_ip(request), None, None, request.url.path)
            return json_admin_error("authentication_required", 401, endpoint=request)
        if not is_admin_user(user):
            logger.warning("Admin API access denied ip=%s user_id=%s email=%s path=%s", _client_ip(request), _safe_model_id(user), getattr(user, "email", None), request.url.path)
            return json_admin_error("admin_access_denied", 403, endpoint=request)
        request.state.admin_user = _public_user_view(user)
        logger.info("Admin API accessed admin_id=%s path=%s", _safe_model_id(user), request.url.path)
        return await func(*args, **kwargs)
    return wrapper


def _upsert_user_profile(db, user, profile_payload):
    profile = None
    normalized_birth_place = profile_payload.get("normalized_birth_place")
    if normalized_birth_place:
        profile = db.query(db_mod.UserProfile).filter(
            db_mod.UserProfile.user_id == user.id,
            db_mod.UserProfile.birth_date == profile_payload["birth_date"],
            db_mod.UserProfile.birth_time == profile_payload["birth_time"],
            db_mod.UserProfile.normalized_birth_place == normalized_birth_place,
        ).first()
    if not profile:
        profile = db.query(db_mod.UserProfile).filter(
            db_mod.UserProfile.user_id == user.id,
            db_mod.UserProfile.birth_date == profile_payload["birth_date"],
            db_mod.UserProfile.birth_time == profile_payload["birth_time"],
            db_mod.UserProfile.birth_city == profile_payload["birth_city"],
        ).first()
    if not profile:
        profile = db_mod.UserProfile(user_id=user.id)
        db.add(profile)

    profile.profile_name = profile_payload.get("full_name") or profile_payload.get("normalized_birth_place") or profile_payload.get("birth_city")
    profile.full_name = profile_payload.get("full_name")
    profile.birth_date = profile_payload.get("birth_date")
    profile.birth_time = profile_payload.get("birth_time")
    profile.birth_city = profile_payload.get("birth_city")
    profile.birth_country = profile_payload.get("birth_country")
    profile.raw_birth_place_input = profile_payload.get("raw_birth_place_input")
    profile.normalized_birth_place = profile_payload.get("normalized_birth_place")
    profile.lat = profile_payload.get("lat")
    profile.lon = profile_payload.get("lon")
    profile.timezone = profile_payload.get("timezone")
    profile.geocode_provider = profile_payload.get("geocode_provider")
    profile.geocode_confidence = profile_payload.get("geocode_confidence")
    profile.natal_data_json = json.dumps(_serialize_temporal_values(profile_payload.get("natal_data") or {}))
    return profile


def _build_calculation_config_payload(calculation_context):
    return {
        "engine_version": ASTRO_ENGINE_VERSION,
        "ayanamsa": calculation_context.ayanamsa,
        "node_mode": calculation_context.node_mode,
        "house_system": calculation_context.house_system,
        "zodiac": ASTRO_CONFIG["zodiac"],
        **get_ayanamsa_trace(calculation_context),
    }


def _save_generated_report(db, user, profile, report_type, payload, interpretation_context, calculation_metadata):
    report = db_mod.GeneratedReport(
        user_id=user.id,
        profile=profile,
        report_type=report_type,
        title=str((interpretation_context or {}).get("primary_focus") or "Vedic Report"),
        full_name=payload.get("full_name"),
        birth_date=payload.get("birth_date"),
        birth_time=payload.get("birth_time"),
        birth_city=payload.get("birth_city"),
        birth_country=payload.get("birth_country"),
        raw_birth_place_input=payload.get("raw_birth_place_input"),
        normalized_birth_place=payload.get("normalized_birth_place"),
        lat=payload.get("latitude"),
        lon=payload.get("longitude"),
        timezone=payload.get("timezone"),
        geocode_provider=payload.get("geocode_provider"),
        geocode_confidence=payload.get("geocode_confidence"),
        calculation_metadata_json=json.dumps(_serialize_temporal_values(calculation_metadata or {})),
        interpretation_context_json=json.dumps(_serialize_temporal_values(interpretation_context or {})),
        result_payload_json=json.dumps(_serialize_temporal_values(payload)),
        access_state="preview",
        is_paid=False,
        pdf_ready=False,
    )
    db.add(report)
    if hasattr(db, "flush"):
        db.flush()
    if getattr(report, "id", None):
        _create_recommendation_followups(db, user=user, report=report, interpretation_context=interpretation_context)
    return report


def _report_view(report):
    access_state = get_report_access_state(report)
    return {
        "id": report.id,
        "report_type": report.report_type,
        "title": report.title,
        "full_name": report.full_name,
        "birth_date": report.birth_date,
        "birth_time": report.birth_time,
        "birth_city": report.birth_city,
        "birth_country": report.birth_country,
        "normalized_birth_place": report.normalized_birth_place,
        "timezone": report.timezone,
        "created_at": report.created_at.strftime("%Y-%m-%d %H:%M") if report.created_at else None,
        "result_payload_json": report.result_payload_json,
        "access_state": access_state,
        "access_label": _report_access_label(access_state),
        "can_view_full_report": can_view_full_report(report),
        "can_download_pdf": can_download_pdf(report),
    }


def _safe_json_loads(value, default):
    try:
        return json.loads(value) if value else default
    except Exception:
        return default


def _report_summary_text(report):
    interpretation_context = _safe_json_loads(getattr(report, "interpretation_context_json", None), {})
    recommendation_layer = interpretation_context.get("recommendation_layer") or interpretation_context.get("signal_layer", {}).get("recommendation_layer") or {}
    top_recommendations = recommendation_layer.get("top_recommendations") or []
    if top_recommendations:
        return str(top_recommendations[0].get("title") or "").strip() or "Current guidance available"
    top_anchors = (interpretation_context.get("signal_layer") or {}).get("top_anchors") or []
    if top_anchors:
        return str(top_anchors[0].get("title") or "").strip() or "Interpretation theme available"
    return str(getattr(report, "title", "") or "Your saved reading").strip()


def _days_since(dt_value):
    if not dt_value:
        return None
    return max((datetime.utcnow() - dt_value).days, 0)


def _report_history_item(db, report):
    recommendation_layer = _recommendation_layer_from_report(report)
    has_recommendations = bool(recommendation_layer.get("top_recommendations"))
    feedback_count = db.query(db_mod.RecommendationFeedback).filter(
        db_mod.RecommendationFeedback.report_id == report.id
    ).count() + db.query(db_mod.InterpretationFeedback).filter(
        db_mod.InterpretationFeedback.report_id == report.id
    ).count()
    pending_followups = db.query(db_mod.RecommendationFollowup).filter(
        db_mod.RecommendationFollowup.report_id == report.id,
        db_mod.RecommendationFollowup.status == "pending",
    ).count()
    days_old = _days_since(report.created_at)
    if feedback_count:
        engagement_tag = "Feedback given"
    elif pending_followups:
        engagement_tag = "Revisit available"
    elif days_old is not None and days_old <= 7:
        engagement_tag = "New"
    else:
        engagement_tag = "Saved"
    access_state = get_report_access_state(report)
    return {
        **_report_view(report),
        "summary": _report_summary_text(report),
        "has_recommendations": has_recommendations,
        "has_feedback": bool(feedback_count),
        "pending_followups": pending_followups,
        "status_tag": _report_access_label(access_state),
        "engagement_tag": engagement_tag,
        "days_old": days_old,
    }


def _build_revisit_context(db, user, report):
    recommendation_feedback = _load_recommendation_feedback_history(db, report_id=report.id, user_id=user.id, limit=50)
    interpretation_feedback = load_feedback_history(db, user_id=user.id, report_id=report.id)
    pending_followups = db.query(db_mod.RecommendationFollowup).filter(
        db_mod.RecommendationFollowup.report_id == report.id,
        db_mod.RecommendationFollowup.user_id == user.id,
        db_mod.RecommendationFollowup.status == "pending",
    ).order_by(db_mod.RecommendationFollowup.scheduled_for.asc()).all()
    return {
        "is_revisit": True,
        "days_since_view": _days_since(report.created_at),
        "has_followup_banner": bool(pending_followups),
        "followup_count": len(pending_followups),
        "followup_titles": [item.recommendation_title for item in pending_followups[:3] if item.recommendation_title],
        "saved_recommendation_indices": sorted({item["recommendation_index"] for item in recommendation_feedback if item.get("recommendation_index")}),
        "saved_anchor_ranks": sorted({item["anchor_rank"] for item in interpretation_feedback if item.get("anchor_rank")}),
        "saved_recommendation_labels": {
            item["recommendation_index"]: item.get("user_feedback_label")
            for item in recommendation_feedback if item.get("recommendation_index")
        },
        "saved_anchor_labels": {
            item["anchor_rank"]: item.get("feedback_label")
            for item in interpretation_feedback if item.get("anchor_rank")
        },
        "recommendation_feedback_count": len(recommendation_feedback),
    }


def _load_recommendation_feedback_history(db, *, user_id=None, report_id=None, limit=100):
    query = db.query(db_mod.RecommendationFeedback)
    if user_id is not None:
        query = query.filter(db_mod.RecommendationFeedback.user_id == user_id)
    if report_id is not None:
        query = query.filter(db_mod.RecommendationFeedback.report_id == report_id)
    rows = query.order_by(db_mod.RecommendationFeedback.created_at.desc()).limit(limit).all()
    return [
        {
            "id": row.id,
            "user_id": row.user_id,
            "report_id": row.report_id,
            "recommendation_index": row.recommendation_index,
            "recommendation_title": row.recommendation_title,
            "recommendation_type": row.recommendation_type,
            "domain": row.domain,
            "user_feedback_label": row.user_feedback_label,
            "user_rating": row.user_rating,
            "acted_on": row.acted_on,
            "saved_for_later": row.saved_for_later,
            "free_text_comment": row.free_text_comment,
            "feedback_source": row.feedback_source or "initial",
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
        for row in rows
    ]


def _normalize_optional_bool(value):
    if value in ("", None):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "on"}:
            return True
        if lowered in {"false", "0", "no", "off"}:
            return False
    return bool(value)


def _recommendation_layer_from_report(report):
    try:
        interpretation_context = json.loads(report.interpretation_context_json or "{}")
    except Exception as exc:
        raise ValueError("invalid_report_context") from exc
    recommendation_layer = interpretation_context.get("recommendation_layer") or interpretation_context.get("signal_layer", {}).get("recommendation_layer") or {}
    return recommendation_layer


def _get_recommendation_from_report(report, recommendation_index):
    recommendation_layer = _recommendation_layer_from_report(report)
    recommendations = recommendation_layer.get("top_recommendations") or []
    if recommendation_index > len(recommendations):
        raise ValueError("invalid_recommendation_index")
    return recommendations[recommendation_index - 1], recommendation_layer


def _create_recommendation_followups(db, *, user, report, interpretation_context):
    if not user or not getattr(user, "id", None) or not getattr(report, "id", None):
        return []
    recommendation_layer = (interpretation_context or {}).get("recommendation_layer") or (
        (interpretation_context or {}).get("signal_layer", {}) or {}
    ).get("recommendation_layer") or {}
    recommendations = recommendation_layer.get("top_recommendations") or []
    created = []
    base_time = report.created_at or datetime.utcnow()
    for index, recommendation in enumerate(recommendations[:5], start=1):
        if not recommendation.get("time_window"):
            continue
        existing = db.query(db_mod.RecommendationFollowup).filter(
            db_mod.RecommendationFollowup.user_id == user.id,
            db_mod.RecommendationFollowup.report_id == report.id,
            db_mod.RecommendationFollowup.recommendation_index == index,
        ).first()
        scheduled_for = derive_followup_time(recommendation, base_time=base_time)
        if existing:
            existing.recommendation_title = recommendation.get("title")
            existing.scheduled_for = scheduled_for
            created.append(existing)
            continue
        followup = db_mod.RecommendationFollowup(
            user_id=user.id,
            report_id=report.id,
            recommendation_index=index,
            recommendation_title=recommendation.get("title"),
            scheduled_for=scheduled_for,
            status="pending",
        )
        db.add(followup)
        created.append(followup)
    return created


def get_pending_followups(db, user_id):
    now = datetime.utcnow()
    rows = db.query(db_mod.RecommendationFollowup).filter(
        db_mod.RecommendationFollowup.user_id == user_id,
        db_mod.RecommendationFollowup.status == "pending",
    ).order_by(db_mod.RecommendationFollowup.scheduled_for.asc()).all()
    return [
        {
            "id": row.id,
            "report_id": row.report_id,
            "recommendation_index": row.recommendation_index,
            "recommendation_title": row.recommendation_title,
            "scheduled_for": row.scheduled_for.isoformat() if row.scheduled_for else None,
            "status": row.status,
            "is_overdue": bool(row.scheduled_for and row.scheduled_for <= now),
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
        for row in rows
    ]


def _save_recommendation_feedback(db, payload, *, report, user, feedback_source="initial"):
    recommendation_index = payload.get("recommendation_index")
    try:
        recommendation_index = int(recommendation_index)
    except (TypeError, ValueError):
        raise ValueError("invalid_recommendation_index") from None
    if recommendation_index < 1 or recommendation_index > 5:
        raise ValueError("invalid_recommendation_index")

    feedback_label = str(payload.get("user_feedback_label") or "").strip().lower()
    allowed_labels = VALID_RECOMMENDATION_FEEDBACK_LABELS if feedback_source == "initial" else VALID_RECOMMENDATION_FOLLOWUP_LABELS
    if feedback_label not in allowed_labels:
        raise ValueError("invalid_feedback_label")

    user_rating = payload.get("user_rating")
    if user_rating in ("", None):
        user_rating = None
    else:
        try:
            user_rating = int(user_rating)
        except (TypeError, ValueError):
            raise ValueError("invalid_user_rating") from None
        if user_rating < 1 or user_rating > 5:
            raise ValueError("invalid_user_rating")

    free_text_comment = str(payload.get("free_text_comment") or "").strip() or None
    if free_text_comment and len(free_text_comment) > 1000:
        free_text_comment = free_text_comment[:1000]

    acted_on = _normalize_optional_bool(payload.get("acted_on"))
    saved_for_later = _normalize_optional_bool(payload.get("saved_for_later"))
    recommendation, _ = _get_recommendation_from_report(report, recommendation_index)

    entry = db.query(db_mod.RecommendationFeedback).filter(
        db_mod.RecommendationFeedback.user_id == user.id,
        db_mod.RecommendationFeedback.report_id == report.id,
        db_mod.RecommendationFeedback.recommendation_index == recommendation_index,
    ).first()
    if not entry:
        entry = db_mod.RecommendationFeedback(
            user_id=user.id,
            report_id=report.id,
            recommendation_index=recommendation_index,
        )
        db.add(entry)

    entry.recommendation_title = recommendation.get("title")
    entry.recommendation_type = recommendation.get("type")
    entry.domain = (recommendation.get("domains") or [None])[0]
    entry.user_feedback_label = feedback_label
    entry.user_rating = user_rating
    entry.acted_on = acted_on
    entry.saved_for_later = saved_for_later
    entry.free_text_comment = free_text_comment
    entry.feedback_source = feedback_source
    db.commit()
    db.refresh(entry)
    return _load_recommendation_feedback_history(db, report_id=report.id, limit=1)[0]


def _complete_recommendation_followup(db, payload, *, followup, report, user):
    feedback_payload = {
        "report_id": report.id,
        "recommendation_index": followup.recommendation_index,
        "user_feedback_label": payload.get("feedback_label"),
        "user_rating": payload.get("user_rating"),
        "acted_on": payload.get("acted_on"),
        "saved_for_later": payload.get("saved_for_later"),
        "free_text_comment": payload.get("comment"),
    }
    saved_feedback = _save_recommendation_feedback(db, feedback_payload, report=report, user=user, feedback_source="followup")
    followup.status = "completed"
    followup.completed_at = datetime.utcnow()
    db.commit()
    db.refresh(followup)
    return {
        "id": followup.id,
        "status": followup.status,
        "completed_at": followup.completed_at.isoformat() if followup.completed_at else None,
        "feedback": saved_feedback,
    }


def _user_admin_view(user, report_count=0):
    return {
        "id": user.id,
        "email": user.email,
        "name": user.name or user.email.split("@")[0],
        "plan_code": user.plan_code,
        "subscription_status": user.subscription_status,
        "created_at": user.created_at.strftime("%Y-%m-%d %H:%M") if user.created_at else None,
        "plan_started_at": user.plan_started_at.strftime("%Y-%m-%d %H:%M") if user.plan_started_at else None,
        "plan_expires_at": user.plan_expires_at.strftime("%Y-%m-%d %H:%M") if user.plan_expires_at else None,
        "stripe_customer_id": getattr(user, "stripe_customer_id", None),
        "stripe_subscription_id": getattr(user, "stripe_subscription_id", None),
        "is_active": user.is_active,
        "is_admin": is_admin_user(user),
        "report_count": report_count,
    }


def _email_log_view(log):
    return {
        "id": log.id,
        "user_id": log.user_id,
        "email_type": log.email_type,
        "recipient_email": log.recipient_email,
        "subject": log.subject,
        "status": log.status,
        "related_event_type": log.related_event_type,
        "related_event_key": log.related_event_key,
        "error_message": log.error_message,
        "created_at": log.created_at.strftime("%Y-%m-%d %H:%M") if log.created_at else None,
    }


def _email_log_admin_view(db, log):
    view = _email_log_view(log)
    related_order = None
    event_key = str(log.related_event_key or "")
    candidate = event_key.rsplit(":", 1)[-1].strip() if event_key else ""
    if candidate:
        query = db.query(db_mod.ServiceOrder)
        related_order = query.filter(
            or_(
                db_mod.ServiceOrder.order_token == candidate,
                db_mod.ServiceOrder.public_token == candidate,
            )
        ).first()
        if related_order is None and candidate.isdigit():
            related_order = query.filter(db_mod.ServiceOrder.id == int(candidate)).first()
    view["related_order_id"] = related_order.id if related_order else None
    return view


def _safe_truncate_text(value, max_len=2500):
    text_value = str(value or "")
    if len(text_value) <= max_len:
        return text_value
    return text_value[: max_len - 3] + "..."


def _safe_json_preview(value, max_len=2500):
    try:
        if isinstance(value, str):
            rendered = value
        else:
            rendered = json.dumps(value, ensure_ascii=False, indent=2, default=str)
    except Exception:
        rendered = str(value)
    return _safe_truncate_text(rendered, max_len=max_len)


def _log_birthplace_event(
    db,
    event_name,
    *,
    provider=None,
    outcome=None,
    location_source=None,
    confidence=None,
    suggestion_count=None,
    metadata=None,
):
    if event_name not in ALLOWED_BIRTHPLACE_EVENTS:
        logger.warning("Ignored unknown birthplace event event_name=%s", event_name)
        return None
    event = db_mod.BirthplaceEventLog(
        event_name=event_name,
        provider=provider,
        outcome=outcome,
        location_source=location_source,
        confidence=confidence,
        suggestion_count=suggestion_count,
        metadata_json=json.dumps(_serialize_temporal_values(metadata or {})),
    )
    db.add(event)
    db.commit()
    return event


def _birthplace_time_window_bounds(period):
    normalized = str(period or "all").strip().lower()
    if normalized == "7d":
        return datetime.utcnow() - timedelta(days=7), "7d"
    if normalized == "30d":
        return datetime.utcnow() - timedelta(days=30), "30d"
    if normalized not in {"all", ""}:
        logger.warning("Unknown birthplace analytics period=%s falling back to all", period)
    return None, "all"


def get_birthplace_observability_summary(db, time_window=None):
    since, normalized_window = _birthplace_time_window_bounds(time_window)
    query = db.query(db_mod.BirthplaceEventLog)
    if since is not None:
        query = query.filter(db_mod.BirthplaceEventLog.created_at >= since)
    events = query.order_by(db_mod.BirthplaceEventLog.created_at.desc()).all()

    counts = {event_name: 0 for event_name in ALLOWED_BIRTHPLACE_EVENTS}
    suggestion_total = 0
    suggestion_count_events = 0
    confidence_buckets = {"high": 0, "medium": 0, "low": 0}

    for event in events:
        counts[event.event_name] = counts.get(event.event_name, 0) + 1
        if event.event_name == "suggestion_results_returned":
            if event.suggestion_count is not None:
                suggestion_total += int(event.suggestion_count)
                suggestion_count_events += 1
        if event.confidence is not None:
            if float(event.confidence) >= 0.8:
                confidence_buckets["high"] += 1
            elif float(event.confidence) >= 0.55:
                confidence_buckets["medium"] += 1
            else:
                confidence_buckets["low"] += 1

    with_selected = counts["submit_with_selected_suggestion"]
    without_selected = counts["submit_without_selected_suggestion"]
    success_count = counts["resolved_birthplace_success"]
    failure_count = counts["resolved_birthplace_failure"]
    total_submits = with_selected + without_selected
    total_resolutions = success_count + failure_count

    return {
        "time_window": normalized_window,
        "generated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "metrics": {
            "total_suggestion_queries": counts["suggestion_results_returned"],
            "empty_suggestion_result_count": sum(
                1 for event in events if event.event_name == "suggestion_results_returned" and int(event.suggestion_count or 0) == 0
            ),
            "average_suggestion_count": round(suggestion_total / suggestion_count_events, 2) if suggestion_count_events else None,
            "suggestion_selected_count": counts["suggestion_selected"],
            "submit_with_selected_suggestion": with_selected,
            "submit_without_selected_suggestion": without_selected,
            "suggestion_selection_rate": round(with_selected / total_submits, 4) if total_submits else None,
            "resolved_birthplace_success_count": success_count,
            "resolved_birthplace_failure_count": failure_count,
            "resolution_success_rate": round(success_count / total_resolutions, 4) if total_resolutions else None,
            "ambiguous_or_low_confidence_birthplace_count": counts["ambiguous_or_low_confidence_birthplace"],
            "stale_resolved_payload_discarded_count": counts["stale_resolved_payload_discarded"],
            "fallback_rate": round(without_selected / total_submits, 4) if total_submits else None,
        },
        "confidence_buckets": confidence_buckets,
        "recent_events": [
            {
                "created_at": event.created_at.strftime("%Y-%m-%d %H:%M") if event.created_at else None,
                "event_name": event.event_name,
                "provider": event.provider,
                "outcome": event.outcome,
                "location_source": event.location_source,
            }
            for event in events[:10]
        ],
    }


def _report_detail_payload(raw_value):
    try:
        parsed = json.loads(raw_value or "{}")
    except Exception:
        return {"raw": _safe_json_preview(raw_value)}
    if isinstance(parsed, dict):
        summary = {}
        for key in ("full_name", "birth_date", "birth_time", "birth_city", "birth_country", "raw_birth_place_input", "normalized_birth_place", "timezone", "report_type", "generated_report_id"):
            if key in parsed:
                summary[key] = parsed[key]
        if "interpretation_context" in parsed and isinstance(parsed["interpretation_context"], dict):
            summary["interpretation_context_keys"] = list(parsed["interpretation_context"].keys())[:12]
        return {"summary": summary, "raw": _safe_json_preview(parsed)}
    return {"raw": _safe_json_preview(parsed)}


def _resolve_birth_location_payload(place_text, country_name=""):
    resolved_location = utils.resolve_birth_location(place_text, country_name)
    return {
        "raw_birth_place_input": resolved_location["raw_input"],
        "normalized_birth_place": resolved_location["normalized_place"],
        "latitude": resolved_location["latitude"],
        "longitude": resolved_location["longitude"],
        "timezone": resolved_location["timezone"],
        "geocode_provider": resolved_location["provider"],
        "geocode_confidence": resolved_location.get("confidence"),
        "location_source": resolved_location.get("location_source", resolved_location.get("provider")),
        "geocode_cache_hit": resolved_location.get("cache_hit", False),
    }


def _resolved_birth_location_payload_from_form(
    *,
    birth_city,
    country="",
    resolved_birth_place=None,
    resolved_latitude=None,
    resolved_longitude=None,
    resolved_timezone=None,
    geocode_provider=None,
    geocode_confidence=None,
):
    has_resolved_payload = all(
        value not in (None, "")
        for value in (resolved_birth_place, resolved_latitude, resolved_longitude, resolved_timezone)
    )
    if has_resolved_payload and not _resolved_selection_matches_input(birth_city, resolved_birth_place):
        logger.warning(
            "Discarding stale resolved birthplace payload visible_input=%s resolved_birth_place=%s",
            birth_city,
            resolved_birth_place,
        )
        if country is not None:
            db = None
            try:
                db = db_mod.SessionLocal()
                _log_birthplace_event(
                    db,
                    "stale_resolved_payload_discarded",
                    provider=geocode_provider or "suggestion",
                    outcome="discarded",
                    location_source="stale_hidden_payload",
                    confidence=float(geocode_confidence) if geocode_confidence not in (None, "") else None,
                )
            except Exception:
                logger.exception("Failed to log stale resolved payload event")
            finally:
                if db is not None:
                    db.close()
        return _resolve_birth_location_payload(birth_city, country)

    if resolved_birth_place and resolved_latitude not in (None, "") and resolved_longitude not in (None, "") and resolved_timezone:
        return {
            "raw_birth_place_input": birth_city.strip(),
            "normalized_birth_place": str(resolved_birth_place).strip(),
            "latitude": float(resolved_latitude),
            "longitude": float(resolved_longitude),
            "timezone": str(resolved_timezone).strip(),
            "geocode_provider": str(geocode_provider or "suggestion").strip(),
            "geocode_confidence": float(geocode_confidence) if geocode_confidence not in (None, "") else None,
            "location_source": "suggestion_selection",
            "geocode_cache_hit": None,
        }
    return _resolve_birth_location_payload(birth_city, country)


def _normalize_place_text(value):
    normalized = normalize("NFKD", str(value or "").strip().lower()).encode("ascii", "ignore").decode("ascii")
    normalized = normalized.replace("ı", "i")
    return " ".join(normalized.split())


def _place_tokens(value):
    normalized = _normalize_place_text(value)
    chunks = re.split(r"[,/\\-]|\s+", normalized)
    return {chunk for chunk in chunks if chunk and len(chunk) > 1}


def _resolved_selection_matches_input(birth_city, resolved_birth_place):
    visible_tokens = _place_tokens(birth_city)
    resolved_tokens = _place_tokens(resolved_birth_place)
    if not visible_tokens or not resolved_tokens:
        return False
    return visible_tokens.issubset(resolved_tokens)


def _build_birth_context(birth_date_str, place_text, country_name="", resolved_location_payload=None):
    location_payload = resolved_location_payload or _resolve_birth_location_payload(place_text, country_name)
    birth_context = utils.build_birth_context(
        birth_date_str,
        {
            "raw_input": location_payload["raw_birth_place_input"],
            "normalized_place": location_payload["normalized_birth_place"],
            "latitude": location_payload["latitude"],
            "longitude": location_payload["longitude"],
            "timezone": location_payload["timezone"],
            "provider": location_payload["geocode_provider"],
            "confidence": location_payload["geocode_confidence"],
        },
    )
    birth_context.update(location_payload)
    return birth_context


def _build_birth_context_from_saved_fields(birth_date_str, *, raw_birth_place_input=None, normalized_birth_place=None, latitude=None, longitude=None, timezone=None, geocode_provider=None, geocode_confidence=None, fallback_place_text=None):
    if latitude is not None and longitude is not None and timezone:
        birth_context = utils.build_birth_context(
            birth_date_str,
            {
                "raw_input": raw_birth_place_input or fallback_place_text or normalized_birth_place or "",
                "normalized_place": normalized_birth_place or raw_birth_place_input or fallback_place_text or "",
                "latitude": latitude,
                "longitude": longitude,
                "timezone": timezone,
                "provider": geocode_provider or "stored",
                "confidence": geocode_confidence,
            },
        )
        birth_context.update(
            {
                "raw_birth_place_input": raw_birth_place_input or fallback_place_text or normalized_birth_place,
                "normalized_birth_place": normalized_birth_place or raw_birth_place_input or fallback_place_text,
                "latitude": latitude,
                "longitude": longitude,
                "timezone": timezone,
                "geocode_provider": geocode_provider or "stored",
                "geocode_confidence": geocode_confidence,
                "location_source": "stored_record",
                "geocode_cache_hit": None,
            }
        )
        return birth_context

    if fallback_place_text:
        return _build_birth_context(birth_date_str, fallback_place_text)

    raise BirthPlaceResolutionError(
        "Birth location is missing normalized geo data.",
        code="missing_geo_data",
        details={"birth_date": birth_date_str},
    )


def _log_chart_calculation_audit(*, location_payload, birth_context, natal_data):
    ascendant = natal_data.get("ascendant") or {}
    calculation_config = natal_data.get("calculation_config") or {}
    log_method = logger.info if ASTRO_DEBUG else logger.debug
    log_method(
        "Chart calculation audit raw_birth_place_input=%s normalized_birth_place=%s latitude=%s longitude=%s timezone=%s local_datetime=%s utc_datetime=%s ascendant_sign=%s ascendant_degree=%s ayanamsa_requested=%s ayanamsa_applied=%s ayanamsa_supported=%s node_mode=%s house_system=%s engine_version=%s location_source=%s geocode_cache_hit=%s planet_longitudes=%s",
        location_payload.get("raw_birth_place_input"),
        location_payload.get("normalized_birth_place"),
        location_payload.get("latitude"),
        location_payload.get("longitude"),
        location_payload.get("timezone"),
        birth_context.get("local_datetime").isoformat() if birth_context.get("local_datetime") else None,
        birth_context.get("utc_datetime").isoformat() if birth_context.get("utc_datetime") else None,
        ascendant.get("sign_idx"),
        ascendant.get("degree"),
        calculation_config.get("ayanamsa_requested", ASTRO_CONFIG["ayanamsa"]),
        calculation_config.get("ayanamsa_applied"),
        calculation_config.get("ayanamsa_supported"),
        calculation_config.get("node_mode", ASTRO_CONFIG["node_mode"]),
        calculation_config.get("house_system", ASTRO_CONFIG["house_system"]),
        calculation_config.get("engine_version", ASTRO_ENGINE_VERSION),
        location_payload.get("location_source"),
        location_payload.get("geocode_cache_hit"),
        {planet["name"]: planet.get("abs_longitude") for planet in natal_data.get("planets", [])},
    )


def _make_calculation_context(birth_context):
    return CalculationContext(
        datetime_local=birth_context["local_datetime"],
        datetime_utc=birth_context["utc_datetime"],
        latitude=birth_context["latitude"],
        longitude=birth_context["longitude"],
        timezone=birth_context["timezone"],
        ayanamsa=ASTRO_CONFIG["ayanamsa"],
        node_mode=ASTRO_CONFIG["node_mode"],
        house_system=ASTRO_CONFIG["house_system"],
    )


def _calculate_chart_bundle_from_birth_context(birth_context, *, personalization=None):
    calculation_context = _make_calculation_context(birth_context)
    birth_dt = calculation_context.datetime_utc
    lat = birth_context["latitude"]
    lon = birth_context["longitude"]
    natal_data = engines_natal.calculate_natal_data(calculation_context)
    _log_chart_calculation_audit(location_payload=birth_context, birth_context=birth_context, natal_data=natal_data)
    moon_lon = next(p["abs_longitude"] for p in natal_data["planets"] if p["name"] == "Moon")
    dasha_data = engines_dasha.calculate_vims_dasha(calculation_context, moon_lon)
    navamsa_data = engines_navamsa.calculate_navamsa(natal_data)
    current_transits = engines_transits.get_current_transits(calculation_context)
    transit_data = engines_transits.score_current_impact(natal_data, current_transits)
    eclipse_data = engines_eclipses.calculate_upcoming_eclipses(calculation_context, natal_data=natal_data)
    fullmoon_data = []
    if engines_fullmoons:
        fullmoon_data = engines_fullmoons.calculate_upcoming_fullmoons(birth_dt, lat, lon, natal_data)
    interpretation_context = {
        "signal_layer": _build_interpretation_accuracy_context(
            natal_data,
            dasha_data,
            personalization=personalization or {},
            transit_data=transit_data,
        )
    }
    interpretation_context["recommendation_layer"] = interpretation_context["signal_layer"].get("recommendation_layer", {})
    return {
        "calculation_context": calculation_context,
        "birth_context": birth_context,
        "birth_summary": " | ".join(
            item for item in [
                birth_context.get("local_datetime").strftime("%Y-%m-%d") if birth_context.get("local_datetime") else None,
                birth_context.get("local_datetime").strftime("%H:%M") if birth_context.get("local_datetime") else None,
                birth_context.get("normalized_birth_place"),
            ] if item
        ),
        "natal_data": natal_data,
        "dasha_data": dasha_data,
        "navamsa_data": navamsa_data,
        "transit_data": transit_data,
        "eclipse_data": eclipse_data,
        "fullmoon_data": fullmoon_data,
        "interpretation_context": interpretation_context,
        "calculation_metadata": build_calculation_metadata_snapshot(
            calculation_context=calculation_context,
            birth_context=birth_context,
        ),
        "calculation_config": _build_calculation_config_payload(calculation_context),
    }


def build_insight(title, severity, category, summary, supporting_metrics, recommended_action):
    return {
        "title": title,
        "severity": severity,
        "category": category,
        "summary": summary,
        "supporting_metrics": supporting_metrics or {},
        "recommended_action": recommended_action,
    }


def _round_ratio(numerator, denominator):
    if not denominator:
        return 0.0
    return round(numerator / denominator, 4)


def _percent(value):
    return round(float(value or 0) * 100, 2)


def compute_conversion_metrics(db):
    total_users = db.query(db_mod.AppUser).count()
    free_users = db.query(db_mod.AppUser).filter(db_mod.AppUser.plan_code == "free").count()
    paid_users = db.query(db_mod.AppUser).filter(db_mod.AppUser.plan_code != "free").count()
    free_multi_report_users = 0
    free_heavy_users = 0
    for user in db.query(db_mod.AppUser).all():
        count = db.query(db_mod.GeneratedReport).filter(db_mod.GeneratedReport.user_id == user.id).count()
        if user.plan_code == "free" and count >= 2:
            free_multi_report_users += 1
        if user.plan_code == "free" and count >= 3:
            free_heavy_users += 1
    return {
        "total_users": total_users,
        "free_users": free_users,
        "paid_users": paid_users,
        "conversion_rate": _round_ratio(paid_users, total_users),
        "free_multi_report_users": free_multi_report_users,
        "free_heavy_users": free_heavy_users,
    }


def compute_retention_metrics(db):
    total_users = db.query(db_mod.AppUser).count()
    reports = db.query(db_mod.GeneratedReport.user_id, db_mod.GeneratedReport.created_at).all()
    user_dates = {}
    for user_id, created_at in reports:
        if not user_id or not created_at:
            continue
        user_dates.setdefault(user_id, set()).add(created_at.date())
    returning_users = sum(1 for dates in user_dates.values() if len(dates) >= 2)
    return {
        "returning_users": returning_users,
        "returning_rate": _round_ratio(returning_users, total_users),
    }


def compute_engagement_metrics(db, now):
    week_ago = now - timedelta(days=7)
    month_ago = now - timedelta(days=30)
    reports_last_7_days = db.query(db_mod.GeneratedReport).filter(db_mod.GeneratedReport.created_at >= week_ago).count()
    reports_last_30_days = db.query(db_mod.GeneratedReport).filter(db_mod.GeneratedReport.created_at >= month_ago).count()
    active_users_last_7_days = len({
        user_id
        for (user_id,) in db.query(db_mod.GeneratedReport.user_id).filter(
            db_mod.GeneratedReport.created_at >= week_ago
        ).distinct().all()
        if user_id
    })
    active_users_last_30_days = len({
        user_id
        for (user_id,) in db.query(db_mod.GeneratedReport.user_id).filter(
            db_mod.GeneratedReport.created_at >= month_ago
        ).distinct().all()
        if user_id
    })
    total_reports = db.query(db_mod.GeneratedReport).count()
    all_report_counts = {}
    for user_id, created_at in db.query(db_mod.GeneratedReport.user_id, db_mod.GeneratedReport.created_at).all():
        if not user_id:
            continue
        all_report_counts[user_id] = all_report_counts.get(user_id, 0) + 1
    top10_reports = sum(sorted(all_report_counts.values(), reverse=True)[:10])
    return {
        "reports_last_7_days": reports_last_7_days,
        "reports_last_30_days": reports_last_30_days,
        "active_users_last_7_days": active_users_last_7_days,
        "active_users_last_30_days": active_users_last_30_days,
        "top10_report_share": _round_ratio(top10_reports, total_reports),
        "total_reports": total_reports,
    }


def compute_revenue_proxy_metrics(db):
    total_users = db.query(db_mod.AppUser).count()
    plan_distribution = {
        plan: db.query(db_mod.AppUser).filter(db_mod.AppUser.plan_code == plan).count()
        for plan in PLAN_FEATURES
    }
    premium_elite_users = plan_distribution.get("premium", 0) + plan_distribution.get("elite", 0)
    paid_users = sum(count for plan, count in plan_distribution.items() if plan != "free")
    basic_share_of_paid = _round_ratio(plan_distribution.get("basic", 0), paid_users)
    return {
        "total_users": total_users,
        "plan_distribution": plan_distribution,
        "premium_elite_users": premium_elite_users,
        "basic_share_of_paid": basic_share_of_paid,
        "paid_users": paid_users,
    }


def compute_billing_signal_metrics(db, now):
    month_ago = now - timedelta(days=30)
    recent_logs = db.query(db_mod.EmailLog).filter(db_mod.EmailLog.created_at >= month_ago).all()
    failed = sum(1 for log in recent_logs if log.status == "failed")
    skipped = sum(1 for log in recent_logs if log.status == "skipped")
    sent = sum(1 for log in recent_logs if log.status == "sent")
    payment_failed = sum(1 for log in recent_logs if log.email_type == "payment_failed")
    cancellation = sum(1 for log in recent_logs if log.email_type == "cancellation")
    recovery = sum(1 for log in recent_logs if log.email_type == "payment_recovery")
    return {
        "email_sent_30d": sent,
        "email_failed_30d": failed,
        "email_skipped_30d": skipped,
        "payment_failed_30d": payment_failed,
        "cancellation_30d": cancellation,
        "recovery_30d": recovery,
    }


def build_upsell_candidates(db):
    users = db.query(db_mod.AppUser).all()
    report_counts = {}
    for (user_id,) in db.query(db_mod.GeneratedReport.user_id).all():
        if not user_id:
            continue
        report_counts[user_id] = report_counts.get(user_id, 0) + 1

    candidates = []
    for user in users:
        count = report_counts.get(user.id, 0)
        reason = None
        target_plan = None
        if user.plan_code == "free" and count >= 3:
            reason = "Free user has repeated report usage and has likely hit a stronger upgrade intent."
            target_plan = "basic"
        elif user.plan_code == "basic" and count >= 5:
            reason = "Basic user shows sustained usage that may fit premium depth and timing features."
            target_plan = "premium"
        elif user.plan_code == "premium" and count >= 8:
            reason = "Premium user shows heavy repeat behavior and may respond to elite guidance positioning."
            target_plan = "elite"
        if reason:
            candidates.append({
                "user_id": user.id,
                "email": user.email,
                "current_plan": user.plan_code,
                "report_count": count,
                "target_plan": target_plan,
                "reason": reason,
            })
    candidates.sort(key=lambda item: (-item["report_count"], item["email"]))
    return candidates[:12]


def generate_admin_insights(db):
    now = datetime.utcnow()
    conversion = compute_conversion_metrics(db)
    retention = compute_retention_metrics(db)
    engagement = compute_engagement_metrics(db, now)
    revenue = compute_revenue_proxy_metrics(db)
    billing = compute_billing_signal_metrics(db, now)
    upsell_candidates = build_upsell_candidates(db)
    insights = []

    conversion_rate = conversion["conversion_rate"]
    if conversion["total_users"] >= 10:
        if conversion_rate < 0.03:
            insights.append(build_insight(
                "Low free-to-paid conversion",
                "warning",
                "conversion",
                f"Only {_percent(conversion_rate)}% of users are on paid plans.",
                {
                    "total_users": conversion["total_users"],
                    "paid_users": conversion["paid_users"],
                    "conversion_rate": _percent(conversion_rate),
                },
                "Audit upgrade messaging and paywall timing around the result page and PDF CTA.",
            ))
        elif conversion_rate <= 0.08:
            insights.append(build_insight(
                "Paid conversion is emerging",
                "info",
                "conversion",
                f"Paid penetration is {_percent(conversion_rate)}%, which shows signal but still leaves room to tighten the upgrade path.",
                {
                    "total_users": conversion["total_users"],
                    "paid_users": conversion["paid_users"],
                    "conversion_rate": _percent(conversion_rate),
                },
                "Compare upgrade prompts across free, basic, and premium journeys to find the strongest conversion moment.",
            ))
        else:
            insights.append(build_insight(
                "Healthy free-to-paid conversion",
                "success",
                "conversion",
                f"Paid penetration reached {_percent(conversion_rate)}%, which suggests the monetization path is landing well.",
                {
                    "total_users": conversion["total_users"],
                    "paid_users": conversion["paid_users"],
                    "conversion_rate": _percent(conversion_rate),
                },
                "Preserve the current conversion flow and test incremental pricing or positioning improvements carefully.",
            ))

    if conversion["free_multi_report_users"] >= 5:
        insights.append(build_insight(
            "Strong free-user upsell pool",
            "info",
            "conversion",
            f"{conversion['free_multi_report_users']} free users have already generated at least two reports.",
            {
                "free_multi_report_users": conversion["free_multi_report_users"],
                "free_heavy_users": conversion["free_heavy_users"],
            },
            "Prioritize upgrade prompts for repeat free users right after a second or third report interaction.",
        ))

    returning_rate = retention["returning_rate"]
    if retention["returning_users"] or conversion["total_users"]:
        if returning_rate < 0.15:
            insights.append(build_insight(
                "Weak returning-user retention",
                "warning",
                "retention",
                f"Only {_percent(returning_rate)}% of users returned on multiple dates.",
                {
                    "returning_users": retention["returning_users"],
                    "returning_rate": _percent(returning_rate),
                },
                "Strengthen re-engagement loops with follow-up timing hooks and clearer reasons to revisit the dashboard.",
            ))
        elif returning_rate > 0.30:
            insights.append(build_insight(
                "Healthy returning-user behavior",
                "success",
                "retention",
                f"{_percent(returning_rate)}% of users returned on separate dates, which is a strong retention signal.",
                {
                    "returning_users": retention["returning_users"],
                    "returning_rate": _percent(returning_rate),
                },
                "Lean into continuity products such as monthly guidance and recurring timing updates.",
            ))
        else:
            insights.append(build_insight(
                "Retention is present but not deep yet",
                "info",
                "retention",
                f"Returning usage is at {_percent(returning_rate)}%, indicating some repeat value but not a durable habit yet.",
                {
                    "returning_users": retention["returning_users"],
                    "returning_rate": _percent(returning_rate),
                },
                "Test stronger revisit triggers after report generation and after timing-window milestones.",
            ))

    active_30 = engagement["active_users_last_30_days"]
    total_users = max(conversion["total_users"], 1)
    active_30_rate = _round_ratio(active_30, total_users)
    if active_30_rate < 0.2 and conversion["total_users"] >= 10:
        insights.append(build_insight(
            "Recent engagement is thin",
            "warning",
            "engagement",
            f"Only {_percent(active_30_rate)}% of users were active in the last 30 days.",
            {
                "active_users_last_30_days": active_30,
                "reports_last_30_days": engagement["reports_last_30_days"],
            },
            "Revisit onboarding and revisit nudges so more accounts return after the first report.",
        ))
    elif active_30_rate > 0.35:
        insights.append(build_insight(
            "Recent engagement is healthy",
            "success",
            "engagement",
            f"{_percent(active_30_rate)}% of users were active in the last 30 days.",
            {
                "active_users_last_30_days": active_30,
                "reports_last_30_days": engagement["reports_last_30_days"],
            },
            "Protect the current engagement loop and look for upsell timing inside repeat-use moments.",
        ))

    if engagement["top10_report_share"] > 0.5 and engagement["total_reports"] >= 20:
        insights.append(build_insight(
            "Usage is concentrated in a small user segment",
            "warning",
            "engagement",
            f"Top 10 users account for {_percent(engagement['top10_report_share'])}% of all reports.",
            {
                "top10_report_share": _percent(engagement["top10_report_share"]),
                "total_reports": engagement["total_reports"],
            },
            "Review whether the product is creating broad repeat value or over-serving a very small power-user group.",
        ))

    if revenue["total_users"] >= 15 and revenue["premium_elite_users"] == 0:
        insights.append(build_insight(
            "No premium or elite penetration yet",
            "critical",
            "revenue",
            "The paid mix has not moved beyond the lower tiers yet.",
            {
                "total_users": revenue["total_users"],
                "premium_users": revenue["plan_distribution"].get("premium", 0),
                "elite_users": revenue["plan_distribution"].get("elite", 0),
            },
            "Review premium positioning, feature differentiation, and why advanced guidance is not converting.",
        ))
    elif revenue["paid_users"] and revenue["basic_share_of_paid"] > 0.7:
        insights.append(build_insight(
            "Paid base is heavily basic-tier weighted",
            "info",
            "revenue",
            f"{_percent(revenue['basic_share_of_paid'])}% of paid users are still on the basic plan.",
            {
                "basic_users": revenue["plan_distribution"].get("basic", 0),
                "premium_users": revenue["plan_distribution"].get("premium", 0),
                "elite_users": revenue["plan_distribution"].get("elite", 0),
            },
            "Sharpen the value step-up between basic and premium, especially around timing intelligence and deeper guidance.",
        ))
    elif revenue["premium_elite_users"] > 0:
        insights.append(build_insight(
            "Higher-tier monetization exists",
            "success",
            "revenue",
            "Premium or elite plans are present, which suggests users are recognizing advanced value.",
            {
                "premium_users": revenue["plan_distribution"].get("premium", 0),
                "elite_users": revenue["plan_distribution"].get("elite", 0),
            },
            "Study the behaviors of higher-tier users and mirror those triggers in upgrade messaging.",
        ))

    if billing["payment_failed_30d"] > billing["cancellation_30d"] and billing["payment_failed_30d"] > 0:
        insights.append(build_insight(
            "Billing recovery attention needed",
            "warning",
            "billing",
            "Payment failures are outpacing cancellations, which often points to recoverable revenue risk.",
            {
                "payment_failed_30d": billing["payment_failed_30d"],
                "cancellation_30d": billing["cancellation_30d"],
                "recovery_30d": billing["recovery_30d"],
            },
            "Audit recovery messaging and make sure account and billing paths reduce friction after a failed payment.",
        ))
    elif billing["cancellation_30d"] > 0:
        insights.append(build_insight(
            "Recent cancellations need review",
            "info",
            "billing",
            "Cancellation signals are present and worth reviewing against product usage and plan value perception.",
            {
                "payment_failed_30d": billing["payment_failed_30d"],
                "cancellation_30d": billing["cancellation_30d"],
                "recovery_30d": billing["recovery_30d"],
            },
            "Look for patterns in plan tier, usage depth, and post-upgrade experience before cancellation.",
        ))

    total_email_events = billing["email_sent_30d"] + billing["email_failed_30d"] + billing["email_skipped_30d"]
    failed_ratio = _round_ratio(billing["email_failed_30d"], total_email_events)
    skipped_ratio = _round_ratio(billing["email_skipped_30d"], total_email_events)
    if total_email_events and failed_ratio > 0.2:
        insights.append(build_insight(
            "Email delivery failure rate is elevated",
            "critical",
            "ops",
            f"{_percent(failed_ratio)}% of recent email attempts failed.",
            {
                "email_sent_30d": billing["email_sent_30d"],
                "email_failed_30d": billing["email_failed_30d"],
                "email_skipped_30d": billing["email_skipped_30d"],
            },
            "Check SMTP/provider stability and email content issues before billing and lifecycle communication suffers.",
        ))
    elif total_email_events and skipped_ratio > 0.25:
        insights.append(build_insight(
            "Email sends are being skipped unusually often",
            "warning",
            "ops",
            f"{_percent(skipped_ratio)}% of recent email attempts were skipped.",
            {
                "email_sent_30d": billing["email_sent_30d"],
                "email_failed_30d": billing["email_failed_30d"],
                "email_skipped_30d": billing["email_skipped_30d"],
            },
            "Review email configuration and trigger conditions so operational messages are not silently dropped.",
        ))
    elif total_email_events:
        insights.append(build_insight(
            "Email operations look healthy",
            "success",
            "ops",
            "Recent transactional email activity looks stable from an operations perspective.",
            {
                "email_sent_30d": billing["email_sent_30d"],
                "email_failed_30d": billing["email_failed_30d"],
                "email_skipped_30d": billing["email_skipped_30d"],
            },
            "Keep monitoring delivery quality as billing and lifecycle volume grows.",
        ))

    quick_metrics = {
        "conversion_rate_pct": _percent(conversion["conversion_rate"]),
        "returning_rate_pct": _percent(retention["returning_rate"]),
        "active_users_last_7_days": engagement["active_users_last_7_days"],
        "active_users_last_30_days": engagement["active_users_last_30_days"],
        "top10_report_share_pct": _percent(engagement["top10_report_share"]),
        "payment_failed_30d": billing["payment_failed_30d"],
        "cancellation_30d": billing["cancellation_30d"],
        "recovery_30d": billing["recovery_30d"],
    }

    billing_watchlist = [
        _email_log_view(log)
        for log in db.query(db_mod.EmailLog)
        .filter(db_mod.EmailLog.email_type.in_(["payment_failed", "payment_recovery", "cancellation"]))
        .order_by(db_mod.EmailLog.created_at.desc())
        .limit(12)
        .all()
    ]

    headline_kpis = {
        "total_users": conversion["total_users"],
        "paid_users": conversion["paid_users"],
        "conversion_rate_pct": _percent(conversion["conversion_rate"]),
        "returning_rate_pct": _percent(retention["returning_rate"]),
        "active_users_last_30_days": engagement["active_users_last_30_days"],
        "upsell_candidates": len(upsell_candidates),
    }

    return {
        "headline_kpis": headline_kpis,
        "insights": insights,
        "upsell_candidates": upsell_candidates,
        "billing_watchlist": billing_watchlist,
        "quick_metrics": quick_metrics,
    }


def build_trend_snapshot(current_value, previous_value):
    delta = current_value - previous_value
    if delta > 0:
        direction = "up"
    elif delta < 0:
        direction = "down"
    else:
        direction = "flat"
    return {
        "current": current_value,
        "previous": previous_value,
        "delta": delta,
        "direction": direction,
    }


def classify_scorecard_status(metric_name, value, context=None):
    context = context or {}
    if metric_name == "conversion_rate":
        if value < 0.03:
            return "risk"
        if value <= 0.08:
            return "watch"
        return "good"
    if metric_name == "returning_rate":
        if value < 0.15:
            return "risk"
        if value <= 0.30:
            return "watch"
        return "good"
    if metric_name == "email_failure_rate":
        if value > 0.2:
            return "risk"
        if value > 0.08:
            return "watch"
        return "good"
    if metric_name == "inactive_paid_users":
        paid_users = max(int(context.get("paid_users", 0)), 1)
        ratio = value / paid_users
        if ratio > 0.4:
            return "risk"
        if ratio > 0.2:
            return "watch"
        return "good"
    if metric_name == "payment_failed_signal_count":
        if value >= 5:
            return "risk"
        if value >= 2:
            return "watch"
        return "good"
    return "watch"


def compute_executive_kpis(db):
    now = datetime.utcnow()
    week_ago = now - timedelta(days=7)
    prev_week_ago = now - timedelta(days=14)
    month_ago = now - timedelta(days=30)
    prev_month_ago = now - timedelta(days=60)

    conversion = compute_conversion_metrics(db)
    retention = compute_retention_metrics(db)
    engagement = compute_engagement_metrics(db, now)
    revenue = compute_revenue_proxy_metrics(db)
    billing = compute_billing_signal_metrics(db, now)
    segment_context = generate_lifecycle_segments(db)

    new_users_last_7_days = db.query(db_mod.AppUser).filter(db_mod.AppUser.created_at >= week_ago).count()
    new_users_previous_7_days = db.query(db_mod.AppUser).filter(
        db_mod.AppUser.created_at >= prev_week_ago,
        db_mod.AppUser.created_at < week_ago,
    ).count()
    new_users_last_30_days = db.query(db_mod.AppUser).filter(db_mod.AppUser.created_at >= month_ago).count()
    new_users_previous_30_days = db.query(db_mod.AppUser).filter(
        db_mod.AppUser.created_at >= prev_month_ago,
        db_mod.AppUser.created_at < month_ago,
    ).count()

    reports_previous_7_days = db.query(db_mod.GeneratedReport).filter(
        db_mod.GeneratedReport.created_at >= prev_week_ago,
        db_mod.GeneratedReport.created_at < week_ago,
    ).count()
    reports_previous_30_days = db.query(db_mod.GeneratedReport).filter(
        db_mod.GeneratedReport.created_at >= prev_month_ago,
        db_mod.GeneratedReport.created_at < month_ago,
    ).count()

    active_users_previous_7_days = len({
        user_id
        for (user_id,) in db.query(db_mod.GeneratedReport.user_id).filter(
            db_mod.GeneratedReport.created_at >= prev_week_ago,
            db_mod.GeneratedReport.created_at < week_ago,
        ).distinct().all()
        if user_id
    })
    active_users_previous_30_days = len({
        user_id
        for (user_id,) in db.query(db_mod.GeneratedReport.user_id).filter(
            db_mod.GeneratedReport.created_at >= prev_month_ago,
            db_mod.GeneratedReport.created_at < month_ago,
        ).distinct().all()
        if user_id
    })

    previous_billing_logs = db.query(db_mod.EmailLog).filter(
        db_mod.EmailLog.created_at >= prev_week_ago,
        db_mod.EmailLog.created_at < week_ago,
    ).all()
    payment_failed_previous_7_days = sum(1 for log in previous_billing_logs if log.email_type == "payment_failed")

    total_email_logs_30d = (
        billing["email_sent_30d"] + billing["email_failed_30d"] + billing["email_skipped_30d"]
    )
    email_failure_rate = _round_ratio(billing["email_failed_30d"], total_email_logs_30d)

    premium_plus_users = revenue["plan_distribution"].get("premium", 0) + revenue["plan_distribution"].get("elite", 0)
    estimated_mrr_score = (
        revenue["plan_distribution"].get("basic", 0)
        + (revenue["plan_distribution"].get("premium", 0) * 2)
        + (revenue["plan_distribution"].get("elite", 0) * 3)
    )

    kpis = {
        "total_users": conversion["total_users"],
        "new_users_last_7_days": new_users_last_7_days,
        "new_users_last_30_days": new_users_last_30_days,
        "total_reports": engagement["total_reports"],
        "reports_last_7_days": engagement["reports_last_7_days"],
        "reports_last_30_days": engagement["reports_last_30_days"],
        "active_users_last_7_days": engagement["active_users_last_7_days"],
        "active_users_last_30_days": engagement["active_users_last_30_days"],
        "paid_users": conversion["paid_users"],
        "conversion_rate": conversion["conversion_rate"],
        "premium_plus_users": premium_plus_users,
        "estimated_mrr_score": estimated_mrr_score,
        "returning_users": retention["returning_users"],
        "returning_rate": retention["returning_rate"],
        "inactive_paid_users": len(segment_context["segments"].get("INACTIVE_PAID_USERS", [])),
        "payment_failed_signal_count": billing["payment_failed_30d"],
        "cancellation_signal_count": billing["cancellation_30d"],
        "email_failed_count": billing["email_failed_30d"],
        "email_skipped_count": billing["email_skipped_30d"],
        "email_failure_rate": email_failure_rate,
    }

    scorecards = [
        {"label": "Conversion Rate", "value": f"{_percent(kpis['conversion_rate'])}%", "status": classify_scorecard_status("conversion_rate", kpis["conversion_rate"])},
        {"label": "Returning Rate", "value": f"{_percent(kpis['returning_rate'])}%", "status": classify_scorecard_status("returning_rate", kpis["returning_rate"])},
        {"label": "Inactive Paid Users", "value": kpis["inactive_paid_users"], "status": classify_scorecard_status("inactive_paid_users", kpis["inactive_paid_users"], {"paid_users": kpis["paid_users"]})},
        {"label": "Email Failure Rate", "value": f"{_percent(kpis['email_failure_rate'])}%", "status": classify_scorecard_status("email_failure_rate", kpis["email_failure_rate"])},
        {"label": "Payment Failed Signals", "value": kpis["payment_failed_signal_count"], "status": classify_scorecard_status("payment_failed_signal_count", kpis["payment_failed_signal_count"])},
    ]

    trends = {
        "new_users_7d": build_trend_snapshot(new_users_last_7_days, new_users_previous_7_days),
        "new_users_30d": build_trend_snapshot(new_users_last_30_days, new_users_previous_30_days),
        "reports_7d": build_trend_snapshot(engagement["reports_last_7_days"], reports_previous_7_days),
        "reports_30d": build_trend_snapshot(engagement["reports_last_30_days"], reports_previous_30_days),
        "active_users_7d": build_trend_snapshot(engagement["active_users_last_7_days"], active_users_previous_7_days),
        "active_users_30d": build_trend_snapshot(engagement["active_users_last_30_days"], active_users_previous_30_days),
        "payment_failed_7d": build_trend_snapshot(billing["payment_failed_30d"], payment_failed_previous_7_days),
    }
    return {"kpis": kpis, "scorecards": scorecards, "trends": trends, "segment_context": segment_context, "revenue": revenue, "billing": billing}


def build_watchlist_items(kpis, trends, revenue, segment_context):
    items = []
    if kpis["conversion_rate"] < 0.03:
        items.append({"title": "Low paid conversion", "severity": "risk", "reason": "Free-to-paid conversion remains below 3%.", "next_link": "/admin/revenue"})
    if revenue["premium_elite_users"] == 0 and revenue["total_users"] >= 15:
        items.append({"title": "No premium or elite penetration", "severity": "watch", "reason": "Monetization is not yet reaching higher-value tiers.", "next_link": "/admin/revenue"})
    if kpis["inactive_paid_users"] > 0:
        items.append({"title": "Inactive paid users need attention", "severity": classify_scorecard_status("inactive_paid_users", kpis["inactive_paid_users"], {"paid_users": kpis["paid_users"]}), "reason": f"{kpis['inactive_paid_users']} paid users show no recent activity.", "next_link": "/admin/segments?group=retention"})
    if kpis["payment_failed_signal_count"] > 0:
        items.append({"title": "Billing recovery pressure", "severity": classify_scorecard_status("payment_failed_signal_count", kpis["payment_failed_signal_count"]), "reason": f"{kpis['payment_failed_signal_count']} payment-failed signals were logged recently.", "next_link": "/admin/segments?group=retention&segment=CHURN_RISK_USERS"})
    if kpis["email_skipped_count"] > max(3, kpis["email_failed_count"]):
        items.append({"title": "Email delivery config needs review", "severity": "watch", "reason": "Skipped email volume is elevated relative to failed sends.", "next_link": "/admin/emails"})
    if trends["reports_7d"]["direction"] == "down":
        items.append({"title": "Weekly report activity is softening", "severity": "watch", "reason": "Last 7-day report volume is below the previous 7-day period.", "next_link": "/admin/insights"})
    return items[:6]


def build_priority_focus_items(kpis, watchlist_items):
    focus = []
    if kpis["conversion_rate"] < 0.08:
        focus.append("Convert active free users and sharpen upgrade timing.")
    if kpis["inactive_paid_users"] > 0:
        focus.append("Re-engage inactive paid users before value perception drops.")
    if kpis["payment_failed_signal_count"] > 0:
        focus.append("Review billing recovery friction and support clarity.")
    if kpis["email_failure_rate"] > 0.08:
        focus.append("Inspect email reliability before lifecycle messaging weakens.")
    if not focus and watchlist_items:
        focus.append("Review current watchlist items and protect the strongest growth loops.")
    if not focus:
        focus.append("Maintain current momentum and keep watching conversion and retention quality.")
    return focus[:3]


def build_weekly_executive_summary(kpis, trends, watchlist_items):
    growth_status = "steady"
    if trends["new_users_7d"]["direction"] == "up" and trends["reports_7d"]["direction"] == "up":
        growth_status = "improving"
    elif trends["new_users_7d"]["direction"] == "down" or trends["reports_7d"]["direction"] == "down":
        growth_status = "softening"

    conversion_phrase = "healthy" if kpis["conversion_rate"] > 0.08 else "emerging" if kpis["conversion_rate"] >= 0.03 else "weak"
    retention_phrase = "above baseline" if kpis["returning_rate"] > 0.30 else "mixed" if kpis["returning_rate"] >= 0.15 else "fragile"
    billing_phrase = "stable" if kpis["payment_failed_signal_count"] == 0 else "needs attention"

    headline = f"Business momentum is {growth_status} this week, while conversion remains {conversion_phrase}."
    growth_summary = f"New users: {kpis['new_users_last_7_days']} in the last 7 days, with {kpis['reports_last_7_days']} reports generated."
    revenue_summary = f"Paid users: {kpis['paid_users']} with {_percent(kpis['conversion_rate'])}% conversion and MRR proxy score {kpis['estimated_mrr_score']}."
    engagement_summary = f"Returning usage is {retention_phrase}, with {kpis['active_users_last_30_days']} active users in the last 30 days."
    risk_summary = f"Billing and ops are {billing_phrase}; payment_failed={kpis['payment_failed_signal_count']}, email_failed={kpis['email_failed_count']}, email_skipped={kpis['email_skipped_count']}."
    recommended_focus = build_priority_focus_items(kpis, watchlist_items)
    return {
        "headline": headline,
        "growth_summary": growth_summary,
        "revenue_summary": revenue_summary,
        "engagement_summary": engagement_summary,
        "risk_summary": risk_summary,
        "recommended_focus": recommended_focus,
    }


def _next_page_for_insight(insight):
    category = str((insight or {}).get("category", "")).strip().lower()
    mapping = {
        "conversion": "/admin/revenue",
        "revenue": "/admin/revenue",
        "retention": "/admin/segments?group=retention",
        "engagement": "/admin/insights",
        "billing": "/admin/billing",
        "ops": "/admin/emails",
    }
    return mapping.get(category, "/admin")


def _serialize_insight_card(insight):
    item = dict(insight or {})
    item["next_page"] = _next_page_for_insight(item)
    return item


def build_admin_summary_api_payload(db):
    executive = compute_executive_kpis(db)
    watchlist_items = build_watchlist_items(
        executive["kpis"],
        executive["trends"],
        executive["revenue"],
        executive["segment_context"],
    )
    weekly_summary = build_weekly_executive_summary(executive["kpis"], executive["trends"], watchlist_items)
    return {
        "kpis": executive["kpis"],
        "scorecards": executive["scorecards"],
        "trends": executive["trends"],
        "watchlist": watchlist_items,
        "priority_focus": weekly_summary["recommended_focus"],
        "weekly_summary": weekly_summary,
        "links": {
            "revenue": "/admin/revenue",
            "insights": "/admin/insights",
            "segments": "/admin/segments",
            "emails": "/admin/emails",
            "billing": "/admin/billing",
            "users": "/admin/users",
        },
    }


def build_admin_revenue_api_payload(db):
    now = datetime.utcnow()
    week_ago = now - timedelta(days=7)
    month_ago = now - timedelta(days=30)
    conversion = compute_conversion_metrics(db)
    engagement = compute_engagement_metrics(db, now)
    revenue = compute_revenue_proxy_metrics(db)
    billing = compute_billing_signal_metrics(db, now)
    recent_paid_users = (
        db.query(db_mod.AppUser)
        .filter(db_mod.AppUser.plan_code != "free")
        .order_by(db_mod.AppUser.plan_started_at.desc(), db_mod.AppUser.created_at.desc())
        .limit(10)
        .all()
    )
    return {
        "core_metrics": {
            "total_users": conversion["total_users"],
            "free_users": conversion["free_users"],
            "paid_users": conversion["paid_users"],
            "total_reports": engagement["total_reports"],
        },
        "conversion_metrics": {
            "conversion_rate": conversion["conversion_rate"],
            "free_multi_report_users": conversion["free_multi_report_users"],
            "free_heavy_users": conversion["free_heavy_users"],
        },
        "usage_metrics": {
            "reports_last_7_days": engagement["reports_last_7_days"],
            "reports_last_30_days": engagement["reports_last_30_days"],
            "active_users_last_7_days": engagement["active_users_last_7_days"],
            "active_users_last_30_days": engagement["active_users_last_30_days"],
        },
        "revenue_proxy": {
            "plan_distribution": revenue["plan_distribution"],
            "premium_elite_users": revenue["premium_elite_users"],
            "basic_share_of_paid": revenue["basic_share_of_paid"],
            "estimated_mrr_score": (
                revenue["plan_distribution"].get("basic", 0)
                + (revenue["plan_distribution"].get("premium", 0) * 2)
                + (revenue["plan_distribution"].get("elite", 0) * 3)
            ),
        },
        "funnel": {
            "new_users_last_7_days": db.query(db_mod.AppUser).filter(db_mod.AppUser.created_at >= week_ago).count(),
            "new_users_last_30_days": db.query(db_mod.AppUser).filter(db_mod.AppUser.created_at >= month_ago).count(),
            "paid_users": conversion["paid_users"],
        },
        "engagement": {
            "avg_reports_per_user": round(engagement["total_reports"] / conversion["total_users"], 2) if conversion["total_users"] else 0.0,
            "top10_report_share": engagement["top10_report_share"],
        },
        "billing_signals": billing,
        "top_users": [_user_admin_view(user, db.query(db_mod.GeneratedReport).filter(db_mod.GeneratedReport.user_id == user.id).count()) for user in recent_paid_users],
    }


def build_admin_insights_api_payload(db):
    data = generate_admin_insights(db)
    return {
        "headline_kpis": data["headline_kpis"],
        "insights": [_serialize_insight_card(item) for item in data["insights"]],
        "upsell_candidates": data["upsell_candidates"],
        "churn_watchlist": data["billing_watchlist"],
        "quick_metrics": data["quick_metrics"],
    }


def _email_base_context(user=None, **extra):
    config = email_utils.get_email_config()
    context = {
        "user_name": (user.name if user and getattr(user, "name", None) else (user.email.split("@")[0] if user else "there")),
        "user_email": getattr(user, "email", "") if user else "",
        "plan_label": get_plan_features(user).get("label", "Free") if user else "Free",
        "dashboard_url": config["app_base_url"].rstrip("/") + "/dashboard",
        "account_url": config["app_base_url"].rstrip("/") + "/account",
        "reports_url": config["app_base_url"].rstrip("/") + "/reports",
        "support_email": config.get("support_email") or config.get("from_address"),
        "billing_email": config.get("billing_email") or config.get("from_address"),
        "app_base_url": config["app_base_url"].rstrip("/"),
    }
    context.update(extra)
    return context


def _find_existing_email_log(db, email_type, recipient_email, event_key):
    if not event_key:
        return None
    return db.query(db_mod.EmailLog).filter(
        db_mod.EmailLog.email_type == email_type,
        db_mod.EmailLog.recipient_email == recipient_email,
        db_mod.EmailLog.related_event_key == event_key,
    ).first()


def _create_email_log(db, *, user_id, email_type, recipient_email, subject, status, related_event_type=None, related_event_key=None, provider_message_id=None, error_message=None):
    log_entry = db_mod.EmailLog(
        user_id=user_id,
        email_type=email_type,
        recipient_email=recipient_email,
        subject=subject,
        status=status,
        related_event_type=related_event_type,
        related_event_key=related_event_key,
        provider_message_id=provider_message_id,
        error_message=error_message,
    )
    db.add(log_entry)
    db.commit()
    return log_entry


def safe_send_template_email(db, *, user, email_type, template_name, subject, event_type=None, event_key=None, attachments=None, **context):
    recipient_email = getattr(user, "email", "") if user else context.get("to_email", "")
    if not recipient_email:
        logger.warning("Email skipped because recipient is missing email_type=%s", email_type)
        return {"status": "skipped", "reason": "missing_recipient"}

    existing = _find_existing_email_log(db, email_type, recipient_email, event_key)
    if existing:
        logger.info("Duplicate email suppressed email_type=%s recipient=%s event_key=%s", email_type, recipient_email, event_key)
        return {"status": "skipped", "reason": "duplicate", "email_log_id": existing.id}

    email_context = dict(context)
    email_context.pop("to_email", None)
    result = email_utils.send_template_email(
        recipient_email,
        template_name,
        subject,
        attachments=attachments,
        **_email_base_context(user, **email_context),
    )
    log_entry = _create_email_log(
        db,
        user_id=getattr(user, "id", None),
        email_type=email_type,
        recipient_email=recipient_email,
        subject=subject,
        status=result.status,
        related_event_type=event_type,
        related_event_key=event_key,
        provider_message_id=result.provider_message_id,
        error_message=result.error_message,
    )
    return {"status": result.status, "ok": result.ok, "email_log_id": log_entry.id}


def _report_order_admin_email():
    configured = str(os.getenv("INTERNAL_REVIEW_EMAIL", "")).strip()
    if configured:
        return configured
    legacy_configured = str(os.getenv("REPORT_ORDER_ADMIN_EMAIL", "")).strip()
    return legacy_configured or "info@focusastrology.com"


def _attach_astro_signal_context(payload, *, report_type=None):
    payload = _serialize_temporal_values(payload or {})
    normalized_report_type = astro_workspace.normalize_workspace_report_type(
        report_type or payload.get("workspace_report_type") or payload.get("report_order_type") or payload.get("report_type")
    )
    if payload.get("astro_signal_context"):
        if normalized_report_type == "parent_child":
            interaction_signals = astro_signal_enrichment.build_parent_child_interaction_signals(
                {
                    "language": payload.get("language"),
                    **(payload.get("astro_signal_context") or {}),
                },
                report_type="parent_child",
            )
            if interaction_signals.get("interaction_patterns") or interaction_signals.get("confidence_notes"):
                payload["astro_signal_context"]["parent_child_interaction_signals"] = interaction_signals
        return payload
    natal_data = payload.get("natal_data") or {}
    if not isinstance(natal_data, dict) or not natal_data.get("planets"):
        return payload
    try:
        payload["astro_signal_context"] = astro_signal_enrichment.build_astro_signal_context(
            natal_data,
            navamsa_data=payload.get("navamsa_data") or {},
            dasha_data=payload.get("dasha_data") or [],
            transit_context=payload.get("timing_data") or payload.get("interpretation_context") or {},
            report_type=normalized_report_type,
        )
    except Exception:
        logger.exception("Astro signal enrichment attach failed report_type=%s", normalized_report_type)
    return payload


def _report_order_chart_payload_from_existing_data(order_data):
    payload = {}
    for key in (
        "natal_data",
        "navamsa_data",
        "dasha_data",
        "timing_data",
        "transit_data",
        "eclipse_data",
        "fullmoon_data",
        "interpretation_context",
        "calculation_config",
        "calculation_metadata",
        "astro_signal_context",
    ):
        value = order_data.get(key)
        if value not in (None, "", [], {}):
            payload[key] = value
    has_natal = isinstance(payload.get("natal_data"), dict) and bool((payload.get("natal_data") or {}).get("planets"))
    if has_natal:
        return payload
    return {}


def _report_order_signal_fallback_note():
    return "Chart context unavailable; interpretation generated without signal enrichment."


def _combine_signal_confidence_notes(*contexts):
    notes = []
    for context in contexts:
        if isinstance(context, dict):
            notes.extend(context.get("confidence_notes") or [])
    return list(dict.fromkeys(note for note in notes if note))


def _build_report_order_birth_context(order_data, *, birth_date, birth_place, raw_birth_place_input=None, normalized_birth_place=None, latitude=None, longitude=None, timezone=None, geocode_provider=None, geocode_confidence=None, birth_country=None):
    if latitude not in (None, "") and longitude not in (None, "") and timezone:
        return _build_birth_context_from_saved_fields(
            birth_date,
            raw_birth_place_input=raw_birth_place_input or birth_place,
            normalized_birth_place=normalized_birth_place or birth_place,
            latitude=float(latitude),
            longitude=float(longitude),
            timezone=str(timezone).strip(),
            geocode_provider=geocode_provider,
            geocode_confidence=geocode_confidence,
            fallback_place_text=birth_place,
        )
    return _build_birth_context(
        birth_date,
        birth_place,
        birth_country or "",
    )


def _report_order_profile_input(order_data, *, role):
    if role == "parent":
        source = dict(order_data.get("parent_profile") or {})
        return {
            "full_name": source.get("full_name") or "Parent",
            "birth_date": source.get("birth_date") or "",
            "birth_time": source.get("birth_time") or "",
            "birth_place": source.get("birth_city") or source.get("birth_place") or "",
            "birth_country": source.get("birth_country") or "",
            "raw_birth_place_input": source.get("raw_birth_place_input") or source.get("birth_city") or source.get("birth_place"),
            "normalized_birth_place": source.get("normalized_birth_place"),
            "latitude": source.get("latitude") if source.get("latitude") not in (None, "") else source.get("lat"),
            "longitude": source.get("longitude") if source.get("longitude") not in (None, "") else source.get("lon"),
            "timezone": source.get("timezone"),
            "geocode_provider": source.get("geocode_provider"),
            "geocode_confidence": source.get("geocode_confidence"),
        }
    child_meta = dict(order_data.get("child_profile_meta") or {})
    return {
        "full_name": child_meta.get("full_name") or order_data.get("full_name") or "Child",
        "birth_date": child_meta.get("birth_date") or order_data.get("birth_date") or "",
        "birth_time": child_meta.get("birth_time") or order_data.get("birth_time") or "",
        "birth_place": child_meta.get("birth_city") or child_meta.get("birth_place") or order_data.get("birth_place") or order_data.get("birth_city") or "",
        "birth_country": child_meta.get("birth_country") or order_data.get("birth_country") or "",
        "raw_birth_place_input": child_meta.get("raw_birth_place_input") or order_data.get("raw_birth_place_input") or order_data.get("birth_place") or order_data.get("birth_city"),
        "normalized_birth_place": child_meta.get("normalized_birth_place") or order_data.get("normalized_birth_place"),
        "latitude": child_meta.get("latitude") if child_meta.get("latitude") not in (None, "") else order_data.get("latitude"),
        "longitude": child_meta.get("longitude") if child_meta.get("longitude") not in (None, "") else order_data.get("longitude"),
        "timezone": child_meta.get("timezone") or order_data.get("timezone"),
        "geocode_provider": child_meta.get("geocode_provider") or order_data.get("geocode_provider"),
        "geocode_confidence": child_meta.get("geocode_confidence") if child_meta.get("geocode_confidence") not in (None, "") else order_data.get("geocode_confidence"),
    }


def _build_single_profile_report_order_chart_payload(order_data):
    existing_payload = _report_order_chart_payload_from_existing_data(order_data)
    if existing_payload:
        return existing_payload

    birth_date = str(order_data.get("birth_date") or "").strip()
    birth_place = str(order_data.get("birth_place") or order_data.get("birth_city") or "").strip()
    if not birth_date or not birth_place:
        return {
            "interpretation_context": {
                "confidence_notes": [_report_order_signal_fallback_note()],
            }
        }

    try:
        birth_context = _build_report_order_birth_context(
            order_data,
            birth_date=birth_date,
            birth_place=birth_place,
            raw_birth_place_input=order_data.get("raw_birth_place_input"),
            normalized_birth_place=order_data.get("normalized_birth_place"),
            latitude=order_data.get("latitude"),
            longitude=order_data.get("longitude"),
            timezone=order_data.get("timezone"),
            geocode_provider=order_data.get("geocode_provider"),
            geocode_confidence=order_data.get("geocode_confidence"),
            birth_country=order_data.get("birth_country"),
        )
        bundle = _calculate_chart_bundle_from_birth_context(birth_context)
    except Exception as exc:
        logger.exception("Paid report chart bundle preparation failed order_id=%s", order_data.get("order_id"))
        confidence_notes = list(((order_data.get("interpretation_context") or {}).get("confidence_notes") or []))
        confidence_notes.append(_report_order_signal_fallback_note())
        return {
            "interpretation_context": {
                **(order_data.get("interpretation_context") or {}),
                "confidence_notes": confidence_notes,
            }
        }

    return {
        "raw_birth_place_input": birth_context.get("raw_birth_place_input"),
        "normalized_birth_place": birth_context.get("normalized_birth_place"),
        "latitude": birth_context.get("latitude"),
        "longitude": birth_context.get("longitude"),
        "timezone": birth_context.get("timezone"),
        "geocode_provider": birth_context.get("geocode_provider"),
        "geocode_confidence": birth_context.get("geocode_confidence"),
        "calculation_config": bundle.get("calculation_config") or {},
        "natal_data": bundle.get("natal_data") or {},
        "dasha_data": bundle.get("dasha_data") or [],
        "navamsa_data": bundle.get("navamsa_data") or {},
        "timing_data": bundle.get("interpretation_context") or {},
        "transit_data": bundle.get("transit_data") or [],
        "eclipse_data": bundle.get("eclipse_data") or [],
        "fullmoon_data": bundle.get("fullmoon_data") or [],
        "interpretation_context": bundle.get("interpretation_context") or {},
        "calculation_metadata": bundle.get("calculation_metadata") or {},
    }


def _build_parent_child_report_order_chart_payload(order_data):
    child_existing = {
        "natal_data": order_data.get("natal_data") or {},
        "navamsa_data": order_data.get("navamsa_data") or {},
        "dasha_data": order_data.get("dasha_data") or [],
        "transit_data": order_data.get("transit_data") or [],
        "eclipse_data": order_data.get("eclipse_data") or [],
        "fullmoon_data": order_data.get("fullmoon_data") or [],
        "interpretation_context": order_data.get("interpretation_context") or {},
        "calculation_config": order_data.get("calculation_config") or {},
        "calculation_metadata": order_data.get("calculation_metadata") or {},
    }
    child_has_chart = isinstance(child_existing["natal_data"], dict) and bool(child_existing["natal_data"].get("planets"))
    parent_signal_existing = order_data.get("parent_astro_signal_context") or {}
    child_signal_existing = order_data.get("child_astro_signal_context") or {}
    parent_natal_existing = order_data.get("parent_natal_data") or {}
    parent_has_chart = isinstance(parent_natal_existing, dict) and bool(parent_natal_existing.get("planets"))
    parent_dasha_existing = order_data.get("parent_dasha_data") or []

    if child_has_chart and parent_has_chart:
        child_signal_context = child_signal_existing or astro_signal_enrichment.build_astro_signal_context(
            child_existing["natal_data"],
            navamsa_data=child_existing["navamsa_data"],
            dasha_data=child_existing["dasha_data"],
            transit_context=child_existing["interpretation_context"] or child_existing["transit_data"] or {},
            report_type="parent_child",
        )
        parent_signal_context = parent_signal_existing or astro_signal_enrichment.build_astro_signal_context(
            parent_natal_existing,
            navamsa_data=order_data.get("parent_navamsa_data") or {},
            dasha_data=parent_dasha_existing,
            transit_context=order_data.get("parent_interpretation_context") or child_existing["interpretation_context"] or {},
            report_type="parent_child",
        )
        return {
            **child_existing,
            "timing_data": child_existing["interpretation_context"] or {},
            "parent_natal_data": parent_natal_existing,
            "parent_dasha_data": parent_dasha_existing,
            "parent_astro_signal_context": parent_signal_context,
            "child_astro_signal_context": child_signal_context,
            "astro_signal_context": {
                "parent_profile_signals": parent_signal_context,
                "child_profile_signals": child_signal_context,
                "confidence_notes": _combine_signal_confidence_notes(parent_signal_context, child_signal_context),
            },
        }
        payload["astro_signal_context"]["parent_child_interaction_signals"] = astro_signal_enrichment.build_parent_child_interaction_signals(
            {
                "language": order_data.get("user_lang") or "tr",
                **payload["astro_signal_context"],
            },
            report_type="parent_child",
        )
        return payload

    parent_profile = _report_order_profile_input(order_data, role="parent")
    child_profile = _report_order_profile_input(order_data, role="child")
    if not all([parent_profile["birth_date"], parent_profile["birth_time"], parent_profile["birth_place"], child_profile["birth_date"], child_profile["birth_time"], child_profile["birth_place"]]):
        return {
            "interpretation_context": {
                "confidence_notes": [_report_order_signal_fallback_note()],
            }
        }

    try:
        parent_birth_context = _build_report_order_birth_context(
            order_data,
            birth_date=parent_profile["birth_date"],
            birth_place=parent_profile["birth_place"],
            raw_birth_place_input=parent_profile["raw_birth_place_input"],
            normalized_birth_place=parent_profile["normalized_birth_place"],
            latitude=parent_profile["latitude"],
            longitude=parent_profile["longitude"],
            timezone=parent_profile["timezone"],
            geocode_provider=parent_profile["geocode_provider"],
            geocode_confidence=parent_profile["geocode_confidence"],
            birth_country=parent_profile["birth_country"],
        )
        child_birth_context = _build_report_order_birth_context(
            order_data,
            birth_date=child_profile["birth_date"],
            birth_place=child_profile["birth_place"],
            raw_birth_place_input=child_profile["raw_birth_place_input"],
            normalized_birth_place=child_profile["normalized_birth_place"],
            latitude=child_profile["latitude"],
            longitude=child_profile["longitude"],
            timezone=child_profile["timezone"],
            geocode_provider=child_profile["geocode_provider"],
            geocode_confidence=child_profile["geocode_confidence"],
            birth_country=child_profile["birth_country"],
        )
        parent_bundle = _calculate_chart_bundle_from_birth_context(parent_birth_context)
        child_bundle = _calculate_chart_bundle_from_birth_context(child_birth_context)
        interpretation_context = order_data.get("interpretation_context") or build_parent_child_interpretation(parent_bundle, child_bundle)
        parent_signal_context = astro_signal_enrichment.build_astro_signal_context(
            parent_bundle.get("natal_data") or {},
            navamsa_data=parent_bundle.get("navamsa_data") or {},
            dasha_data=parent_bundle.get("dasha_data") or [],
            transit_context=interpretation_context,
            report_type="parent_child",
        )
        child_signal_context = astro_signal_enrichment.build_astro_signal_context(
            child_bundle.get("natal_data") or {},
            navamsa_data=child_bundle.get("navamsa_data") or {},
            dasha_data=child_bundle.get("dasha_data") or [],
            transit_context=interpretation_context,
            report_type="parent_child",
        )
    except Exception:
        logger.exception("Paid parent-child chart bundle preparation failed order_id=%s", order_data.get("order_id"))
        return {
            "interpretation_context": {
                **(order_data.get("interpretation_context") or {}),
                "confidence_notes": [_report_order_signal_fallback_note()],
            }
        }

    return {
        "raw_birth_place_input": child_birth_context.get("raw_birth_place_input"),
        "normalized_birth_place": child_birth_context.get("normalized_birth_place"),
        "latitude": child_birth_context.get("latitude"),
        "longitude": child_birth_context.get("longitude"),
        "timezone": child_birth_context.get("timezone"),
        "geocode_provider": child_birth_context.get("geocode_provider"),
        "geocode_confidence": child_birth_context.get("geocode_confidence"),
        "calculation_config": child_bundle.get("calculation_config") or {},
        "natal_data": child_bundle.get("natal_data") or {},
        "dasha_data": child_bundle.get("dasha_data") or [],
        "navamsa_data": child_bundle.get("navamsa_data") or {},
        "timing_data": interpretation_context or {},
        "transit_data": child_bundle.get("transit_data") or [],
        "eclipse_data": child_bundle.get("eclipse_data") or [],
        "fullmoon_data": child_bundle.get("fullmoon_data") or [],
        "interpretation_context": interpretation_context or {},
        "calculation_metadata": child_bundle.get("calculation_metadata") or {},
        "parent_profile": {
            "full_name": parent_profile["full_name"],
            "birth_date": parent_profile["birth_date"],
            "birth_time": parent_profile["birth_time"],
            "birth_city": parent_birth_context.get("normalized_birth_place") or parent_profile["birth_place"],
            "birth_country": parent_profile["birth_country"],
        },
        "child_profile_meta": {
            "full_name": child_profile["full_name"],
            "birth_date": child_profile["birth_date"],
            "birth_time": child_profile["birth_time"],
            "birth_city": child_birth_context.get("normalized_birth_place") or child_profile["birth_place"],
            "birth_country": child_profile["birth_country"],
        },
        "parent_natal_data": parent_bundle.get("natal_data") or {},
        "parent_dasha_data": parent_bundle.get("dasha_data") or [],
        "parent_astro_signal_context": parent_signal_context,
        "child_astro_signal_context": child_signal_context,
        "astro_signal_context": {
            "parent_profile_signals": parent_signal_context,
            "child_profile_signals": child_signal_context,
            "confidence_notes": _combine_signal_confidence_notes(parent_signal_context, child_signal_context),
        },
    }
    payload["astro_signal_context"]["parent_child_interaction_signals"] = astro_signal_enrichment.build_parent_child_interaction_signals(
        {
            "language": order_data.get("user_lang") or "tr",
            **payload["astro_signal_context"],
        },
        report_type="parent_child",
    )
    return payload


def _build_report_order_chart_payload(order_data):
    if astro_workspace.normalize_workspace_report_type(order_data.get("report_type")) == "parent_child":
        return _build_parent_child_report_order_chart_payload(order_data)
    return _build_single_profile_report_order_chart_payload(order_data)


def _build_report_order_payload(order_data, product):
    payload = {
        "workflow": "report_order_admin_review",
        "language": "en" if str(order_data.get("user_lang") or "tr").lower() == "en" else "tr",
        "report_order_type": order_data["report_type"],
        "report_type": order_data["report_type"],
        "bundle_type": order_data.get("bundle_type") or "",
        "included_products": order_data.get("included_products") or [],
        "report_product_title": product["title"],
        "report_product_focus": product["draft_focus"],
        "customer": {
            "full_name": order_data["full_name"],
            "email": order_data["email"],
        },
        "birth_data": {
            "birth_date": order_data["birth_date"],
            "birth_time": order_data["birth_time"],
            "birth_place": order_data["birth_city"],
        },
        "customer_note": order_data.get("optional_note") or "",
        "service_model": {
            "delivery": "Raporlar, hazırlık sürecinin ardından en geç 7 gün içinde e-posta ile teslim edilir.",
            "review_step": "AI destekli taslak önce yöneticiye iletilir; müşteriye otomatik gönderilmez.",
            "human_review_required": True,
        },
        "requested_at": datetime.now(pytz.UTC).isoformat(),
    }
    payload.update(_build_report_order_chart_payload(order_data))
    return _attach_astro_signal_context(payload, report_type=order_data["report_type"])


def _generate_report_order_draft(order_payload):
    try:
        return ai_logic.generate_interpretation(order_payload), "generated"
    except (ai_logic.AIConfigurationError, ai_logic.AIServiceError):
        logger.exception("Report order AI draft generation degraded")
    except Exception:
        logger.exception("Unexpected report order AI draft generation error")
    fallback = (
        "AI destekli taslak bu ortamda otomatik üretilemedi. "
        "Müşteri bilgileri ve rapor talebi manuel inceleme için aşağıdadır."
    )
    return fallback, "draft_unavailable"


def _record_order_task_error(order, task_name, exc):
    order.last_task_error = f"{task_name}: {exc}"
    logger.exception("Service order task failed task=%s order_id=%s", task_name, getattr(order, "id", None))


def send_report_draft_to_admin(db, *, order_data, product, draft_text, draft_status):
    logger.info("Internal review email disabled; report draft is reviewed in Admin > Reports.")
    return {"status": "skipped", "reason": "admin_reports_queue"}
    admin_email = _report_order_admin_email()
    if not admin_email:
        logger.warning("Report order admin email skipped because no admin email is configured")
        return {"status": "skipped", "reason": "missing_admin_email"}

    event_key = f"internal_review:{order_data.get('order_token') or order_data['email'].strip().lower()}:{order_data['report_type']}"
    subject = f"İç inceleme hazır: {product['title']} - {order_data['full_name']}"
    attachments = []
    pdf_path = Path(order_data.get("final_pdf_path") or "")
    if order_data.get("pdf_status") in {"completed", "ready"} and pdf_path.exists() and pdf_path.is_file():
        attachments.append({"path": str(pdf_path), "filename": f"internal_review_report_{order_data.get('order_id') or 'draft'}.pdf"})
    return safe_send_template_email(
        db,
        user=None,
        to_email=admin_email,
        email_type="report_order_admin_draft",
        template_name="report_order_admin_draft",
        subject=subject,
        event_type="report_order",
        event_key=event_key,
        attachments=attachments or None,
        order=order_data,
        product=product,
        draft_text=_safe_truncate_text(draft_text, 6000),
        draft_status=draft_status,
    )


def _order_data_from_service_order(order):
    payload = _service_order_payload(order)
    email_config = email_utils.get_email_config()
    app_base_url = str(email_config.get("app_base_url") or "").rstrip("/")
    order_data = {
        "order_id": order.id,
        "order_token": order.order_token or order.public_token or "",
        "full_name": order.customer_name or payload.get("full_name") or "",
        "email": order.customer_email or payload.get("email") or "",
        "birth_date": order.birth_date or payload.get("birth_date") or "",
        "birth_time": order.birth_time or payload.get("birth_time") or "",
        "birth_city": payload.get("birth_city") or payload.get("city") or order.birth_place or "",
        "birth_district": payload.get("birth_district") or payload.get("district") or "",
        "birth_place": order.birth_place or payload.get("birth_place") or payload.get("birth_city") or "",
        "optional_note": order.optional_note or payload.get("optional_note") or "",
        "report_type": order.product_type,
        "report_title": payload.get("report_title") or _service_order_product(order).get("title", ""),
        "submitted_at": payload.get("submitted_at") or (order.created_at.isoformat() if order.created_at else datetime.now(pytz.UTC).isoformat()),
        "source": payload.get("source") or "paid_report_order",
        "admin_detail_url": f"{app_base_url}/admin/reports/{order.id}" if app_base_url else f"/admin/reports/{order.id}",
        "pdf_status": order.pdf_status or "",
        "final_pdf_path": order.final_pdf_path or "",
        "user_lang": payload.get("user_lang") or getattr(order, "user_lang", None) or "tr",
        "raw_birth_place_input": payload.get("raw_birth_place_input") or payload.get("birth_place") or payload.get("birth_city"),
        "normalized_birth_place": payload.get("normalized_birth_place"),
        "latitude": payload.get("latitude") if payload.get("latitude") not in (None, "") else payload.get("lat"),
        "longitude": payload.get("longitude") if payload.get("longitude") not in (None, "") else payload.get("lon"),
        "timezone": payload.get("timezone"),
        "geocode_provider": payload.get("geocode_provider"),
        "geocode_confidence": payload.get("geocode_confidence"),
    }
    for key in (
        "natal_data",
        "navamsa_data",
        "dasha_data",
        "timing_data",
        "transit_data",
        "eclipse_data",
        "fullmoon_data",
        "interpretation_context",
        "calculation_config",
        "calculation_metadata",
        "astro_signal_context",
        "parent_profile",
        "child_profile_meta",
        "parent_natal_data",
        "parent_dasha_data",
        "parent_navamsa_data",
        "parent_interpretation_context",
        "parent_astro_signal_context",
        "child_astro_signal_context",
    ):
        if key in payload:
            order_data[key] = payload.get(key)
    return order_data


def generate_ai_draft_for_order(db, order):
    if order.service_type != "report":
        raise ValueError("AI draft generation is only valid for report orders.")
    if getattr(order, "ai_draft_text", None):
        order.ai_draft_status = order.ai_draft_status or order.draft_status or "generated"
        db.commit()
        return {"status": "skipped", "reason": "already_exists"}
    product = _service_order_product(order)
    order.status = "draft_pending" if order.status == "paid" else order.status
    order.ai_draft_status = "started"
    db.commit()
    order_data = _order_data_from_service_order(order)
    order_payload = _build_report_order_payload(order_data, product)
    try:
        draft_text, draft_status = _generate_report_order_draft(order_payload)
    except Exception as exc:
        order.ai_draft_status = "failed"
        _record_order_task_error(order, "generate_ai_draft_for_order", exc)
        db.commit()
        raise
    order.ai_draft_text = draft_text
    order.ai_draft_created_at = datetime.utcnow()
    order.ai_draft_version = (getattr(order, "ai_draft_version", None) or 0) + 1
    order.draft_status = draft_status
    order.ai_draft_status = draft_status
    order.status = "draft_ready" if draft_status in {"generated", "draft_unavailable"} else order.status
    order.last_task_error = None
    db.commit()
    return {"status": draft_status, "order_id": order.id}


def send_admin_notification_for_order(db, order):
    if order.service_type != "report":
        raise ValueError("Admin draft notification is only valid for report orders.")
    logger.info("Admin draft email disabled order_id=%s; use /admin/reports for review.", order.id)
    return {"status": "skipped", "reason": "admin_reports_queue"}
    if getattr(order, "draft_sent_at", None):
        return {"status": "skipped", "reason": "already_sent"}
    if not getattr(order, "ai_draft_text", None):
        generate_ai_draft_for_order(db, order)
        db.refresh(order)
    product = _service_order_product(order)
    order_data = _order_data_from_service_order(order)
    result = send_report_draft_to_admin(
        db,
        order_data=order_data,
        product=product,
        draft_text=order.ai_draft_text or "",
        draft_status=order.draft_status or order.ai_draft_status or "generated",
    )
    if result.get("status") == "sent":
        order.draft_sent_at = datetime.utcnow()
    if result.get("status") in {"sent", "skipped"} and order.status in {"paid", "draft_pending"}:
        order.status = "draft_ready"
    db.commit()
    return result


def finalize_report_order_after_payment(db, order, payment_data=None):
    payment_data = payment_data or {}
    if getattr(order, "ai_draft_text", None) and getattr(order, "draft_sent_at", None):
        return {"status": "skipped", "reason": "already_sent"}
    order.payment_provider = payment_data.get("provider") or order.payment_provider
    order.payment_session_id = payment_data.get("session_id") or order.payment_session_id
    order.payment_reference = payment_data.get("payment_reference") or order.payment_reference
    db.commit()
    draft_result = generate_ai_draft_for_order(db, order)
    return {"status": "completed", "draft": draft_result}


def send_report_customer_confirmation(db, order):
    if getattr(order, "customer_confirmation_sent_at", None):
        return {"status": "skipped", "reason": "already_sent"}
    order_data = _order_data_from_service_order(order)
    if not order_data.get("email"):
        return {"status": "skipped", "reason": "missing_customer_email"}
    result = safe_send_template_email(
        db,
        user=None,
        to_email=order_data["email"],
        email_type="report_order_customer_confirmation",
        template_name="report_order_customer_confirmation",
        subject=f"Rapor talebiniz alındı: {_service_order_product(order).get('title', '')}",
        event_type="report_order_paid",
        event_key=f"report_customer_confirmation:{order.order_token}",
        order=order_data,
        product=_service_order_product(order),
    )
    if result.get("status") == "sent":
        order.customer_confirmation_sent_at = datetime.utcnow()
        db.commit()
    return result


def send_customer_confirmation_for_order(db, order):
    if order.service_type != "report":
        raise ValueError("Customer confirmation is only valid for report orders.")
    return send_report_customer_confirmation(db, order)


def _service_order_pdf_context(order):
    order_data = _order_data_from_service_order(order)
    product = _service_order_product(order)
    title = product.get("title") or order_data.get("report_title") or "Vedik Astroloji Raporu"
    text = html_lib.escape(order.ai_draft_text or "Rapor taslağı henüz hazır değil.").replace("\n", "<br>")
    return {
        "title": title,
        "client_name": order_data.get("full_name") or order.customer_name or "Danışan",
        "generated_at": datetime.utcnow().strftime("%Y-%m-%d"),
        "body_html": text,
    }


def generate_pdf_for_order(db, order):
    if order.service_type != "report":
        raise ValueError("PDF generation is only valid for report orders.")
    pdf_path = Path(getattr(order, "final_pdf_path", "") or "")
    if order.pdf_status in {"completed", "ready"} and pdf_path.exists():
        return {"status": "skipped", "reason": "already_ready", "path": str(pdf_path)}
    if not getattr(order, "ai_draft_text", None):
        generate_ai_draft_for_order(db, order)
        db.refresh(order)
    order.pdf_status = "processing"
    db.commit()
    context = _service_order_pdf_context(order)
    html_content = (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<style>body{font-family:serif;color:#18263f;line-height:1.6;padding:48px}"
        "h1{font-size:28px;margin-bottom:8px}.meta{color:#6f6042;margin-bottom:28px}"
        ".report{font-size:15px}</style></head><body>"
        f"<h1>{html_lib.escape(context['title'])}</h1>"
        f"<div class='meta'>{html_lib.escape(context['client_name'])} · {context['generated_at']}</div>"
        f"<div class='report'>{context['body_html']}</div></body></html>"
    )
    try:
        _ensure_pdf_runtime_environment()
        from weasyprint import HTML
        pdf_bytes = HTML(string=html_content, base_url=str(BASE_DIR)).write_pdf()
        if not _validate_pdf_bytes(pdf_bytes):
            raise ai_logic.AIServiceError("Generated service order PDF is invalid.")
        output_dir = BASE_DIR / "data" / "service_order_pdfs"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"service_order_{order.id}.pdf"
        output_path.write_bytes(pdf_bytes)
    except Exception as exc:
        order.pdf_status = "failed"
        _record_order_task_error(order, "generate_pdf_for_order", exc)
        db.commit()
        raise
    order.pdf_status = "completed"
    order.final_pdf_path = str(output_path)
    order.last_task_error = None
    db.commit()
    return {"status": "ready", "path": str(output_path)}


def send_consultation_confirmation(db, order):
    if not getattr(order, "customer_email", None):
        return {"status": "skipped", "reason": "missing_customer_email"}
    return safe_send_template_email(
        db,
        user=None,
        to_email=order.customer_email,
        email_type="consultation_payment_confirmation",
        template_name="consultation_payment_confirmation",
        subject="Danışmanlık ödemeniz alındı",
        event_type="consultation_paid",
        event_key=f"consultation_confirmation:{order.order_token}",
        order=order,
        product=_service_order_product(order),
    )


def _iyzico_payload_value(payload, key):
    if isinstance(payload, dict):
        return payload.get(key)
    return None


def _payment_env_flag(name, default=False):
    raw = str(os.getenv(name, "true" if default else "false")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _validate_iyzico_retrieve_payload(order, retrieve_payload):
    payments.IyzicoProvider.verify_response_signature_payload("checkout_retrieve", retrieve_payload)
    status = str(_iyzico_payload_value(retrieve_payload, "status") or "").lower()
    payment_status = str(_iyzico_payload_value(retrieve_payload, "paymentStatus") or "").lower()
    conversation_id = str(_iyzico_payload_value(retrieve_payload, "conversationId") or "")
    basket_id = str(_iyzico_payload_value(retrieve_payload, "basketId") or "")
    paid_price = _amount_decimal(_iyzico_payload_value(retrieve_payload, "paidPrice"))
    currency = str(_iyzico_payload_value(retrieve_payload, "currency") or "").upper()
    fraud_status = str(_iyzico_payload_value(retrieve_payload, "fraudStatus") or "")
    expected_amount = _order_amount(order)
    expected_currency = str(getattr(order, "currency", None) or "TRY").upper()
    errors = []
    if status != "success":
        errors.append("status")
    if payment_status != "success":
        errors.append("paymentStatus")
    if conversation_id != str(getattr(order, "provider_conversation_id", None) or _order_public_token(order)):
        errors.append("conversationId")
    if basket_id != str(order.id):
        errors.append("basketId")
    if paid_price != expected_amount:
        errors.append("paidPrice")
    if currency != expected_currency:
        errors.append("currency")
    if fraud_status != "1":
        errors.append("fraudStatus")
    payment_id = str(_iyzico_payload_value(retrieve_payload, "paymentId") or "")
    if not payment_id:
        errors.append("paymentId")
    transactions = retrieve_payload.get("itemTransactions") if isinstance(retrieve_payload, dict) else []
    transaction_id = ""
    if isinstance(transactions, list) and transactions:
        transaction_id = str((transactions[0] or {}).get("paymentTransactionId") or "")
    if errors:
        raise payments.PaymentVerificationError("Iyzico verification failed: " + ", ".join(errors))
    return {
        "provider": "iyzico",
        "session_id": getattr(order, "provider_token", None),
        "payment_reference": payment_id,
        "payment_id": payment_id,
        "payment_transaction_id": transaction_id,
        "payment_status": payment_status,
        "fraud_status": fraud_status,
        "completed_at": datetime.utcnow(),
        "raw": retrieve_payload,
    }


def finalize_paid_order_if_valid(db, order, retrieve_payload):
    payment_data = _validate_iyzico_retrieve_payload(order, retrieve_payload)
    payment_id = payment_data["payment_id"]
    paid_or_later_statuses = {
        "paid",
        "draft_pending",
        "draft_sent_to_admin",
        "draft_ready",
        "under_review",
        "ready_to_send",
        "delivered",
        "confirmed",
        "prepared",
        "completed",
        "cancelled",
        "no_show",
        "refunded",
        "partially_refunded",
    }
    if getattr(order, "provider_payment_id", None) == payment_id and getattr(order, "status", None) in paid_or_later_statuses:
        return {"changed": False, "payment_data": payment_data}
    existing = db.query(db_mod.ServiceOrder).filter(
        db_mod.ServiceOrder.provider_payment_id == payment_id,
        db_mod.ServiceOrder.id != order.id,
    ).first()
    if existing:
        raise payments.PaymentVerificationError("Iyzico paymentId is already linked to another order.")
    now = datetime.utcnow()
    order.status = "paid"
    order.provider_name = "iyzico"
    order.payment_provider = "iyzico"
    order.provider_payment_id = payment_id
    order.provider_transaction_id = payment_data.get("payment_transaction_id") or order.provider_transaction_id
    order.payment_reference = payment_id
    order.payment_verified_at = now
    order.paid_at = now
    order.fraud_status = payment_data["fraud_status"]
    log_admin_action(
        db,
        order,
        "payment_verified",
        actor="iyzico",
        metadata={
            "payment_id": payment_id,
            "payment_transaction_id": payment_data.get("payment_transaction_id"),
            "provider_token": getattr(order, "provider_token", None),
            "conversation_id": getattr(order, "provider_conversation_id", None),
        },
    )
    db.commit()
    return {"changed": True, "payment_data": payment_data}


def mark_payment_under_review(db, order, retrieve_payload, actor="iyzico"):
    fraud_status = str(_iyzico_payload_value(retrieve_payload, "fraudStatus") or "")
    order.status = "payment_under_review"
    order.fraud_status = fraud_status or order.fraud_status
    order.payment_verified_at = datetime.utcnow()
    log_admin_action(db, order, "payment_under_review", actor=actor, metadata={"fraud_status": fraud_status})
    db.commit()
    return {"status": "payment_under_review", "fraud_status": fraud_status}


def run_post_payment_triggers(db, order, payment_data):
    if order.service_type == "report":
        order.payment_provider = payment_data.get("provider") or order.payment_provider
        order.payment_session_id = payment_data.get("session_id") or order.payment_session_id
        order.payment_reference = payment_data.get("payment_reference") or order.payment_reference
        order.status = "draft_pending" if order.status == "paid" else order.status
        db.commit()
        return {"status": "queued", "tasks": enqueue_report_post_payment_tasks(order)}
    if order.service_type == "consultation":
        confirmation = send_consultation_confirmation(db, order)
        return {"consultation_confirmation": confirmation}
    return {"status": "skipped", "reason": "unknown_service_type"}


def process_verified_service_payment(db, order, retrieve_payload):
    result = finalize_paid_order_if_valid(db, order, retrieve_payload)
    if result["changed"]:
        result["post_payment"] = run_post_payment_triggers(db, order, result["payment_data"])
    return result


def expire_unpaid_consultations(db=None):
    owns_session = db is None
    db = db or db_mod.SessionLocal()
    try:
        cutoff = datetime.utcnow() - timedelta(minutes=30)
        orders = db.query(db_mod.ServiceOrder).filter(
            db_mod.ServiceOrder.service_type == "consultation",
            db_mod.ServiceOrder.status == "booking_pending_payment",
            db_mod.ServiceOrder.created_at < cutoff,
        ).all()
        for order in orders:
            order.status = "booking_expired"
        db.commit()
        return len(orders)
    finally:
        if owns_session:
            db.close()


REPORT_ADMIN_TRANSITIONS = {
    "mark_draft_ready": {"from": {"paid", "draft_pending", "draft_ready"}, "to": "draft_ready", "timestamp": "ai_draft_created_at"},
    "mark_under_review": {"from": {"draft_ready", "under_review"}, "to": "under_review", "timestamp": "review_started_at"},
    "mark_ready_to_send": {"from": {"under_review", "ready_to_send"}, "to": "ready_to_send", "timestamp": "ready_to_send_at"},
    "mark_delivered": {"from": {"ready_to_send", "delivered"}, "to": "delivered", "timestamp": "delivered_at"},
}
CONSULTATION_ADMIN_TRANSITIONS = {
    "mark_confirmed": {"from": {"paid", "confirmed"}, "to": "confirmed", "timestamp": "confirmed_at"},
    "mark_prepared": {"from": {"confirmed", "prepared"}, "to": "prepared", "timestamp": "prepared_at"},
    "mark_completed": {"from": {"prepared", "completed"}, "to": "completed", "timestamp": "completed_at"},
}
TERMINAL_ADMIN_STATUSES = {"refunded", "cancelled", "no_show"}


def _admin_actor(request):
    user = getattr(request.state, "admin_user", None) or getattr(request.state, "current_user", None) or {}
    if isinstance(user, dict):
        return user.get("email") or user.get("name") or "admin"
    return getattr(user, "email", None) or "admin"


def log_admin_action(db, order, action, actor=None, metadata=None):
    log = db_mod.AdminActionLog(
        order_id=order.id,
        action=action,
        actor=actor or "admin",
        metadata_json=json.dumps(metadata or {}, ensure_ascii=False, default=str),
    )
    db.add(log)
    return log


def log_action(db, user_email, action, target_type, target_id, detail=""):
    entry = db_mod.ActivityLog(
        user_email=user_email,
        action=action,
        target_type=target_type,
        target_id=str(target_id) if target_id is not None else None,
        detail=detail or "",
    )
    db.add(entry)
    return entry


def _validate_order_paid(order):
    if getattr(order, "status", "") in {"awaiting_payment", "booking_pending_payment", "booking_expired", "initiated"} or not getattr(order, "paid_at", None):
        raise ValueError("Order must be paid before this admin action.")


def apply_admin_order_transition(db, order, action, actor=None):
    transitions = REPORT_ADMIN_TRANSITIONS if order.service_type == "report" else CONSULTATION_ADMIN_TRANSITIONS
    rule = transitions.get(action)
    if not rule:
        raise ValueError("Invalid admin action.")
    _validate_order_paid(order)
    if order.status not in rule["from"]:
        raise ValueError(f"Invalid state transition from {order.status}.")
    now = datetime.utcnow()
    order.status = rule["to"]
    timestamp_field = rule.get("timestamp")
    if timestamp_field and not getattr(order, timestamp_field, None):
        setattr(order, timestamp_field, now)
    log_admin_action(db, order, action, actor=actor, metadata={"to": rule["to"]})
    db.commit()
    return order


def _refund_amount_decimal(order, refund_amount=None):
    amount = _amount_decimal(refund_amount) if refund_amount not in {None, ""} else _order_amount(order)
    if amount <= Decimal("0"):
        raise ValueError("Refund amount must be greater than zero.")
    order_amount = _order_amount(order)
    if order_amount and amount > order_amount:
        raise ValueError("Refund amount cannot exceed order amount.")
    return amount


def refund_service_order_payment(order, refund_amount, reason):
    provider = payments.get_payment_provider()
    if getattr(provider, "provider_name", "") != "iyzico" or not hasattr(provider, "refund_order_payment"):
        raise payments.PaymentConfigurationError("Provider refund API is not configured.")
    return provider.refund_order_payment(order, refund_amount, reason=reason)


def send_refund_confirmation_email(db, order, refund_amount):
    if not getattr(order, "customer_email", None):
        return {"status": "skipped", "reason": "missing_customer_email"}
    return safe_send_template_email(
        db,
        user=None,
        to_email=order.customer_email,
        email_type="refund_confirmation",
        template_name="refund_confirmation",
        subject="İade işleminiz kaydedildi",
        event_type="order_refund",
        event_key=f"refund_confirmation:{order.order_token}:{order.refunded_at.isoformat() if order.refunded_at else ''}",
        order=order,
        product=_service_order_product(order),
        refund_amount=str(refund_amount),
    )


def send_cancellation_confirmation_email(db, order):
    if not getattr(order, "customer_email", None):
        return {"status": "skipped", "reason": "missing_customer_email"}
    return safe_send_template_email(
        db,
        user=None,
        to_email=order.customer_email,
        email_type="cancellation_confirmation",
        template_name="order_cancellation_confirmation",
        subject="Talebiniz iptal edildi",
        event_type="order_cancelled",
        event_key=f"order_cancelled:{order.order_token}:{order.cancelled_at.isoformat() if order.cancelled_at else ''}",
        order=order,
        product=_service_order_product(order),
    )


def request_order_refund(db, order, refund_amount=None, reason="", actor=None, refund_mode="provider"):
    _validate_order_paid(order)
    if order.status in {"refunded"} or getattr(order, "refund_status", None) == "refunded":
        raise ValueError("Order has already been fully refunded.")
    amount = _refund_amount_decimal(order, refund_amount)
    mode = str(refund_mode or "provider").strip().lower()
    log_admin_action(db, order, "refund_requested", actor=actor, metadata={"amount": str(amount), "mode": mode, "reason": reason})
    if mode == "manual":
        order.refund_amount = amount
        order.refund_reason = str(reason or "").strip()
        order.refund_status = "manual_review_needed"
        order.refund_provider_status = "manual_review_needed"
        log_admin_action(db, order, "refund_manual_review_needed", actor=actor, metadata={"amount": str(amount), "reason": reason})
        db.commit()
        return {"status": "manual_review_needed", "refund_amount": str(amount)}
    provider_result = refund_service_order_payment(order, amount, reason)
    now = datetime.utcnow()
    order.refund_amount = amount
    order.refund_reason = str(reason or "").strip()
    order.refunded_at = now
    full_refund = amount == _order_amount(order)
    order.refund_status = "refunded" if full_refund else "partially_refunded"
    order.status = order.refund_status
    order.provider_refund_id = provider_result.get("refund_reference") or order.provider_refund_id
    order.refund_provider_status = provider_result.get("provider_status") or provider_result.get("status") or order.refund_provider_status
    log_admin_action(db, order, "refund", actor=actor, metadata={"amount": str(amount), "mode": mode, "provider_result": provider_result})
    db.commit()
    send_refund_confirmation_email(db, order, amount)
    return provider_result


def _consultation_can_cancel_free(order, now=None):
    scheduled_start = getattr(order, "scheduled_start", None)
    if not scheduled_start:
        return False
    now = now or datetime.utcnow()
    return scheduled_start - now >= timedelta(hours=24)


def cancel_service_order(db, order, reason="", actor=None, admin_override=False):
    _validate_order_paid(order)
    if order.status in {"cancelled", "refunded", "no_show"}:
        raise ValueError("Order is already in a terminal state.")
    free_window = True
    if order.service_type == "consultation":
        free_window = _consultation_can_cancel_free(order)
        if not free_window and not admin_override:
            raise ValueError("Consultation cancellation is inside the 24-hour window. Admin override is required.")
    order.status = "cancelled"
    order.cancelled_at = datetime.utcnow()
    order.cancellation_reason = str(reason or "").strip()
    log_admin_action(db, order, "cancel", actor=actor, metadata={"admin_override": bool(admin_override), "free_window": bool(free_window)})
    db.commit()
    send_cancellation_confirmation_email(db, order)
    return {"status": "cancelled", "free_window": free_window}


def mark_consultation_no_show(db, order, reason="", actor=None):
    if order.service_type != "consultation":
        raise ValueError("Only consultation orders can be marked no_show.")
    _validate_order_paid(order)
    if order.status in {"refunded", "cancelled", "no_show"}:
        raise ValueError("Consultation is already in a terminal state.")
    order.status = "no_show"
    order.no_show_at = datetime.utcnow()
    if reason:
        order.internal_notes = ((order.internal_notes or "").rstrip() + f"\nNo-show note: {reason}").strip()
    log_admin_action(db, order, "mark_no_show", actor=actor, metadata={"reason": reason})
    db.commit()
    return order


def retrieve_iyzico_payment_detail(order, payment_id="", conversation_id=""):
    payment_id = str(payment_id or getattr(order, "provider_payment_id", "") or "").strip()
    if not payment_id:
        raise ValueError("paymentId is required for iyzico payment detail reconciliation.")
    provider = payments.get_payment_provider()
    if getattr(provider, "provider_name", "") != "iyzico" or not hasattr(provider, "retrieve_payment_detail"):
        raise payments.PaymentConfigurationError("Iyzico payment detail API is not configured.")
    return provider.retrieve_payment_detail(payment_id, conversation_id or getattr(order, "provider_conversation_id", None) or _order_public_token(order))


def reconcile_order_payment(db, order, token="", conversation_id="", payment_id="", actor=None):
    token = str(token or getattr(order, "provider_token", "") or "").strip()
    payment_id = str(payment_id or "").strip()
    if not token and not payment_id:
        raise ValueError("Payment token or paymentId is required for iyzico reconciliation.")
    provider = payments.get_payment_provider()
    conversation_id = str(conversation_id or getattr(order, "provider_conversation_id", None) or _order_public_token(order))
    if token:
        if getattr(provider, "provider_name", "") != "iyzico" or not hasattr(provider, "retrieve_checkout_form"):
            raise payments.PaymentConfigurationError("Iyzico retrieve API is not configured.")
        retrieve_payload = provider.retrieve_checkout_form(token, conversation_id)
    else:
        retrieve_payload = retrieve_iyzico_payment_detail(order, payment_id=payment_id, conversation_id=conversation_id)
    try:
        result = process_verified_service_payment(db, order, retrieve_payload)
    except payments.PaymentVerificationError as exc:
        if "fraudStatus" in str(exc):
            result = mark_payment_under_review(db, order, retrieve_payload, actor=actor or "admin_reconcile")
        else:
            raise
    order.reconciliation_notes = f"Manual reconciliation by {actor or 'admin'} at {datetime.utcnow().isoformat()}"
    log_admin_action(db, order, "reconcile_payment", actor=actor, metadata={"token": token, "payment_id": payment_id, "conversation_id": conversation_id, "result": result})
    db.commit()
    return result


def save_order_internal_notes(db, order, notes, actor=None):
    order.internal_notes = str(notes or "").strip()
    log_admin_action(db, order, "save_internal_notes", actor=actor)
    db.commit()
    return order


def send_final_report_delivery_email(db, order, actor=None):
    if order.service_type != "report":
        raise ValueError("Only report orders can be delivered by report email.")
    _validate_order_paid(order)
    if order.status == "delivered":
        raise ValueError("Report has already been delivered.")
    if order.status != "ready_to_send":
        raise ValueError("Report must be ready_to_send before delivery.")
    pdf_path = Path(getattr(order, "final_pdf_path", "") or "")
    if order.pdf_status not in {"completed", "ready"} or not pdf_path.exists() or not pdf_path.is_file():
        order.last_task_error = "Final report delivery blocked: PDF is not ready."
        db.commit()
        raise ValueError("Final PDF must be generated before delivery.")
    order_data = _order_data_from_service_order(order)
    if not order_data.get("email"):
        raise ValueError("Customer email is missing.")
    result = safe_send_template_email(
        db,
        user=None,
        to_email=order_data["email"],
        email_type="final_report_delivery",
        template_name="final_report_delivery",
        subject=f"Raporunuz hazır: {_service_order_product(order).get('title', '')}",
        event_type="report_delivered",
        event_key=f"final_report_delivery:{order.order_token}",
        attachments=[{"path": str(pdf_path), "filename": f"vedic_report_{order.id}.pdf"}],
        order=order_data,
        product=_service_order_product(order),
        final_report_text=order.ai_draft_text or "",
        final_pdf_path=str(pdf_path),
    )
    if result.get("status") != "sent":
        raise ValueError("Delivery email could not be sent.")
    now = datetime.utcnow()
    order.status = "delivered"
    order.delivered_at = now
    log_admin_action(db, order, "send_final_report", actor=actor, metadata={"email_log_id": result.get("email_log_id")})
    db.commit()
    return result


def deliver_final_report_for_order(db, order, actor=None):
    if getattr(order, "delivered_at", None) or order.status == "delivered":
        return {"status": "skipped", "reason": "already_delivered"}
    pdf_path = Path(getattr(order, "final_pdf_path", "") or "")
    if order.pdf_status not in {"completed", "ready"} or not pdf_path.exists() or not pdf_path.is_file():
        order.last_task_error = "Delivery skipped because final PDF is not ready."
        db.commit()
        return {"status": "skipped", "reason": "pdf_not_ready"}
    return send_final_report_delivery_email(db, order, actor=actor or "celery")


def enqueue_report_post_payment_tasks(order):
    from report_tasks import generate_ai_draft_task
    from email_tasks import send_customer_confirmation_email_task

    return {
        "ai_draft": generate_ai_draft_task.delay(order.id).id,
        "customer_confirmation": send_customer_confirmation_email_task.delay(order.id).id,
    }


def enqueue_final_report_delivery_tasks(order):
    if order.service_type != "report":
        raise ValueError("Only report orders can be delivered by report email.")
    _validate_order_paid(order)
    if order.status == "delivered":
        raise ValueError("Report has already been delivered.")
    if order.status != "ready_to_send":
        raise ValueError("Report must be ready_to_send before delivery.")
    from report_tasks import generate_pdf_task

    pdf_result = generate_pdf_task.apply_async(args=(order.id,), kwargs={"deliver_after": True})
    return {"pdf": pdf_result.id, "delivery": "queued_after_pdf_success"}


def maybe_send_welcome_email(db, user):
    logger.info("Welcome email trigger evaluated user_id=%s", user.id)
    html = render_email(
        "welcome.html",
        name=getattr(user, "name", None) or getattr(user, "email", ""),
        calculator_url="https://focusastrology.com/calculator",
        reports_url="https://focusastrology.com/reports",
        astrologer_name=get_astrologer_name(db),
    )
    ok = _send_logged_email(
        db,
        email_type="welcome",
        to_email=user.email,
        subject="Focus Astrology'e hoş geldiniz",
        html_body=html,
        event_type="signup",
        event_key=f"welcome:{user.id}",
        user_id=user.id,
    )
    return {"status": "sent" if ok else "failed", "ok": ok}


def maybe_send_report_order_confirmation_email(db, order):
    product = _service_order_product(order)
    report_type_label = product.get("title") or getattr(order, "product_type", "Rapor")
    html = render_email(
        "order_confirmation.html",
        name=order.customer_name or order.customer_email,
        report_type=report_type_label,
        delivery_days=get_setting(db, "site_report_delivery_days", "7"),
        to_email=order.customer_email,
        astrologer_name=get_astrologer_name(db),
    )
    ok = _send_logged_email(
        db,
        email_type="report_order_confirmation",
        to_email=order.customer_email,
        subject="Rapor talebiniz alÃ„Â±ndÃ„Â± ââ‚¬â€ Focus Astrology",
        html_body=html,
        event_type="report_order",
        event_key=f"report_order_confirmation:{order.id}",
    )
    return {"status": "sent" if ok else "failed", "ok": ok}


def maybe_send_plan_activation_email(db, user, previous_plan, current_plan, event_key=None):
    if normalize_plan_code(previous_plan) == normalize_plan_code(current_plan):
        logger.info("Plan activation email skipped because plan is unchanged user_id=%s", user.id)
        return {"status": "skipped", "reason": "unchanged_plan"}
    return safe_send_template_email(
        db,
        user=user,
        email_type="plan_upgraded",
        template_name="plan_upgraded_email",
        subject=f"Your {get_plan_features(current_plan).get('label', current_plan.title())} plan is active",
        event_type="subscription_activated",
        event_key=event_key or f"plan:{user.id}:{current_plan}:{datetime.utcnow().date().isoformat()}",
        previous_plan_label=get_plan_features(previous_plan).get("label", normalize_plan_code(previous_plan).title()),
        plan_label=get_plan_features(current_plan).get("label", normalize_plan_code(current_plan).title()),
        feature_summary=", ".join(get_plan_features(current_plan).get("allowed_report_types", [])),
    )


def maybe_send_payment_success_email(db, user, plan_code, invoice_id=None, event_key=None):
    return safe_send_template_email(
        db,
        user=user,
        email_type="payment_success",
        template_name="payment_success_email",
        subject="Payment received successfully",
        event_type="invoice.paid",
        event_key=event_key or f"invoice_paid:{invoice_id or user.id}",
        plan_label=get_plan_features(plan_code).get("label", normalize_plan_code(plan_code).title()),
        invoice_id=invoice_id or "-",
    )


def maybe_send_payment_failed_email(db, user, plan_code, invoice_id=None, recovery_url=None, event_key=None):
    return safe_send_template_email(
        db,
        user=user,
        email_type="payment_failed",
        template_name="payment_failed_email",
        subject="Payment issue on your Jyotish account",
        event_type="invoice.payment_failed",
        event_key=event_key or f"invoice_failed:{invoice_id or user.id}",
        plan_label=get_plan_features(plan_code).get("label", normalize_plan_code(plan_code).title()),
        invoice_id=invoice_id or "-",
        recovery_url=recovery_url or _email_base_context(user)["account_url"],
    )


def maybe_send_cancellation_email(db, user, previous_plan, current_plan="free", event_key=None):
    return safe_send_template_email(
        db,
        user=user,
        email_type="cancellation",
        template_name="cancellation_email",
        subject="Your Jyotish plan has changed",
        event_type="subscription.cancelled",
        event_key=event_key or f"cancel:{user.id}:{previous_plan}:{current_plan}:{datetime.utcnow().date().isoformat()}",
        previous_plan_label=get_plan_features(previous_plan).get("label", normalize_plan_code(previous_plan).title()),
        current_plan_label=get_plan_features(current_plan).get("label", normalize_plan_code(current_plan).title()),
    )


def maybe_send_recovery_email(db, user, plan_code, invoice_id=None, recovery_url=None, event_key=None):
    return safe_send_template_email(
        db,
        user=user,
        email_type="payment_recovery",
        template_name="payment_failed_email",
        subject="Action needed to keep your plan active",
        event_type="billing.recovery",
        event_key=event_key or f"billing_recovery:{invoice_id or user.id}",
        plan_label=get_plan_features(plan_code).get("label", normalize_plan_code(plan_code).title()),
        invoice_id=invoice_id or "-",
        recovery_url=recovery_url or _email_base_context(user)["account_url"],
    )


def process_billing_notification_event(db, event_payload):
    event_type = str(event_payload.get("event_type") or "").strip()
    event_key = str(event_payload.get("event_id") or event_payload.get("invoice_id") or event_payload.get("subscription_id") or "").strip() or None
    user = None
    if event_payload.get("user_id"):
        user = db.query(db_mod.AppUser).filter(db_mod.AppUser.id == int(event_payload["user_id"])).first()
    elif event_payload.get("email"):
        user = db.query(db_mod.AppUser).filter(db_mod.AppUser.email == str(event_payload["email"]).strip().lower()).first()

    if not user:
        logger.warning("Billing notification skipped because user was not found event_type=%s", event_type)
        return {"status": "skipped", "reason": "user_not_found"}

    next_plan = normalize_plan_code(event_payload.get("plan_code") or user.plan_code)
    previous_plan = normalize_plan_code(event_payload.get("previous_plan") or user.plan_code)
    invoice_id = event_payload.get("invoice_id")
    recovery_url = event_payload.get("recovery_url")

    if event_type in {"checkout.session.completed", "subscription_activated", "customer.subscription.updated"}:
        user.plan_code = next_plan
        user.subscription_status = str(event_payload.get("subscription_status") or "active")
        user.plan_started_at = datetime.utcnow()
        db.commit()
        return maybe_send_plan_activation_email(db, user, previous_plan, next_plan, event_key=event_key)
    if event_type in {"invoice.paid", "payment_succeeded"}:
        user.plan_code = next_plan
        user.subscription_status = "active"
        db.commit()
        return maybe_send_payment_success_email(db, user, next_plan, invoice_id=invoice_id, event_key=event_key)
    if event_type in {"invoice.payment_failed", "payment_failed"}:
        user.subscription_status = str(event_payload.get("subscription_status") or "past_due")
        db.commit()
        maybe_send_payment_failed_email(db, user, next_plan, invoice_id=invoice_id, recovery_url=recovery_url, event_key=event_key)
        return maybe_send_recovery_email(db, user, next_plan, invoice_id=invoice_id, recovery_url=recovery_url, event_key=f"recovery:{event_key or invoice_id or user.id}")
    if event_type in {"customer.subscription.deleted", "subscription_cancelled", "downgraded"}:
        user.plan_code = next_plan
        user.subscription_status = str(event_payload.get("subscription_status") or "canceled")
        db.commit()
        return maybe_send_cancellation_email(db, user, previous_plan, next_plan, event_key=event_key)

    logger.info("Billing notification event ignored event_type=%s", event_type)
    return {"status": "skipped", "reason": "unsupported_event"}


@app.middleware("http")
async def maintenance_mode_middleware(request: Request, call_next):
    return await call_next(request)


@app.middleware("http")
async def load_user_context(request: Request, call_next):
    db = db_mod.SessionLocal()
    try:
        user = get_request_user(request, db)
        request.state.current_user = _public_user_view(user)
        request.state.current_user_id = _user_id(user) if user else None
        request.state.plan_code = get_user_plan(user)
        request.state.plan_features = get_plan_features(user)
        request.state.lang = get_preferred_language(request, user)
        path = request.url.path
        exempt = path.startswith("/admin") or path.startswith("/static") or path in {"/login", "/logout", "/signup"}
        maintenance_user = user or _request_user_from_signed_cookie(request, db)
        if not exempt and get_setting(db, "site_maintenance_mode", "false") == "true" and not (maintenance_user and getattr(maintenance_user, "is_admin", False)):
            return Response(
                content="""<!DOCTYPE html><html><head><title>Bakım</title>
                <style>body{font-family:sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0;background:#0d1117;color:#f0f0f0;}
                .box{text-align:center;max-width:420px;padding:48px 32px;}
                h1{font-size:1.6rem;margin-bottom:16px;}p{color:#888;line-height:1.7;}</style></head>
                <body><div class="box">
                <h1>Bakım Çalışması</h1>
                <p>Sitemiz kısa süre için bakım modunda. En kısa sürede geri döneceğiz.</p>
                </div></body></html>""",
                status_code=503,
                media_type="text/html",
            )
        if request.method == "POST" and path.startswith("/admin"):
            body = await request.body()
            if not verify_csrf_token(request, _csrf_token_from_body(request, body)):
                return Response("Invalid CSRF token", status_code=403, media_type="text/plain")
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        response.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
        return response
    finally:
        db.close()


def _extract_transit_planet(event_name):
    match = re.search(r"Transit ([A-Za-z]+)", str(event_name))
    return match.group(1) if match else ""


def _legacy_duration_for_planet(planet_name):
    if planet_name in {"Saturn", "Pluto"}:
        return 90
    if planet_name in {"Jupiter", "Uranus", "Neptune"}:
        return 45
    if planet_name in {"Mars", "Venus", "Mercury"}:
        return 14
    return 7


def _importance_level(score):
    if score >= 85:
        return "critical"
    if score >= 70:
        return "major"
    if score >= 55:
        return "strong"
    if score >= 40:
        return "moderate"
    return "minor"


def _life_area_from_house(house):
    house_map = {
        1: "personal_growth",
        2: "finances",
        4: "home",
        6: "health",
        7: "relationships",
        8: "personal_growth",
        9: "spirituality",
        10: "career",
        11: "social_network",
    }
    try:
        return house_map.get(int(house), "general")
    except (TypeError, ValueError):
        return "general"


def _life_area_from_impacts(impacts):
    points = {impact.get("point") for impact in impacts if isinstance(impact, dict)}
    if {"Sun", "Saturn", "Jupiter"} & points:
        return "career"
    if {"Venus", "Moon"} & points:
        return "relationships"
    if {"Lagna", "Mars", "Ketu", "Rahu"} & points:
        return "personal_growth"
    return "general"


def _build_phase28_event_stream(transit_impact, eclipse_data, fullmoon_data):
    events = []
    today = datetime.now(pytz.UTC).strftime("%Y-%m-%d")

    for idx, transit in enumerate(transit_impact):
        event_name = transit.get("event", "")
        planet_name = _extract_transit_planet(event_name)
        house = transit.get("house")
        importance_score = int(round(float(transit.get("score", 0))))
        events.append(
            {
                "event_id": f"TRANSIT_{idx}_{today}",
                "event_type": "CURRENT_TRANSIT_IMPACT",
                "date": today,
                "importance_score": importance_score,
                "importance_level": _importance_level(importance_score),
                "duration_days": _legacy_duration_for_planet(planet_name),
                "duration": _legacy_duration_for_planet(planet_name),
                "transit_planet": planet_name,
                "house": house,
                "dominant_activation_type": (
                    "outer_planet_event"
                    if planet_name in {"Saturn", "Jupiter", "Uranus", "Neptune", "Pluto"}
                    else "trigger_event"
                ),
                "life_area": _life_area_from_house(house),
            }
        )

    for idx, eclipse in enumerate(eclipse_data):
        impact_count = len(eclipse.get("natal_impacts", []))
        eclipse_type = str(eclipse.get("type", ""))
        subtype = str(eclipse.get("subtype", ""))
        importance_score = min(96, 62 + impact_count * 8)
        duration_days = 45 if "Solar" in eclipse_type else 30
        events.append(
            {
                "event_id": f"ECLIPSE_{idx}_{eclipse.get('date')}",
                "event_type": "ECLIPSE",
                "date": eclipse.get("date"),
                "importance_score": importance_score,
                "importance_level": _importance_level(importance_score),
                "duration_days": duration_days,
                "duration": duration_days,
                "planet": "Sun" if "Solar" in eclipse_type else "Moon",
                "transit_planets": ["Sun", "Moon"],
                "dominant_activation_type": "multi_activation" if impact_count >= 2 else "trigger_event",
                "life_area": _life_area_from_impacts(eclipse.get("natal_impacts", [])),
                "dominant_theme": "identity_transformation" if "Tam" in subtype else "closure_cycle",
            }
        )

    for idx, lunation in enumerate(fullmoon_data):
        if isinstance(lunation, Mapping):
            event = dict(lunation)
        elif isinstance(lunation, datetime):
            event = {"date": lunation.strftime("%Y-%m-%d")}
        elif isinstance(lunation, date):
            event = {"date": lunation.isoformat()}
        else:
            continue

        importance_score = int(round(float(event.get("importance_score", 68))))
        duration_days = int(event.get("duration_days") or event.get("duration") or 3)
        event.setdefault("event_id", f"LUNATION_{idx}_{event.get('date', today)}")
        event.setdefault("event_type", "FULL_MOON")
        event.setdefault("importance_score", importance_score)
        event.setdefault("importance_level", _importance_level(importance_score))
        event.setdefault("duration_days", duration_days)
        event.setdefault("duration", duration_days)
        event.setdefault("dominant_activation_type", "trigger_event")
        event.setdefault("life_area", _life_area_from_house(event.get("moon_house") or event.get("house")))
        events.append(event)

    events.sort(key=lambda item: item.get("date", "9999-12-31"))
    return events


def _confidence_level_label(score):
    if score >= 80:
        return "high"
    if score >= 60:
        return "moderate"
    return "low"


def _safe_get(mapping, *keys, default=None):
    current = mapping
    for key in keys:
        if not isinstance(current, Mapping) or key not in current:
            return default
        current = current[key]
    return current


def _normalize_window(window, fallback_label):
    if isinstance(window, Mapping):
        return {
            "start": window.get("start"),
            "end": window.get("end"),
            "label": window.get("label") or fallback_label,
        }
    return None


def _top_window(windows, fallback_label):
    if not windows:
        return None
    return _normalize_window(windows[0], fallback_label)


def _decision_posture(
    stress_ratio,
    growth_ratio,
    confidence_score,
    peak_window,
    pressure_window,
):
    if stress_ratio >= 70 and pressure_window:
        return "avoid"
    if growth_ratio >= 65 and confidence_score >= 75:
        return "act"
    if growth_ratio >= 55 and confidence_score >= 60:
        return "act_cautiously"
    if stress_ratio >= 55 and peak_window:
        return "hold"
    return "prepare"


def _tone_mode(stress_ratio, growth_ratio, confidence_score):
    if stress_ratio >= 60:
        return "calm_strategic"
    if growth_ratio >= 60 and confidence_score >= 75:
        return "confident_expansive"
    if confidence_score < 60:
        return "careful_guidance"
    return "balanced_advisory"


def _serialize_temporal_values(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {key: _serialize_temporal_values(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_serialize_temporal_values(item) for item in value]
    if isinstance(value, tuple):
        return [_serialize_temporal_values(item) for item in value]
    if isinstance(value, set):
        return [_serialize_temporal_values(item) for item in value]
    return value


def _build_interpretation_context(phase28_events, psychological_themes, life_area_analysis, narrative_analysis, timing_intelligence):
    stress_vs_growth_ratio = psychological_themes.get("stress_vs_growth_ratio", {"stress": 50, "growth": 50})
    top_importance = max((int(event.get("importance_score", 0)) for event in phase28_events), default=0)
    top_narrative_score = max(
        (
            int(narrative.get("narrative_score", 0))
            for bucket in ("primary_narratives", "secondary_narratives", "emerging_narratives")
            for narrative in narrative_analysis.get(bucket, [])
        ),
        default=0,
    )
    confidence_map = {
        "very_high": 90,
        "high": 78,
        "moderate": 62,
        "low": 45,
    }
    timing_confidence = timing_intelligence.get("timing_confidence", "moderate")
    confidence_score = confidence_map.get(timing_confidence, 62)

    major_windows = timing_intelligence.get("major_peak_windows", [])[:5]
    opportunity_windows = timing_intelligence.get("opportunity_windows", [])[:3]
    pressure_windows = timing_intelligence.get("pressure_windows", [])[:3]
    primary_narratives = narrative_analysis.get("primary_narratives", [])
    dominant_life_areas = life_area_analysis.get("dominant_life_areas", [])

    peak_window = _top_window(major_windows, "Ana yogunlasma donemi")
    opportunity_window = _top_window(opportunity_windows, "Acilim ve buyume alani")
    pressure_window = _top_window(pressure_windows, "Baski ve dikkat donemi")

    stress_ratio = stress_vs_growth_ratio.get("stress", 50)
    growth_ratio = stress_vs_growth_ratio.get("growth", 50)
    timing_strategy = timing_intelligence.get("interpretation_timing_strategy", "mixed")
    primary_focus = life_area_analysis.get("primary_life_focus")
    secondary_focus = life_area_analysis.get("secondary_life_focus")
    decision_posture = _decision_posture(
        stress_ratio,
        growth_ratio,
        confidence_score,
        peak_window,
        pressure_window,
    )
    tone_mode = _tone_mode(stress_ratio, growth_ratio, confidence_score)

    return {
        "stress_vs_growth_ratio": stress_vs_growth_ratio,
        "importance_score": max(top_importance, top_narrative_score),
        "confidence_level": _confidence_level_label(confidence_score),
        "confidence_score": confidence_score,
        "timing_confidence": timing_confidence,
        "tone_flags": {
            "high_stress": stress_vs_growth_ratio.get("stress", 0) >= 60,
            "high_opportunity": stress_vs_growth_ratio.get("growth", 0) >= 60,
            "high_confidence": confidence_score >= 80,
            "low_confidence": confidence_score < 60,
        },
        "primary_life_focus": primary_focus,
        "secondary_life_focus": secondary_focus,
        "primary_focus": primary_focus,
        "secondary_focus": secondary_focus,
        "life_stability_index": life_area_analysis.get("life_stability_index"),
        "top_timing_windows": {
            "peak": peak_window,
            "opportunity": opportunity_window,
            "pressure": pressure_window,
        },
        "opportunity_summary_windows": opportunity_windows,
        "pressure_summary_windows": pressure_windows,
        "interpretation_timing_strategy": timing_strategy,
        "timing_strategy": timing_strategy,
        "dominant_narratives": [item.get("narrative_type", "").replace("_", " ") for item in primary_narratives if item.get("narrative_type")],
        "dominant_life_areas": [item.get("life_area") for item in dominant_life_areas if item.get("life_area")],
        "decision_posture": decision_posture,
        "tone_mode": tone_mode,
    }


def _build_interpretation_accuracy_context(natal_data, dasha_data, *, personalization=None, transit_data=None):
    layer = build_interpretation_layer(
        natal_data,
        dasha_data=dasha_data,
        personalization=personalization,
        transit_data=transit_data,
    )
    top_signals = []
    for signal in layer.get("prioritized_signals", [])[:5]:
        top_signals.append(
            {
                "type": signal.get("type"),
                "planet": signal.get("planet"),
                "other_planet": signal.get("other_planet"),
                "house": signal.get("house"),
                "sign": signal.get("sign"),
                "score": signal.get("score"),
                "tags": signal.get("tags", [])[:3],
            }
        )
    return {
        "prioritized_signals": top_signals,
        "domain_scores": layer.get("domain_mapping", {}).get("domain_scores", {}),
        "premium_interpretation": layer.get("interpretation", {}),
        "top_anchors": layer.get("anchors", {}).get("top_anchors", []),
        "narrative_backbone": layer.get("anchors", {}).get("narrative_backbone", ""),
        "anchor_prompt_block": layer.get("anchors", {}).get("anchor_prompt_block", ""),
        "confidence_notes": layer.get("anchors", {}).get("confidence_notes", []),
        "feedback_ready": bool(layer.get("feedback_ready")),
        "calibration_summary": layer.get("calibration_summary", {}),
        "personalization_summary": layer.get("personalization_summary", {}),
        "anchor_calibration_notes": layer.get("anchor_calibration_notes", []),
        "personalization_ready": bool(layer.get("personalization")),
        "recommendation_layer": layer.get("recommendation_layer", {}),
    }


def _public_error(message, status_code=400):
    raise HTTPException(status_code=status_code, detail=message)


def _launch_panel_copy(language="tr"):
    if str(language or "tr").lower() == "en":
        return {
            "title": "Coming Soon",
            "message": (
                "Focus Astrology is currently in the final preparation phase for personal reports, "
                "consultation services, and payment infrastructure. To give you a secure, professional, "
                "and high-standard experience, we are keeping this area temporarily closed."
            ),
            "home_label": "Back to Home",
            "info_label": "Get Information",
            "empty_label": "No fusion predictions available for this profile.",
        }
    return {
        "title": "Yak\u0131nda Yay\u0131nda",
        "message": (
            "Focus Astrology \u015fu anda ki\u015fisel raporlar, dan\u0131\u015fmanl\u0131k hizmetleri ve \u00f6deme altyap\u0131s\u0131 i\u00e7in "
            "son haz\u0131rl\u0131k s\u00fcrecindedir. Size g\u00fcvenli, profesyonel ve y\u00fcksek standartlarda bir deneyim "
            "sunabilmek i\u00e7in bu alan\u0131 ge\u00e7ici olarak kapal\u0131 tutuyoruz."
        ),
        "home_label": "Ana Sayfaya D\u00f6n",
        "info_label": "Bilgi Al",
        "empty_label": "Bu profil i\u00e7in hen\u00fcz bir birle\u015fik \u00f6ng\u00f6r\u00fc penceresi g\u00f6r\u00fcnm\u00fcyor.",
    }

def _coming_soon_template_response(request, *, status_code=200):
    copy = _launch_panel_copy(getattr(request.state, "lang", "tr"))
    return templates.TemplateResponse(
        request=request,
        name="coming_soon.html",
        status_code=status_code,
        context=_auth_template_context(
            request,
            coming_soon=copy,
        ),
    )


def _coming_soon_json_response(language="tr"):
    copy = _launch_panel_copy(language)
    return JSONResponse(
        {
            "ok": False,
            "launch_mode": True,
            "message": copy.get("title") or ("Coming Soon" if str(language or "tr").lower() == "en" else "Yakinda Yayinda"),
        },
        status_code=200,
    )


def _ensure_runtime_dir(path):
    path.mkdir(parents=True, exist_ok=True)
    return path


def _ensure_fontconfig_runtime_file():
    _ensure_runtime_dir(RUNTIME_CACHE_DIR)
    _ensure_runtime_dir(FONTCONFIG_CACHE_DIR)
    _ensure_runtime_dir(RUNTIME_TMP_DIR)

    include_paths = []
    if (GTK_FONTS_DIR / "conf.d").exists():
        include_paths.append((GTK_FONTS_DIR / "conf.d").as_posix())
    elif GTK_FONTS_DIR.exists():
        include_paths.append(GTK_FONTS_DIR.as_posix())

    font_dirs = []
    if WINDOWS_FONTS_DIR.exists():
        font_dirs.append(WINDOWS_FONTS_DIR.as_posix())
    gtk_share_fonts = GTK_SHARE_DIR / "fonts"
    if gtk_share_fonts.exists():
        font_dirs.append(gtk_share_fonts.as_posix())

    if not include_paths and not font_dirs:
        return None

    include_xml = "\n".join(f'    <include ignore_missing="yes">{path}</include>' for path in include_paths)
    font_dir_xml = "\n".join(f"    <dir>{path}</dir>" for path in font_dirs)
    runtime_config = f"""<?xml version="1.0"?>
<!DOCTYPE fontconfig SYSTEM "fonts.dtd">
<fontconfig>
{font_dir_xml}
{include_xml}
    <cachedir>{FONTCONFIG_CACHE_DIR.as_posix()}</cachedir>
</fontconfig>
"""
    FONTCONFIG_RUNTIME_FILE.write_text(runtime_config, encoding="utf-8")
    return FONTCONFIG_RUNTIME_FILE


def _prepend_env_path(path_value):
    current_entries = [entry for entry in os.environ.get("PATH", "").split(os.pathsep) if entry]
    if path_value not in current_entries:
        os.environ["PATH"] = path_value + os.pathsep + os.environ.get("PATH", "")


def configure_windows_weasyprint_runtime():
    global _PDF_RUNTIME_CONFIGURED, _PDF_DLL_DIRECTORY_HANDLE

    if _PDF_RUNTIME_CONFIGURED or not sys.platform.startswith("win"):
        return

    _ensure_runtime_dir(RUNTIME_CACHE_DIR)
    _ensure_runtime_dir(FONTCONFIG_CACHE_DIR)
    _ensure_runtime_dir(RUNTIME_TMP_DIR)

    if GTK_BIN_DIR.exists():
        gtk_bin = str(GTK_BIN_DIR)
        _prepend_env_path(gtk_bin)
        os.environ["WEASYPRINT_DLL_DIRECTORIES"] = gtk_bin
        if hasattr(os, "add_dll_directory") and _PDF_DLL_DIRECTORY_HANDLE is None:
            try:
                _PDF_DLL_DIRECTORY_HANDLE = os.add_dll_directory(gtk_bin)
            except (FileNotFoundError, OSError):
                logger.warning("Could not register GTK DLL directory: %s", gtk_bin, exc_info=True)

    runtime_fontconfig = _ensure_fontconfig_runtime_file()
    if GTK_FONTS_DIR.exists():
        os.environ["FONTCONFIG_PATH"] = str(GTK_FONTS_DIR)
    if runtime_fontconfig and runtime_fontconfig.exists():
        os.environ["FONTCONFIG_FILE"] = str(runtime_fontconfig)
    elif GTK_FONTS_FILE.exists():
        os.environ["FONTCONFIG_FILE"] = str(GTK_FONTS_FILE)
    if GTK_SHARE_DIR.exists():
        os.environ["XDG_DATA_DIRS"] = str(GTK_SHARE_DIR)

    os.environ["XDG_CACHE_HOME"] = str(RUNTIME_CACHE_DIR)
    os.environ["FC_CACHEDIR"] = str(FONTCONFIG_CACHE_DIR)
    os.environ["TEMP"] = str(RUNTIME_TMP_DIR)
    os.environ["TMP"] = str(RUNTIME_TMP_DIR)
    os.environ["TMPDIR"] = str(RUNTIME_TMP_DIR)
    os.environ["HOME"] = str(BASE_DIR)
    os.environ["USERPROFILE"] = str(BASE_DIR)
    _PDF_RUNTIME_CONFIGURED = True


def _focus_label(value, language="tr"):
    focus_map = {
        "career": {"tr": "Kariyer", "en": "Career"},
        "finances": {"tr": "Finans", "en": "Finances"},
        "relationships": {"tr": "İlişkiler", "en": "Relationships"},
        "personal_growth": {"tr": "Kişisel gelişim", "en": "Personal growth"},
        "spirituality": {"tr": "Anlam ve inanç", "en": "Meaning and belief"},
        "home": {"tr": "İç dünya ve ev", "en": "Home and inner world"},
        "health": {"tr": "Sağlık", "en": "Health"},
        "social_network": {"tr": "Sosyal ağ", "en": "Social network"},
        "family": {"tr": "Aile", "en": "Family"},
        "parent_child_guidance": {"tr": "Ebeveyn-çocuk rehberliği", "en": "Parent-child guidance"},
        "child_growth": {"tr": "Çocuğun gelişimi", "en": "Child growth"},
        "general": {"tr": "Genel yaşam akışı", "en": "General life flow"},
    }
    key = str(value or "general").strip().lower().replace(" ", "_")
    if key in focus_map:
        return focus_map[key].get(language, focus_map[key]["tr"])
    return str(value or "")


def _result_language(request, user=None):
    language = getattr(getattr(request, "state", None), "lang", None) or get_preferred_language(request, user)
    language = str(language or "tr").lower()
    return language if language in {"tr", "en"} else "tr"


def _labelize(value):
    return str(value or "").replace("_", " ").strip().title()


_PARENT_CHILD_TEXT_TR = {
    "Expressive, fast-moving, and naturally motivated by visible momentum.": "İfade gücü yüksek, hızlı tepki veren ve görünür hareketle doğal olarak motive olan bir yapı.",
    "Steady, tactile, and more secure when life feels structured and predictable.": "Düzen ve öngörülebilirlik olduğunda daha güvende hisseden, sağlam ve somut bir yapı.",
    "Curious, verbal, and stimulated by questions, patterns, and exchange.": "Meraklı, sözel ifade ile güçlenen ve soru-cevap akışıyla canlı kalan bir yapı.",
    "Sensitive, imaginative, and emotionally responsive to the tone of the environment.": "Hassas, hayal gücü kuvvetli ve bulunduğu ortamın duygusal tonuna açık bir yapı.",
    "Layered, responsive, and shaped strongly by emotional tone and rhythm.": "Katmanlı, duyarlı ve duygusal ton ile ritimden güçlü şekilde etkilenen bir yapı.",
    "Shows clear inner patterning when given consistency.": "Tutarlılık olduğunda iç düzenini daha net gösterir.",
    "Responds well to supportive encouragement rather than force.": "Baskıdan çok destekleyici teşvikle daha iyi yanıt verir.",
    "Needs more recovery time after emotional intensity or overstimulation.": "Duygusal yoğunluk veya aşırı uyarılma sonrası daha fazla toparlanma süresine ihtiyaç duyar.",
    "Can internalize pressure quickly when expectations feel heavy or rigid.": "Beklentiler ağır ya da katı hissedildiğinde baskıyı hızla içine alabilir.",
    "May become guarded when emotional tone changes too quickly around them.": "Etrafındaki duygusal ton çok hızlı değiştiğinde içine kapanabilir.",
    "Needs calm emotional mirroring, predictability, and room to process feelings before being pushed into action.": "Eyleme itilmeden önce sakin duygusal eşlik, öngörülebilirlik ve duygularını işleyecek alana ihtiyaç duyar.",
    "Needs responsive conversation, movement, and encouragement that keeps emotional expression active.": "Duygusal ifadesini canlı tutan karşılıklı iletişim, hareket alanı ve cesaretlendirmeye ihtiyaç duyar.",
    "Grows best when guidance feels spacious and confidence-building.": "Rehberlik alan açtığında ve güven duygusunu güçlendirdiğinde daha iyi gelişir.",
    "Grows best when support is steady, simple, and emotionally consistent.": "Destek düzenli, sade ve duygusal olarak tutarlı olduğunda daha iyi gelişir.",
    "Learns quickly through conversation, questions, and repeating ideas out loud.": "Konuşarak, soru sorarak ve fikirleri sesli tekrar ederek daha hızlı öğrenir.",
    "Learns best through repetition, examples, and steady hands-on reinforcement.": "Tekrar, örnekler ve düzenli uygulamalı destekle daha iyi öğrenir.",
    "Attention rises when the task feels active and mentally alive.": "Görev hareketli ve zihinsel olarak canlı hissettirdiğinde dikkati artar.",
    "Attention improves when pace is slower and the environment is clearly organized.": "Tempo yavaşlayıp ortam net biçimde düzenlendiğinde dikkati toparlanır.",
    "Confidence grows when the child is invited to speak before being corrected.": "Düzeltilmeden önce konuşmasına alan açıldığında özgüveni artar.",
    "Confidence grows when the child is given time to prepare before public expression.": "Kendini görünür biçimde ifade etmeden önce hazırlanma zamanı verildiğinde özgüveni artar.",
    "May become self-critical when school pressure feels constant.": "Okul baskısı sürekli hissedildiğinde kendine fazla yüklenebilir.",
    "Can push too hard, then lose patience when the pace feels repetitive.": "Kendini fazla zorlayıp tempo tekrar hissi verdiğinde sabırsızlaşabilir.",
    "May need adults to translate pressure into calm structure rather than urgency.": "Yetişkinlerin baskıyı aceleye değil, sakin yapıya dönüştürmesine ihtiyaç duyabilir.",
    "The child thrives when encouragement is consistent, emotionally safe, and tied to realistic pacing.": "Cesaretlendirme düzenli, duygusal olarak güvenli ve gerçekçi bir tempoya bağlı olduğunda çocuk daha iyi gelişir.",
    "The relationship strengthens when correction becomes guidance and timing is treated with patience.": "Düzeltme rehberliğe dönüştüğünde ve zamanlama sabırla ele alındığında ilişki güçlenir.",
    "Confidence grows through calm repetition and trust-building support.": "Sakin tekrar ve güven inşa eden destekle özgüven büyür.",
    "Emotional flow is easier here because both charts process feelings with a similar tempo.": "Her iki harita da duyguları benzer bir tempoda işlediği için duygusal akış burada daha kolaydır.",
    "Emotional tone may differ: one chart processes quickly while the other needs more time and softness.": "Duygusal ton farkı olabilir: bir harita hızlı işlerken diğeri daha fazla zaman ve yumuşaklık ister.",
    "Communication can feel naturally smooth when ideas are spoken through in real time.": "Fikirler anlık olarak konuşulduğunda iletişim daha doğal ve akıcı hissedilebilir.",
    "Misunderstandings are more likely when the parent explains quickly but the child needs slower repetition or reassurance.": "Ebeveyn hızlı anlattığında ama çocuk daha yavaş tekrar ya da güvenceye ihtiyaç duyduğunda yanlış anlaşılmalar artabilir.",
    "Conflict can rise when urgency replaces regulation.": "Düzenleme yerine acele geçtiğinde çatışma artabilir.",
    "The child may carry correction as pressure more strongly than adults expect.": "Çocuk, düzeltmeyi yetişkinlerin tahmin ettiğinden daha güçlü bir baskı olarak taşıyabilir.",
    "Friction is more likely from pacing differences than from lack of care.": "Sürtüşme, ilgisizlikten çok tempo farklarından doğabilir.",
    "The parent chart naturally brings encouragement, perspective, and long-range support.": "Ebeveyn haritası doğal olarak cesaretlendirme, perspektif ve uzun vadeli destek getirir.",
    "The relationship flows best when emotional safety comes before instruction.": "İlişki, yönlendirmeden önce duygusal güven geldiğinde en iyi akar.",
    "Use short, calm, repeatable language and check for emotional readiness before teaching.": "Kısa, sakin ve tekrarlanabilir bir dil kullanın; öğretmeden önce duygusal hazır oluşu kontrol edin.",
    "Invite questions and dialogue so the child can process by interacting, not only by listening.": "Çocuğun yalnızca dinleyerek değil etkileşime girerek işlemesi için soru ve diyalog alanı açın.",
    "Avoid turning discipline into chronic pressure or constant evaluation.": "Disiplini kronik baskıya ya da sürekli değerlendirmeye dönüştürmeyin.",
    "Avoid demanding emotional expression on a timetable when the child is still internalizing.": "Çocuk hâlâ içselleştirirken duygusal ifadeyi belli bir takvime zorlamayın.",
    "Avoid over-correcting in the moment; timing matters as much as content.": "Anın içinde aşırı düzeltmeye gitmeyin; zamanlama en az içerik kadar önemlidir.",
    "The parent can help most by combining consistency, emotional safety, and a clear rhythm.": "Ebeveyn en çok; tutarlılığı, duygusal güveni ve net bir ritmi birleştirerek destek olabilir.",
    "Child core emotional pattern": "Çocuğun temel duygusal örüntüsü",
    "Parent-child relationship dynamic": "Ebeveyn-çocuk ilişki dinamiği",
    "Parenting guidance that lands best": "En iyi karşılık bulan ebeveynlik yaklaşımı",
    "The relationship strengthens when support stays calm, specific, and repeatable.": "Destek sakin, net ve tekrarlanabilir kaldığında ilişki güçlenir.",
    "Lead with calm, specific communication": "Sakin ve net iletişimle başlayın",
    "Current phase": "Mevcut dönem",
    "Support learning through the child's natural pace": "Öğrenmeyi çocuğun doğal temposu üzerinden destekleyin",
    "School and routine cycles": "Okul ve rutin döngüleri",
    "Reduce pressure before correcting behavior": "Davranışı düzeltmeden önce baskıyı azaltın",
    "Moments of stress": "Stres anları",
    "Growth-supportive rhythm": "Gelişimi destekleyen ritim",
    "Current season": "Mevcut dönem",
    "Pressure-sensitive periods": "Baskıya hassas dönemler",
    "When routines feel overloaded": "Rutinler fazla yüklü hissettirdiğinde",
    "The child's current timing is being colored by ": "Çocuğun mevcut zamanlaması ",
    " period emphasis": " dönemi vurgusu",
}


_SIGNAL_LABELS_TR = {
    "money": "finans",
    "wealth": "kaynak ve kazanç",
    "career": "kariyer",
    "growth": "büyüme",
    "timing": "zamanlama",
    "nodes": "Ay Düğümleri",
    "relationship": "ilişki",
    "parent-child": "ebeveyn-çocuk",
    "child core emotional pattern": "çocuğun temel duygusal örüntüsü",
    "parent-child relationship dynamic": "ebeveyn-çocuk ilişki dinamiği",
    "parenting guidance that lands best": "en etkili ebeveynlik yaklaşımı",
    "current phase": "mevcut dönem",
    "school and routine cycles": "okul ve rutin döngüleri",
    "moments of stress": "stres anları",
    "growth-supportive rhythm": "gelişimi destekleyen ritim",
    "pressure-sensitive periods": "baskıya hassas dönemler",
    "mars period emphasis": "Mars dönemi vurgusu",
    "lead with calm, specific communication": "sakin ve net iletişimle ilerleyin",
    "support learning through the child's natural pace": "öğrenmeyi çocuğun doğal temposuyla destekleyin",
    "reduce pressure before correcting behavior": "davranışı düzeltmeden önce baskıyı azaltın",
    "invite questions and dialogue so the child can process by interacting, not only by listening": "çocuğun yalnızca dinleyerek değil, etkileşime girerek işlemesi için soru ve diyalog alanı açın",
}


def _localize_signal_label(value, language):
    text = str(value or "")
    if language != "tr" or not text:
        return text
    normalized = text.strip().lower()
    return _SIGNAL_LABELS_TR.get(normalized, text)


def _localize_signal_phrase(value, language):
    text = str(value or "")
    if language != "tr" or not text:
        return text
    exact = _PARENT_CHILD_TEXT_TR.get(text) or _RESULT_PHRASE_LOCALIZATION_TR.get(text)
    if exact:
        return exact
    normalized = text.strip().lower()
    if normalized in _SIGNAL_LABELS_TR:
        return _SIGNAL_LABELS_TR[normalized]
    if text.endswith(" period emphasis"):
        planet = text[: -len(" period emphasis")].strip()
        return f"{planet} dönemi vurgusu"
    if text.startswith("The chart is currently led by "):
        body = text.replace("The chart is currently led by ", "", 1).rstrip(".")
        body = body.replace("money and nodes", "finans ve Ay Düğümleri")
        body = body.replace("Timing and money", "zamanlama ve finans")
        body = body.replace("timing and money", "zamanlama ve finans")
        body = body.replace("money", "finans")
        body = body.replace("nodes", "Ay Düğümleri")
        return f"Haritada şu anda {body} teması öne çıkıyor."
    if text.startswith("The chart is currently "):
        body = text.replace("The chart is currently ", "", 1).rstrip(".")
        body = body.replace("money and nodes", "finans ve Ay Düğümleri")
        body = body.replace("Timing and money", "zamanlama ve finans")
        body = body.replace("timing and money", "zamanlama ve finans")
        body = body.replace("money", "finans")
        body = body.replace("nodes", "Ay Düğümleri")
        return f"Haritada şu anda {body} vurgusu öne çıkıyor."
    localized = text
    replacements = {
        "Current phase": "Mevcut dönem",
        "Current Phase": "Mevcut dönem",
        "Timing": "Zamanlama",
        "timing": "zamanlama",
        "Timing and money need extra care.": "Zamanlama ve finans alanı daha dikkatli ele alınmalı.",
        "Growth can happen through relationship repair.": "Büyüme, ilişkileri onarma üzerinden desteklenebilir.",
        "Nodes can scatter attention.": "Ay Düğümleri dikkati dağıtabilir.",
        "Nodes": "Ay Düğümleri",
        "nodes": "Ay Düğümleri",
        "money": "finans",
        "relationships": "ilişkiler",
        "wealth": "kaynak ve kazanç",
        "growth-supportive rhythm": "gelişimi destekleyen ritim",
        "pressure-sensitive periods": "baskıya hassas dönemler",
        "school and routine cycles": "okul ve rutin döngüleri",
        "Child core emotional pattern": "Çocuğun temel duygusal örüntüsü",
        "Parent-child relationship dynamic": "Ebeveyn-çocuk ilişki dinamiği",
    }
    for source, target in replacements.items():
        localized = localized.replace(source, target)
    return localized


def _localize_parent_child_phrase(value, language):
    text = str(value or "")
    if language != "tr" or not text:
        return text
    text = _localize_signal_phrase(text, language)
    if text in _PARENT_CHILD_TEXT_TR:
        return _PARENT_CHILD_TEXT_TR[text]
    if text.startswith("The child's current timing is being colored by ") and text.endswith(", so emotional support and pacing should be read in that context."):
        planet = text.replace("The child's current timing is being colored by ", "", 1).replace(", so emotional support and pacing should be read in that context.", "")
        return f"Çocuğun mevcut zamanlaması {planet} tarafından renklendiği için duygusal destek ve tempo bu bağlamda değerlendirilmelidir."
    return text


def _localize_parent_child_interpretation_context(interpretation_context, language):
    if language != "tr" or not isinstance(interpretation_context, dict):
        return interpretation_context or {}
    context = copy.deepcopy(interpretation_context)
    for section_name in (
        "child_profile",
        "relationship_dynamics",
        "school_guidance",
        "parenting_guidance",
        "growth_guidance",
        "parent_profile",
        "child_profile_meta",
    ):
        section = context.get(section_name)
        if isinstance(section, dict):
            for key, value in list(section.items()):
                if isinstance(value, str):
                    section[key] = _localize_parent_child_phrase(value, language)
                elif isinstance(value, list):
                    section[key] = [
                        _localize_parent_child_phrase(item, language) if isinstance(item, str) else item
                        for item in value
                    ]
    for list_name in ("watch_areas",):
        items = context.get(list_name)
        if isinstance(items, list):
            context[list_name] = [
                _localize_parent_child_phrase(item, language) if isinstance(item, str) else item
                for item in items
            ]
    for list_name in ("timing_notes",):
        items = context.get(list_name)
        if isinstance(items, list):
            localized_items = []
            for item in items:
                if not isinstance(item, dict):
                    localized_items.append(item)
                    continue
                row = dict(item)
                for key, value in list(row.items()):
                    if isinstance(value, str):
                        row[key] = _localize_parent_child_phrase(value, language)
                localized_items.append(row)
            context[list_name] = localized_items
    if isinstance(context.get("summary"), str):
        context["summary"] = _localize_parent_child_phrase(context["summary"], language)
    return context


_RESULT_DOMAIN_LABELS_TR = {
    "career": "kariyer",
    "money": "finans",
    "finances": "finans",
    "relationships": "ilişkiler",
    "inner_state": "iç dünya",
    "growth": "büyüme",
    "personal_growth": "kişisel gelişim",
    "home": "ev ve iç denge",
    "health": "sağlık",
    "social_network": "sosyal çevre",
    "education": "öğrenme",
    "spirituality": "anlam arayışı",
}

_RESULT_PHRASE_LOCALIZATION_TR = {
    "Prioritize deliberate career positioning": "Kariyer yönünüzü bilinçli şekilde önceliklendirin",
    "Delay major financial commitments": "Büyük finansal taahhütleri yavaşlatın",
    "Have important conversations with more clarity": "Önemli konuşmaları daha net bir çerçeveyle yapın",
    "Rebuild routine and inner steadiness": "Rutini ve iç dengeyi yeniden kurun",
    "Use the current opening for targeted growth": "Mevcut açılımı hedefli büyüme için kullanın",
    "This recommendation is driven by current dasha emphasis and reinforced by chart themes around structured progress in work decisions.": "Bu öneri, mevcut dasha vurgusu ve iş kararlarında daha yapılandırılmış ilerleme ihtiyacını gösteren harita temalarıyla desteklenir.",
    "This recommendation is driven by current dasha emphasis and reinforced by chart themes around caution in money decisions.": "Bu öneri, mevcut dasha vurgusu ve finansal kararlarda daha temkinli ilerleme ihtiyacını gösteren harita temalarıyla desteklenir.",
    "This recommendation is driven by the current chart emphasis on relational honesty and cleaner emotional boundaries.": "Bu öneri, ilişkilerde dürüstlük ve daha temiz duygusal sınırlar gerektiren mevcut harita vurgusundan gelir.",
    "This recommendation is driven by a concentration of signals that reward steadier pacing, reflection, and better emotional regulation.": "Bu öneri, daha dengeli tempo, düşünerek hareket etme ve duygusal düzenleme isteyen güçlü sinyal yoğunluğuna dayanır.",
    "This recommendation is driven by the current dasha opening and reinforced by broader chart themes around visible expansion.": "Bu öneri, mevcut dasha açılımı ve görünür büyümeyi destekleyen daha geniş harita temalarıyla güçlenir.",
    "Inner expansion under invisible pressure": "Görünmeyen baskı altında içsel genişleme",
    "Career ambition with material consequences": "Maddi sonuçları olan kariyer odağı",
    "Emotional independence in relationships": "İlişkilerde duygusal bağımsızlık",
    "Growth path with public consequence": "Görünür sonuçları olan büyüme yolu",
    "Security rebuilding through restraint": "Ölçülülükle güvenliği yeniden kurma",
    "A smaller but still useful supporting theme remains active.": "Daha küçük ama yine de anlamlı bir destek teması aktif kalıyor.",
    "This anchor preserves narrative completeness when the chart compresses into fewer dominant clusters.": "Bu odak, harita daha az sayıda güçlü kümeye sıkıştığında anlatının bütünlüğünü korur.",
    "Current dasha emphasis is centered on {planet}, so timing-sensitive guidance is weighted more heavily.": "Mevcut dasha vurgusu {planet} üzerinde toplandığı için zamanlamaya duyarlı rehberlik daha fazla ağırlık taşır.",
    "Slightly prioritized due to stronger user response to clear_direct guidance.": "Daha net ve doğrudan rehberliğe verilen güçlü kullanıcı tepkisi nedeniyle hafifçe önceliklendirildi.",
    "Slightly softened because this domain has recently been rated as too generic.": "Bu alan yakın zamanda fazla genel bulunduğu için hafifçe yumuşatıldı.",
    "Recommendation wording stays more direct because recent feedback favored clearer guidance.": "Son geri bildirimler daha net rehberliği öne çıkardığı için öneri dili daha doğrudan tutuldu.",
    "Recent recommendation feedback suggests users respond well to direct, action-oriented guidance.": "Son öneri geri bildirimleri, doğrudan ve eyleme dönük rehberliğin daha iyi karşılandığını gösteriyor.",
    "Maturity, meaningful expansion, and stronger long-range vision.": "Olgunlaşma, anlamlı genişleme ve daha güçlü uzun vadeli vizyon.",
    "Strategic positioning, earned credibility, and visible progress.": "Stratejik konumlanma, kazanılmış güvenilirlik ve görünür ilerleme.",
    "Cleaner standards, better reciprocity, and emotional clarity.": "Daha temiz standartlar, daha iyi karşılıklılık ve duygusal netlik.",
    "Inner steadiness, self-awareness, and better energetic boundaries.": "İçsel denge, öz farkındalık ve daha sağlıklı sınırlar.",
    "Better prioritization, cleaner value decisions, and resource discipline.": "Daha iyi önceliklendirme, daha temiz değer kararları ve kaynak disiplini.",
    "Inflation, drift, or chasing meaning without grounded follow-through.": "Abartı, dağılma veya somut takip olmadan anlam peşinde koşma.",
    "Pressure fatigue, over-control, or mistaking delay for failure.": "Baskı yorgunluğu, aşırı kontrol veya gecikmeyi başarısızlık sanma.",
    "Mixed signals, over-accommodation, or avoidable emotional repetition.": "Karışık sinyaller, aşırı uyumlanma veya önlenebilir duygusal tekrar.",
    "Withdrawal, overload, or losing clarity through internal noise.": "Geri çekilme, aşırı yüklenme veya iç gürültüyle netliği kaybetme.",
    "Leakage, reactive decisions, or comfort spending under pressure.": "Sızıntı, tepkisel kararlar veya baskı altında rahatlama harcamaları.",
}

_NARRATIVE_LABELS_TR = {
    "career_transition": "Kariyer geçişi",
    "relationship_transition": "İlişki geçişi",
    "financial_restructuring": "Finansal yeniden yapılanma",
    "identity_reinvention": "Kimlik yenilenmesi",
    "emotional_healing": "Duygusal iyileşme",
    "responsibility_cycle": "Sorumluluk döngüsü",
    "growth_opportunity": "Büyüme fırsatı",
    "life_redirection": "Yaşam yönünün yeniden belirlenmesi",
    "inner_transformation": "İçsel dönüşüm",
    "stability_building": "İstikrar inşası",
    "release_and_closure": "Bırakma ve kapanış",
    "expansion_period": "Genişleme dönemi",
    "pressure_test_phase": "Baskı ve sınav dönemi",
    "parent_child_guidance": "Ebeveyn-çocuk rehberliği",
    "child_growth": "Çocuğun gelişimi",
}

_PRIORITY_LABELS_TR = {
    "high": "Yüksek öncelik",
    "medium": "Orta öncelik",
    "low": "Düşük öncelik",
}

_RECOMMENDATION_TYPE_LABELS_TR = {
    "action": "Önerilen adım",
    "avoidance": "Şimdilik yavaşlat",
    "timing": "Zamanlama önemli",
    "focus": "Odak alanı",
}


def _localized_result_phrase(value, language):
    text = str(value or "")
    if language != "tr" or not text:
        return text
    text = _localize_signal_phrase(text, language)
    if " shaping " in text:
        lead, domain = text.split(" shaping ", 1)
        lead_map = {
            "concentration": "Odak",
            "flow": "Akış",
            "timing": "Zamanlama",
            "support": "Destek",
            "activation": "Aktivasyon",
            "nodes": "Ay Düğümleri",
        }
        domain_map = {
            "career": "Kariyeri",
            "growth": "Büyümeyi",
            "relationships": "İlişkileri",
            "relationships and money": "İlişkileri ve maddi akışı",
            "personal growth": "Kişisel gelişimi",
            "home": "Ev ve iç dengeyi",
            "finances": "Finansı",
            "money": "Finansı",
            "wealth": "Kaynak ve kazancı",
        }
        localized_lead = lead_map.get(lead.strip().lower(), _localized_result_label(lead, language))
        localized_domain = domain_map.get(domain.strip().lower(), _focus_label(domain, language))
        return f"{localized_domain} şekillendiren {localized_lead}"
    narrative_text = localize_narrative_text(text, language)
    if narrative_text != text:
        return narrative_text
    if text in _RESULT_PHRASE_LOCALIZATION_TR:
        return _RESULT_PHRASE_LOCALIZATION_TR[text]
    if ". This cluster lands most strongly across " in text:
        lead, cluster = text.split(". This cluster lands most strongly across ", 1)
        lead_text = localize_narrative_text(f"{lead}.", language)
        parts = [part.strip() for part in cluster.rstrip(".").split(" and ")]
        localized = [_RESULT_DOMAIN_LABELS_TR.get(part.replace(" ", "_"), part) for part in parts]
        return f"{lead_text} Bu küme en güçlü şekilde {' ve '.join(localized)} alanlarında görünür."
    if text.startswith("This cluster lands most strongly across "):
        domain_text = text.split(" across ", 1)[1].rstrip(".")
        parts = [part.strip() for part in domain_text.split(" and ")]
        localized = [_RESULT_DOMAIN_LABELS_TR.get(part.replace(" ", "_"), part) for part in parts]
        return f"Bu küme en güçlü şekilde {' ve '.join(localized)} alanlarında görünür."
    if text.startswith("This cluster concentrates the chart's strongest weight across "):
        domain_text = text.split(" across ", 1)[1].rstrip(".")
        parts = [part.strip() for part in domain_text.split(" and ")]
        localized = [_RESULT_DOMAIN_LABELS_TR.get(part.replace(" ", "_"), part) for part in parts]
        return f"Bu küme haritadaki en güçlü ağırlığı {' ve '.join(localized)} alanlarında toplar."
    if text.startswith("This anchor shapes decision quality, emotional orientation, and timing across "):
        domain_text = text.split(" across ", 1)[1].rstrip(".")
        parts = [part.strip() for part in domain_text.split(", ")]
        localized = [_RESULT_DOMAIN_LABELS_TR.get(part.replace(" ", "_"), part) for part in parts]
        return f"Bu odak, {' ve '.join(localized)} alanlarında karar kalitesini, duygusal yönelimi ve zamanlama hissini etkiler."
    if text.startswith("This anchor shapes decision quality, emotional orientation, and zamanlama across "):
        domain_text = text.split(" across ", 1)[1].rstrip(".")
        parts = [part.strip() for part in domain_text.split(", ")]
        localized = [_RESULT_DOMAIN_LABELS_TR.get(part.replace(" ", "_"), part) for part in parts]
        return f"Bu odak, {' ve '.join(localized)} alanlarında karar kalitesini, duygusal yönelimi ve zamanlama hissini etkiler."
    if text.startswith("Supporting emphasis ") and " around " in text:
        prefix, subject = text.split(" around ", 1)
        return f"Destekleyici vurgu {prefix.replace('Supporting emphasis ', '')}: {subject}"
    if text.startswith("during the current ") and text.endswith(" phase"):
        planet = text.replace("during the current ", "", 1).replace(" phase", "")
        return f"mevcut {planet} döneminde"
    if text.startswith("The chart is currently led by "):
        return _localize_signal_phrase(text, language)
    if text == "next 4-6 weeks":
        return "önümüzdeki 4-6 hafta"
    if text == "next 4-8 weeks":
        return "önümüzdeki 4-8 hafta"
    if text == "next 2-3 months":
        return "önümüzdeki 2-3 ay"
    if text.startswith("Current dasha emphasis is centered on ") and text.endswith(", so timing-sensitive guidance is weighted more heavily."):
        planet = text.replace("Current dasha emphasis is centered on ", "", 1).replace(", so timing-sensitive guidance is weighted more heavily.", "")
        return f"Mevcut dasha vurgusu {planet} üzerinde toplandığı için zamanlamaya duyarlı rehberlik daha fazla ağırlık taşır."
    return text


def _localized_result_label(value, language):
    if language != "tr":
        return _labelize(value)
    key = str(value or "").strip().lower().replace(" ", "_")
    return _NARRATIVE_LABELS_TR.get(key) or _focus_label(key, language) or _labelize(value)


def _localize_window_rows_for_result(rows, language):
    if language != "tr":
        return rows or []
    localized = []
    for row in rows or []:
        item = dict(row)
        item["title"] = _localized_result_phrase(item.get("title"), language)
        item["time_window"] = _localized_result_phrase(item.get("time_window"), language)
        localized.append(item)
    return localized


def _localize_result_layer_text(interpretation_context, language):
    if language != "tr" or not isinstance(interpretation_context, dict):
        return interpretation_context or {}

    context = copy.deepcopy(interpretation_context)
    signal_layer = context.get("signal_layer")
    if isinstance(signal_layer, dict):
        for anchor in signal_layer.get("top_anchors") or []:
            if not isinstance(anchor, dict):
                continue
            for field in ("title", "summary", "why_it_matters", "opportunity", "risk", "prompt_anchor"):
                if field in anchor:
                    anchor[field] = _localized_result_phrase(anchor.get(field), language)
            for signal in anchor.get("supporting_signals") or []:
                if isinstance(signal, dict) and "text" in signal:
                    signal["text"] = _localized_result_phrase(signal.get("text"), language)

        recommendation_layer = signal_layer.get("recommendation_layer")
        if isinstance(recommendation_layer, dict):
            _localize_recommendation_layer_for_result(recommendation_layer, language)
            context["recommendation_layer"] = recommendation_layer
        context["signal_layer"] = signal_layer

    recommendation_layer = context.get("recommendation_layer") or {}
    if isinstance(recommendation_layer, dict):
        _localize_recommendation_layer_for_result(recommendation_layer, language)
        context["recommendation_layer"] = recommendation_layer

    narrative_analysis = localize_narrative_analysis(context.get("narrative_analysis") or {}, language)
    if isinstance(narrative_analysis, dict):
        narrative_analysis["life_period_summary"] = _localized_result_phrase(narrative_analysis.get("life_period_summary"), language)
        for bucket in ("primary_narratives", "secondary_narratives", "emerging_narratives"):
            narratives = narrative_analysis.get(bucket) or []
            for narrative in narratives:
                if not isinstance(narrative, dict):
                    continue
                for field in ("narrative_summary", "narrative_psychological_meaning", "narrative_external_manifestation", "recommended_focus", "risk_factor", "growth_potential", "intensity"):
                    if field in narrative:
                        narrative[field] = _localized_result_phrase(narrative.get(field), language)
        context["narrative_analysis"] = narrative_analysis

    for narrative in context.get("dominant_narrative_details") or []:
        if not isinstance(narrative, dict):
            continue
        for field in ("narrative_summary", "narrative_psychological_meaning", "narrative_external_manifestation", "recommended_focus", "risk_factor", "growth_potential", "intensity"):
            if field in narrative:
                narrative[field] = _localized_result_phrase(narrative.get(field), language)

    for section_name in ("growth_guidance", "timing_notes"):
        section_value = context.get(section_name)
        if isinstance(section_value, dict):
            for key, value in list(section_value.items()):
                if isinstance(value, str):
                    section_value[key] = _localized_result_phrase(value, language)
        elif isinstance(section_value, list):
            for item in section_value:
                if isinstance(item, dict):
                    for key, value in list(item.items()):
                        if isinstance(value, str):
                            item[key] = _localized_result_phrase(value, language)

    return _localize_parent_child_interpretation_context(context, language)


def _localize_recommendation_layer_for_result(recommendation_layer, language):
    for item in recommendation_layer.get("top_recommendations") or []:
        if not isinstance(item, dict):
            continue
        for field in ("title", "reasoning", "time_window", "calibration_note"):
            if field in item:
                item[field] = _localized_result_phrase(item.get(field), language)
        item["priority_label"] = _PRIORITY_LABELS_TR.get(str(item.get("priority") or "").lower(), _localized_result_label(item.get("priority", "medium"), language))
        item["type_label"] = _RECOMMENDATION_TYPE_LABELS_TR.get(str(item.get("type") or "").lower(), _localized_result_label(item.get("type", "focus"), language))
        if item.get("linked_anchor_title"):
            item["linked_anchor_title"] = _localized_result_phrase(item.get("linked_anchor_title"), language)
        for anchor in item.get("linked_anchors") or []:
            if isinstance(anchor, dict):
                for field in ("title", "summary", "why_it_matters", "opportunity", "risk"):
                    if field in anchor:
                        anchor[field] = _localized_result_phrase(anchor.get(field), language)
    recommendation_layer["opportunity_windows"] = _localize_window_rows_for_result(
        recommendation_layer.get("opportunity_windows") or [],
        language,
    )
    recommendation_layer["risk_windows"] = _localize_window_rows_for_result(
        recommendation_layer.get("risk_windows") or [],
        language,
    )
    recommendation_layer["recommendation_notes"] = [
        _localized_result_phrase(note, language)
        for note in recommendation_layer.get("recommendation_notes") or []
    ]


def _localized_methodology_value(value, language):
    normalized = str(value or "").strip().lower()
    translation_key = {
        "sidereal": "pdf.methodology_value_sidereal",
        "true": "pdf.methodology_value_true",
        "false": "pdf.methodology_value_false",
        "whole_sign": "pdf.methodology_value_whole_sign",
    }.get(normalized)
    if translation_key:
        return translate_text(translation_key, language)
    return _labelize(value)


def normalize_report_type(report_type):
    normalized = str(report_type or "").strip().lower()
    return normalized if normalized in REPORT_TYPES else "premium"


def normalize_report_order_type(report_type):
    normalized = str(report_type or "").strip().lower().replace("-", "_")
    return normalized if normalized in REPORT_ORDER_PRODUCTS else None


def normalize_report_bundle_type(bundle_type):
    normalized = str(bundle_type or "").strip().lower().replace("-", "_")
    return normalized if normalized in REPORT_BUNDLE_PRODUCTS else None


def get_report_type_config(report_type):
    normalized = normalize_report_type(report_type)
    return normalized, dict(REPORT_TYPES[normalized])


def _decision_items(language, posture, primary_focus):
    focus = _focus_label(primary_focus, language)
    if language == "en":
        presets = {
            "act": {
                "do": [
                    f"Take visible steps in {focus.lower()}.",
                    "Use momentum, but keep execution structured.",
                    "Turn insight into commitment instead of waiting for perfect clarity.",
                ],
                "avoid": [
                    "Do not delay decisions unnecessarily.",
                    "Do not spread attention across too many fronts.",
                    "Avoid reactive commitments that dilute the main direction.",
                ],
            },
            "act_cautiously": {
                "do": [
                    f"Move forward in {focus.lower()} with clear pacing.",
                    "Break important decisions into smaller controlled stages.",
                    "Choose structure over speed.",
                ],
                "avoid": [
                    "Avoid emotional urgency.",
                    "Do not over-commit capacity.",
                    "Do not amplify risks you cannot control.",
                ],
            },
            "hold": {
                "do": [
                    "Stabilize the current structure before major moves.",
                    "Use this period for observation and preparation.",
                    "Protect long-term direction from short-term noise.",
                ],
                "avoid": [
                    "Avoid rushed commitments on unclear ground.",
                    "Do not force momentum where timing is not ready.",
                    "Do not let pressure rewrite your priorities.",
                ],
            },
            "avoid": {
                "do": [
                    "Reduce exposure in high-pressure areas.",
                    f"Stay deliberate in {focus.lower()} instead of reacting.",
                    "Preserve energy and prevent avoidable damage.",
                ],
                "avoid": [
                    "Avoid opening new crisis fronts.",
                    "Do not make major promises under stress.",
                    "Avoid emotional or financial overreaction.",
                ],
            },
            "prepare": {
                "do": [
                    "Use this period to prepare the next strong move.",
                    "Organize unfinished structures.",
                    "Collect signal before making firm commitments.",
                ],
                "avoid": [
                    "Avoid forcing certainty too early.",
                    "Do not pivot based on weak data.",
                    "Avoid rushing preparation-heavy choices.",
                ],
            },
        }
    else:
        presets = {
            "act": {
                "do": [
                    f"{focus} alanında görünür adımlar at.",
                    "İvme varsa bunu yapılı ve net şekilde kullan.",
                    "İçgörüyü uygulamaya geçir, fazla bekleme.",
                ],
                "avoid": [
                    "Gereksiz erteleme yapma.",
                    "Dikkatini çok fazla yöne dağıtma.",
                    "Tepkisel taahhütlerle ana yönünü sulandırma.",
                ],
            },
            "act_cautiously": {
                "do": [
                    f"{focus} alanında ölçülü ama net ilerle.",
                    "Önemli kararları kademeli uygula.",
                    "Hız yerine yapı ve ritmi seç.",
                ],
                "avoid": [
                    "Duygusal aciliyete kapılma.",
                    "Kapasiteni fazla yükleme.",
                    "Kontrol edemediğin riskleri büyütme.",
                ],
            },
            "hold": {
                "do": [
                    "Büyük hamlelerden önce mevcut yapını dengele.",
                    "Bu dönemi gözlem ve hazırlık için kullan.",
                    "Kısa vadeli gürültünün uzun vadeli yönünü bozmasına izin verme.",
                ],
                "avoid": [
                    "Belirsiz zeminde hızlı taahhüt verme.",
                    "Zamanlama hazır değilken zorla ivme yaratma.",
                    "Baskının önceliklerini değiştirmesine izin verme.",
                ],
            },
            "avoid": {
                "do": [
                    "Yüksek baskı alanlarında maruziyeti azalt.",
                    f"{focus} alanında tepkisel değil planlı davran.",
                    "Enerjiyi koru ve önlenebilir hasarı azalt.",
                ],
                "avoid": [
                    "Yeni kriz alanları açma.",
                    "Stres altında büyük sözler verme.",
                    "Duygusal veya finansal aşırı tepki verme.",
                ],
            },
            "prepare": {
                "do": [
                    "Bir sonraki güçlü hamleyi hazırla.",
                    "Yarım kalan yapıları toparla.",
                    "Net taahhütlerden önce sinyal topla.",
                ],
                "avoid": [
                    "Kesinliği erken zorlamaya çalışma.",
                    "Zayıf veriyle yön değiştirme.",
                    "Hazırlık isteyen seçimlerde acele etme.",
                ],
            },
        }
    return presets.get(posture or "prepare", presets["prepare"])


def _split_ai_sections(text):
    sections = []
    current = {"title": "", "blocks": []}
    for line in str(text or "").splitlines():
        stripped = line.strip()
        if stripped.startswith("###"):
            if current["title"] or current["blocks"]:
                sections.append(current)
            current = {"title": stripped.replace("###", "", 1).strip(), "blocks": []}
            continue
        if not stripped:
            continue
        if stripped.startswith("- "):
            if current["blocks"] and current["blocks"][-1]["type"] == "list":
                current["blocks"][-1]["items"].append(stripped[2:].strip())
            else:
                current["blocks"].append({"type": "list", "items": [stripped[2:].strip()]})
        else:
            current["blocks"].append({"type": "paragraph", "text": stripped})
    if current["title"] or current["blocks"]:
        sections.append(current)
    return sections


def _filter_ai_sections_for_report(ai_sections, report_config):
    filtered_sections = []
    include_timing = report_config.get("include_timing", True)
    include_action_guidance = report_config.get("include_action_guidance", True)
    timing_markers = ("zamanlama", "timing", "peak", "window")
    guidance_markers = ("stratejik", "yonlendirme", "guidance", "recommend", "action", "yapmali", "riskler", "firsatlar")

    for section in ai_sections:
        title = str(section.get("title") or "").strip().lower()
        if not include_timing and any(marker in title for marker in timing_markers):
            continue
        if not include_action_guidance and any(marker in title for marker in guidance_markers):
            continue
        filtered_sections.append(section)
    return filtered_sections


def _filter_report_context(report_context):
    report_config = dict(report_context.get("report_type_config") or {})
    filtered = dict(report_context)
    filtered["show_pdf_download"] = bool(report_config.get("include_pdf"))
    filtered["show_scores"] = bool(report_config.get("include_scores"))
    filtered["show_lunations"] = bool(report_config.get("include_lunations"))
    filtered["show_timing"] = bool(report_config.get("include_timing"))
    filtered["show_action_guidance"] = bool(report_config.get("include_action_guidance"))
    filtered["ai_sections"] = _filter_ai_sections_for_report(report_context.get("ai_sections") or [], report_config)

    if not filtered["show_timing"]:
        filtered["peak_window"] = None
        filtered["opportunity_window"] = None
        filtered["pressure_window"] = None

    if not filtered["show_action_guidance"]:
        filtered["decision_items"] = {"do": [], "avoid": []}

    if not filtered["show_lunations"]:
        filtered["fullmoon_data"] = []

    return filtered


def _prepare_report_context(payload):
    payload = _attach_astro_signal_context(payload)
    language = str(payload.get("language", "tr")).lower()
    if language not in {"tr", "en"}:
        language = "tr"
    selected_report_type = astro_workspace.normalize_workspace_report_type(
        payload.get("workspace_report_type") or payload.get("report_order_type") or payload.get("report_type")
    )
    render_report_type = payload.get("render_report_type") or ("parent_child" if selected_report_type == "parent_child" else "premium")
    _, report_type_config = get_report_type_config(render_report_type)
    report_meta = astro_workspace.localized_workspace_report_meta(selected_report_type, language)
    interpretation_context = _localize_result_layer_text(payload.get("interpretation_context") or {}, language)
    primary_focus = interpretation_context.get("primary_focus") or interpretation_context.get("primary_life_focus") or "general"
    secondary_focus = interpretation_context.get("secondary_focus") or interpretation_context.get("secondary_life_focus") or "general"
    dominant_narratives = interpretation_context.get("dominant_narratives") or []
    dominant_life_areas = interpretation_context.get("dominant_life_areas") or []
    timing_windows = interpretation_context.get("top_timing_windows") or {}
    ai_interpretation = payload.get("ai_interpretation") or payload.get("interpretation") or payload.get("yorum")
    if not ai_interpretation:
        if selected_report_type == "parent_child":
            ai_interpretation = build_parent_child_ai_summary(interpretation_context, language=language)
        else:
            ai_interpretation = ai_logic.generate_interpretation(_attach_astro_signal_context(payload, report_type=selected_report_type))
    signal_layer = interpretation_context.get("signal_layer") or {}
    recommendation_layer = interpretation_context.get("recommendation_layer") or signal_layer.get("recommendation_layer") or {}
    calculation_config = payload.get("calculation_config") or {}
    parent_profile = payload.get("parent_profile") or interpretation_context.get("parent_profile") or {}
    child_profile_meta = payload.get("child_profile_meta") or interpretation_context.get("child_profile_meta") or {}
    report_structure = report_structure_v3.build_report_structure_v3(
        payload.get("astro_signal_context") or {},
        selected_report_type,
        language,
    )
    top_anchors = [
        {
            "rank": anchor.get("rank"),
            "title": anchor.get("title"),
            "summary": anchor.get("summary"),
            "why_it_matters": anchor.get("why_it_matters"),
            "opportunity": anchor.get("opportunity"),
            "risk": anchor.get("risk"),
        }
        for anchor in (signal_layer.get("top_anchors") or [])[:3]
    ]
    top_recommendations = [
        {
            "title": item.get("title"),
            "type_label": item.get("type_label") or _localized_result_label(item.get("type", "focus"), language),
            "time_window": item.get("time_window"),
            "reasoning": item.get("reasoning"),
            "priority_label": item.get("priority_label") or _localized_result_label(item.get("priority", "medium"), language),
            "linked_anchor_title": (item.get("linked_anchors") or [{}])[0].get("title"),
        }
        for item in (recommendation_layer.get("top_recommendations") or [])[:5]
    ]
    methodology_notes = [
        {"label": translate_text("pdf.methodology_zodiac", language), "value": _localized_methodology_value(calculation_config.get("zodiac", "sidereal"), language)},
        {"label": translate_text("pdf.methodology_ayanamsa", language), "value": _labelize(calculation_config.get("ayanamsa", "lahiri"))},
        {"label": translate_text("pdf.methodology_node_mode", language), "value": _localized_methodology_value(calculation_config.get("node_mode", "true"), language)},
        {"label": translate_text("pdf.methodology_house_system", language), "value": _localized_methodology_value(calculation_config.get("house_system", "whole_sign"), language)},
        {"label": translate_text("pdf.methodology_birth_place", language), "value": payload.get("normalized_birth_place") or payload.get("birth_city") or "-"},
        {"label": translate_text("pdf.methodology_engine_version", language), "value": calculation_config.get("engine_version") or ASTRO_ENGINE_VERSION},
    ]

    report_title = payload.get("report_title") or report_meta["title"]
    report_subtitle = payload.get("report_subtitle") or report_meta["subtitle"]
    decision_items = _decision_items(language, interpretation_context.get("decision_posture"), primary_focus)
    return {
        "language": language,
        "generated_at": datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M UTC"),
        "report_id": f"JY-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        "report_title": report_title,
        "report_subtitle": report_subtitle,
        "report_type": selected_report_type,
        "render_report_type": render_report_type,
        "report_type_config": report_type_config,
        "report_type_label": payload.get("report_type_label") or report_meta["label"],
        "access_notice": payload.get("access_notice"),
        "client_name": payload.get("full_name") or ("Private Client" if language == "en" else "Özel Profil"),
        "birth_summary": " | ".join(
            item for item in [
                payload.get("birth_date"),
                payload.get("birth_time"),
                payload.get("birth_city"),
            ] if item
        ),
        "interpretation_context": interpretation_context,
        "parent_profile": parent_profile,
        "child_profile_meta": child_profile_meta,
        "child_profile_report": interpretation_context.get("child_profile") or {},
        "relationship_dynamics_report": interpretation_context.get("relationship_dynamics") or {},
        "school_guidance_report": interpretation_context.get("school_guidance") or {},
        "parenting_guidance_report": interpretation_context.get("parenting_guidance") or {},
        "watch_areas_report": interpretation_context.get("watch_areas") or [],
        "growth_guidance_report": interpretation_context.get("growth_guidance") or {},
        "timing_notes_report": interpretation_context.get("timing_notes") or [],
        "signal_layer": signal_layer,
        "report_structure_v3": report_structure,
        "report_v3_titles": report_structure.get("section_titles") or {},
        "top_anchors": top_anchors,
        "recommendation_layer": recommendation_layer,
        "top_recommendations": top_recommendations,
        "opportunity_windows_report": recommendation_layer.get("opportunity_windows") or [],
        "risk_windows_report": recommendation_layer.get("risk_windows") or [],
        "methodology_notes": methodology_notes,
        "primary_focus_label": _focus_label(primary_focus, language),
        "secondary_focus_label": _focus_label(secondary_focus, language),
        "confidence_label": _localized_result_label(interpretation_context.get("confidence_level", "moderate"), language),
        "decision_posture_label": _localized_result_label(interpretation_context.get("decision_posture", "prepare"), language),
        "timing_strategy_label": _localized_result_label(interpretation_context.get("timing_strategy", "mixed"), language),
        "dominant_narrative_label": _localized_result_label(dominant_narratives[0], language) if dominant_narratives else ("Current life cycle" if language == "en" else "Mevcut yaşam döngüsü"),
        "dominant_life_area_label": _focus_label(dominant_life_areas[0], language) if dominant_life_areas else _focus_label(primary_focus, language),
        "peak_window": timing_windows.get("peak"),
        "opportunity_window": timing_windows.get("opportunity"),
        "pressure_window": timing_windows.get("pressure"),
        "ai_interpretation": ai_interpretation,
        "ai_sections": _split_ai_sections(ai_interpretation),
        "decision_items": decision_items,
        "natal_data": payload.get("natal_data") or {},
        "parent_natal_data": payload.get("parent_natal_data") or {},
        "parent_dasha_data": payload.get("parent_dasha_data") or [],
        "dasha_data": payload.get("dasha_data") or [],
        "navamsa_data": payload.get("navamsa_data") or {},
        "transit_data": payload.get("transit_data") or [],
        "eclipse_data": payload.get("eclipse_data") or [],
        "fullmoon_data": payload.get("fullmoon_data") or [],
    }


def _render_report_preview_context(request, payload_data, report=None, current_user=None, unlock_success=False):
    payload_data = _serialize_temporal_values(payload_data)
    ai_status_note = None
    try:
        report_context = _prepare_report_context(payload_data)
    except (ai_logic.AIConfigurationError, ai_logic.AIServiceError):
        logger.exception("Report preview AI generation degraded")
        preview_language = str(payload_data.get("language", "tr")).lower()
        ai_status_note = (
            "AI interpretation was unavailable at preview time. The rest of the report is still based on the current engine output."
            if preview_language == "en"
            else "AI yorum preview aninda kullanilamadi. Raporun geri kalani mevcut engine ciktilariyla olusturuldu."
        )
        payload_data["ai_interpretation"] = (
            "### AI INTERPRETATION UNAVAILABLE\n\nPlease retry when the AI service is available."
            if preview_language == "en"
            else "### AI YORUMU KULLANILAMIYOR\n\nAI servis tekrar kullanilabilir oldugunda yeniden deneyin."
        )
        report_context = _prepare_report_context(payload_data)
    report_context = _filter_report_context(report_context)
    report_context = _apply_report_access_context(
        report_context,
        report,
        current_user=current_user,
        unlock_success=unlock_success,
    )
    report_context["request"] = request
    report_context["ai_status_note"] = ai_status_note
    report_context["payload_json"] = payload_data
    return report_context


def _ensure_pdf_runtime_environment():
    if sys.platform.startswith("win"):
        configure_windows_weasyprint_runtime()


def _sanitize_download_name(value, fallback="user", max_length=60):
    normalized = re.sub(r"[<>:\"/\\\\|?*]+", " ", str(value or ""))
    normalized = re.sub(r"\s+", "_", normalized.strip())
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "_", normalized).strip("._-")
    if not normalized:
        normalized = fallback
    return normalized[:max_length] or fallback


def _validate_pdf_bytes(pdf_bytes):
    return bool(pdf_bytes and len(pdf_bytes) > 4 and pdf_bytes.startswith(b"%PDF"))


def _pdf_logo_data_uri():
    logo_path = BASE_DIR / "static" / "focus-logo.png"
    if not logo_path.exists():
        return None
    encoded = base64.b64encode(logo_path.read_bytes()).decode("ascii")
    return f"data:image/png;base64,{encoded}"


def _generate_pdf_bytes_from_report(report_context):
    try:
        _ensure_pdf_runtime_environment()
        from weasyprint import HTML
    except Exception as exc:
        raise ai_logic.AIServiceError(
            "PDF export servisi bu ortamda henuz hazir degil. WeasyPrint icin gerekli sistem kutuphaneleri eksik."
        ) from exc

    template = templates.env.get_template("report_pdf.html")
    render_context = dict(report_context or {})
    render_context.setdefault("pdf_base_url", str(BASE_DIR))
    render_context.setdefault("logo_data_uri", _pdf_logo_data_uri())
    html_content = template.render(render_context)
    try:
        pdf_bytes = HTML(string=html_content, base_url=render_context["pdf_base_url"]).write_pdf()
    except Exception as exc:
        raise ai_logic.AIServiceError("PDF raporu render edilirken bir sistem hatasi olustu.") from exc
    if not _validate_pdf_bytes(pdf_bytes):
        raise ai_logic.AIServiceError("PDF raporu olusturuldu ancak cikti dogrulanamadi.")
    return pdf_bytes


def _auth_template_context(request, **extra):
    current_user = extra.get("current_user") or extra.get("dashboard_user") or getattr(request.state, "admin_user", None) or getattr(request.state, "current_user", None)
    flash = get_flash(request)
    context = {
        "request": request,
        "lang": getattr(request.state, "lang", "en"),
        "csrf_token": ensure_csrf_token(request),
        "current_user": current_user,
        "flash_success": extra.get("flash_success") or flash.get("flash_success") or extra.get("notice") or "",
        "flash_error": extra.get("flash_error") or flash.get("flash_error") or extra.get("error") or "",
        "unread_contact": extra.get("unread_contact", _unread_contact_count() if str(getattr(request, "url", "")).find("/admin") >= 0 else 0),
    }
    context.update(_launch_mode_flags())
    context.update(extra)
    return context


def _json_loads_safe(value, default):
    try:
        return json.loads(value or "null")
    except Exception:
        return default


templates.env.globals["json_loads_safe"] = _json_loads_safe


def _ai_quality_context(request, **extra):
    active_page = extra.pop("active_page", "training_dashboard")
    return _auth_template_context(
        request,
        dashboard_user=request.state.admin_user,
        active_page=active_page,
        **extra,
    )


def _knowledge_item_metadata(item):
    metadata = _json_loads_safe(getattr(item, "metadata_json", None), {}) or {}
    if not isinstance(metadata, dict):
        metadata = {}
    return metadata


def _csv_to_list(value):
    items = []
    for part in str(value or "").split(","):
        cleaned = str(part or "").strip()
        if cleaned and cleaned not in items:
            items.append(cleaned)
    return items


def _knowledge_review_body(metadata):
    blocks = []
    for label, key in [
        ("Classical View", "classical_view"),
        ("Modern Synthesis", "modern_synthesis"),
        ("Interpretation Logic", "interpretation_logic"),
        ("Strong Condition", "strong_condition"),
        ("Weak Condition", "weak_condition"),
        ("Risk Pattern", "risk_pattern"),
        ("Opportunity Pattern", "opportunity_pattern"),
        ("Dasha Activation", "dasha_activation"),
        ("Transit Activation", "transit_activation"),
        ("Safe Language Notes", "safe_language_notes"),
        ("What Not To Say", "what_not_to_say"),
        ("Premium Synthesis", "premium_synthesis_sentence"),
    ]:
        value = str(metadata.get(key) or "").strip()
        if value:
            blocks.append(f"{label}: {value}")
    return "\n\n".join(blocks).strip()


def _knowledge_review_rows(items):
    rows = []
    for item in items:
        metadata = _knowledge_item_metadata(item)
        rows.append(
            {
                "item": item,
                "metadata": metadata,
                "title": item.title,
                "category": metadata.get("category") or item.item_type or "-",
                "primary_entity": metadata.get("primary_entity") or "-",
                "source_title": metadata.get("source_title") or (item.source_document.title if item.source_document else "-"),
                "confidence_level": metadata.get("confidence_level") or "-",
                "sensitivity_level": metadata.get("sensitivity_level") or "-",
                "created_at": item.created_at,
            }
        )
    return rows


def _review_required_items(db):
    items = db.query(db_mod.KnowledgeItem).order_by(db_mod.KnowledgeItem.created_at.desc()).all()
    results = []
    for item in items:
        metadata = _knowledge_item_metadata(item)
        if str(item.status or "").strip().lower() == "review_required" or bool(metadata.get("review_required")):
            results.append(item)
    return results


def _knowledge_item_coverage_entities(item, metadata):
    return [
        str(value).lower()
        for value in (metadata.get("coverage_entities") or _json_loads_safe(item.coverage_entities_json, []))
        if str(value or "").strip()
    ]


def _apply_knowledge_review_status(db, item, status):
    metadata = _knowledge_item_metadata(item)
    normalized_status = str(status or "").strip().lower() or "review_required"
    metadata["review_required"] = normalized_status == "review_required"
    metadata["status"] = normalized_status
    knowledge_service.update_knowledge_item(
        db,
        item,
        title=item.title,
        body_text=_knowledge_review_body(metadata) or item.body_text,
        summary_text=metadata.get("premium_synthesis_sentence") or item.summary_text,
        entities=_knowledge_item_coverage_entities(item, metadata),
        metadata=metadata,
        status=normalized_status,
    )
    return item


def _is_auto_approve_blocked(item, metadata):
    title = str(getattr(item, "title", "") or "").strip().lower()
    noise_score = float(metadata.get("noise_score") or 0)
    if metadata.get("is_toc") is True:
        return True
    if metadata.get("is_index") is True:
        return True
    if noise_score >= 0.7:
        return True
    if any(marker in title for marker in ("contents", "bölümler", "bolumler", "index")):
        return True
    source_page_start = metadata.get("source_page_start")
    source_page_end = metadata.get("source_page_end")
    if source_page_start and source_page_end and int(source_page_start) <= 3:
        support_fields = [
            metadata.get("classical_view"),
            metadata.get("modern_synthesis"),
            metadata.get("interpretation_logic"),
            metadata.get("strong_condition"),
            metadata.get("opportunity_pattern"),
        ]
        if not any(str(value or "").strip() for value in support_fields):
            return True
    return False


def slugify_article_title(value):
    translated = str(value or "").translate(ARTICLE_SLUG_CHAR_MAP)
    normalized = normalize("NFKD", translated).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", normalized.lower()).strip("-")
    return slug or "article"


def _unique_article_slug(db, title, article_id=None):
    base_slug = slugify_article_title(title)
    slug = base_slug
    counter = 2
    while True:
        query = db.query(db_mod.Article).filter(db_mod.Article.slug == slug)
        if article_id:
            query = query.filter(db_mod.Article.id != article_id)
        if not query.first():
            return slug
        slug = f"{base_slug}-{counter}"
        counter += 1


def _article_category_meta(category_slug):
    normalized = str(category_slug or "").strip().lower()
    if normalized not in ARTICLE_CATEGORY_LABELS:
        return None
    return {"slug": normalized, "label": ARTICLE_CATEGORY_LABELS[normalized]}


def _repair_mojibake_text(value):
    text = str(value or "")
    if not any(marker in text for marker in ("Ã", "Ä", "Å", "â")):
        return text
    current = text
    for _ in range(4):
        try:
            repaired = current.encode("cp1252").decode("utf-8")
        except Exception:
            break
        if repaired == current:
            break
        current = repaired
        if not any(marker in current for marker in ("Ã", "Ä", "Å", "â")):
            break
    return current


def _seed_articles(db):
    changed = False
    legacy_articles = (
        db.query(db_mod.Article)
        .filter(db_mod.Article.title.in_(LEGACY_ARTICLE_SEED_TITLES), db_mod.Article.author_name == "Focus Astrology")
        .all()
    )
    for article in legacy_articles:
        if article.is_published:
            article.is_published = False
            changed = True

    for item in ARTICLE_SEED_CONTENT:
        slug = slugify_article_title(item["title"])
        legacy_titles = [title for title in item.get("legacy_titles", []) if title]
        article = (
            db.query(db_mod.Article)
            .filter(
                or_(
                    db_mod.Article.slug == slug,
                    db_mod.Article.title == item["title"],
                    db_mod.Article.title.in_(legacy_titles) if legacy_titles else False,
                )
            )
            .first()
        )
        if article:
            updated_fields = {
                "title": item["title"],
                "slug": slug,
                "category": item["category"],
                "excerpt": item["excerpt"],
                "body": item["body"],
                "author_name": item.get("author_name") or "Focus Astrology",
                "reading_time": item.get("reading_time") or 4,
                "language": item.get("language") or "tr",
                "is_published": True,
                "published_at": item.get("published_at") or datetime.utcnow(),
            }
            for field_name, field_value in updated_fields.items():
                if getattr(article, field_name) != field_value:
                    setattr(article, field_name, field_value)
                    changed = True
            continue
        db.add(
            db_mod.Article(
                title=item["title"],
                slug=_unique_article_slug(db, item["title"]),
                category=item["category"],
                excerpt=item["excerpt"],
                body=item["body"],
                is_published=True,
                published_at=item.get("published_at") or datetime.utcnow(),
                author_name=item.get("author_name") or "Focus Astrology",
                reading_time=item.get("reading_time") or 4,
                language=item.get("language") or "tr",
            )
        )
        changed = True
    if changed:
        db.commit()


def _localized_article_payload(article, language=None):
    requested_language = str(language or article.language or "tr").lower()
    localized = ARTICLE_LOCALIZED_CONTENT.get(article.slug, {}).get(requested_language, {})
    title = _repair_mojibake_text(localized.get("title") or article.title)
    excerpt = _repair_mojibake_text(localized.get("excerpt") or article.excerpt or "")
    body = _repair_mojibake_text(localized.get("body") or article.body or "")
    return {
        "title": title,
        "excerpt": excerpt,
        "body": body,
        "language": requested_language if localized else (article.language or requested_language or "en"),
    }


def _article_view(article, language=None):
    category = _article_category_meta(article.category) or {"slug": article.category, "label": _labelize(article.category)}
    published_at = article.published_at or article.created_at
    localized_payload = _localized_article_payload(article, language=language)
    body_paragraphs = [part.strip() for part in str(localized_payload["body"] or "").split("\n\n") if part.strip()]
    return {
        "id": article.id,
        "title": localized_payload["title"],
        "slug": article.slug,
        "category": category,
        "excerpt": localized_payload["excerpt"],
        "body": localized_payload["body"],
        "body_paragraphs": body_paragraphs,
        "cover_image": article.cover_image,
        "is_published": bool(article.is_published),
        "published_at": published_at.strftime("%Y-%m-%d") if published_at else None,
        "created_at": article.created_at.strftime("%Y-%m-%d") if article.created_at else None,
        "author_name": article.author_name or "Focus Astrology",
        "reading_time": int(article.reading_time or 4),
        "language": localized_payload["language"],
    }


def _published_articles_query(db, language=None):
    query = db.query(db_mod.Article).filter(
        db_mod.Article.is_published.is_(True),
        func.length(func.trim(func.coalesce(db_mod.Article.title, ""))) > 0,
        func.length(func.trim(func.coalesce(db_mod.Article.excerpt, ""))) > 0,
        func.length(func.trim(func.coalesce(db_mod.Article.body, ""))) > 0,
        db_mod.Article.title != "Premium Timing Note Updated",
        ~db_mod.Article.slug.like("premium-timing-note%"),
    )
    requested_language = str(language or "").strip().lower()
    if requested_language == "tr":
        query = query.filter(or_(db_mod.Article.language == "tr", db_mod.Article.language.is_(None), db_mod.Article.language == ""))
    elif requested_language == "en":
        localized_slugs = [
            slug
            for slug, localized in ARTICLE_LOCALIZED_CONTENT.items()
            if localized.get("en")
        ]
        query = query.filter(or_(db_mod.Article.language == "en", db_mod.Article.slug.in_(localized_slugs)))
    return query


def get_related_articles(db, article, limit=3, language=None):
    if not article:
        return []
    related = (
        _published_articles_query(db, language=language)
        .filter(db_mod.Article.category == article.category, db_mod.Article.id != article.id)
        .order_by(db_mod.Article.published_at.desc(), db_mod.Article.created_at.desc())
        .limit(limit)
        .all()
    )
    if len(related) < limit:
        seen_ids = {article.id, *[item.id for item in related]}
        fallback = (
            _published_articles_query(db, language=language)
            .filter(~db_mod.Article.id.in_(seen_ids))
            .order_by(db_mod.Article.published_at.desc(), db_mod.Article.created_at.desc())
            .limit(limit - len(related))
            .all()
        )
        related.extend(fallback)
    return [_article_view(item, language=language) for item in related[:limit]]


def get_latest_articles(db, limit=3, language=None):
    _seed_articles(db)
    items = (
        _published_articles_query(db, language=language)
        .order_by(db_mod.Article.published_at.desc(), db_mod.Article.created_at.desc())
        .limit(limit)
        .all()
    )
    return [_article_view(item, language=language) for item in items]


def get_homepage_latest_articles(db, limit=3, language=None):
    _seed_articles(db)
    items = (
        _published_articles_query(db, language=language)
        .filter(or_(db_mod.Article.published_at.is_(None), db_mod.Article.published_at <= datetime.utcnow()))
        .order_by(db_mod.Article.published_at.desc(), db_mod.Article.created_at.desc())
        .limit(limit)
        .all()
    )
    return [_article_view(item, language=language) for item in items]


def _match_related_articles_for_result(db, interpretation_context, language=None):
    _seed_articles(db)
    signal_layer = (interpretation_context or {}).get("signal_layer") or {}
    articles = [_article_view(item, language=language) for item in _published_articles_query(db, language=language).all()]
    matched = match_articles_to_result(
        signal_layer.get("prioritized_signals") or [],
        signal_layer.get("top_anchors") or [],
        signal_layer.get("domain_scores") or {},
        articles,
    )
    return matched[:3]


@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
async def index(request: Request, db: Session = Depends(get_db)):
    current_language = getattr(getattr(request, "state", None), "lang", None) or "en"
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context=base_context(
            request,
            db,
            latest_articles=get_homepage_latest_articles(db, limit=3, language=current_language),
        ),
    )


@app.api_route("/health", methods=["GET", "HEAD"])
async def health():
    return {"status": "ok"}


@app.get("/calculator", response_class=HTMLResponse)
async def calculator(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse(
        request=request,
        name="calculator.html",
        context=base_context(request, db),
    )


@app.get("/about", response_class=HTMLResponse)
async def about(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse(
        request=request,
        name="about.html",
        context=base_context(request, db),
    )


@app.get("/sss", response_class=HTMLResponse)
async def faq_page(request: Request, db: Session = Depends(get_db)):
    faq_items = (
        db.query(db_mod.FAQItem)
        .filter(db_mod.FAQItem.is_published.is_(True))
        .order_by(db_mod.FAQItem.category.asc(), db_mod.FAQItem.sort_order.asc(), db_mod.FAQItem.id.asc())
        .all()
    )
    faq_by_category = {}
    for item in faq_items:
        faq_by_category.setdefault(item.category, []).append(item)
    return templates.TemplateResponse(
        request=request,
        name="sss.html",
        context=base_context(request, db, faq_by_category=faq_by_category),
    )


@app.get("/contact", response_class=HTMLResponse)
async def contact_page(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse(
        request=request,
        name="contact.html",
        context=base_context(request, db, success=False, form_data={}),
    )


@app.post("/contact", response_class=HTMLResponse)
async def contact_submit(request: Request, db: Session = Depends(get_db)):
    form_data = await request.form()
    if not verify_csrf_token(request, form_data.get(CSRF_FORM_FIELD, "")):
        raise HTTPException(status_code=403, detail="Invalid CSRF token")

    name = str(form_data.get("name", "")).strip()
    email = str(form_data.get("email", "")).strip()
    subject = str(form_data.get("subject", "genel")).strip() or "genel"
    message = str(form_data.get("message", "")).strip()
    errors = []
    if len(name) < 2:
        errors.append("Ad en az 2 karakter olmalıdır.")
    if "@" not in email:
        errors.append("Geçerli bir e-posta girin.")
    if len(message) < 10:
        errors.append("Mesaj en az 10 karakter olmalıdır.")
    if errors:
        return templates.TemplateResponse(
            request=request,
            name="contact.html",
            status_code=400,
            context=base_context(request, db, error_message=" ".join(errors), form_data=form_data, success=False),
        )
    if not check_rate_limit(f"contact:{_client_ip(request)}", max_calls=5, window_seconds=3600):
        return templates.TemplateResponse(
            request=request,
            name="contact.html",
            status_code=429,
            context=base_context(request, db, error_message="Çok fazla mesaj gönderildi. Lütfen daha sonra tekrar deneyin.", form_data=form_data, success=False),
        )

    msg = db_mod.ContactMessage(name=name, email=email, subject=subject, message=message)
    db.add(msg)
    db.commit()
    html = render_email(
        "contact_autoreply.html",
        name=name,
        instagram_url=get_setting(db, "site_instagram_url", ""),
        astrologer_name=get_astrologer_name(db),
    )
    _send_logged_email(
        db,
        email_type="contact_autoreply",
        to_email=email,
        subject="MesajÃ„Â±nÃ„Â±z alÃ„Â±ndÃ„Â± ââ‚¬â€ Focus Astrology",
        html_body=html,
        event_type="contact_message",
        event_key=f"contact:{msg.id}",
    )
    print(f"[CONTACT] New message from {name} <{email}>: {message[:80]}")
    return templates.TemplateResponse(
        request=request,
        name="contact.html",
        context=base_context(request, db, success=True, form_data={}),
    )


@app.get("/privacy", response_class=HTMLResponse)
async def privacy(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="privacy.html",
        context=_auth_template_context(request),
    )


@app.get("/terms", response_class=HTMLResponse)
async def terms(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="terms.html",
        context=_auth_template_context(request),
    )


@app.get("/disclaimer", response_class=HTMLResponse)
async def disclaimer(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="disclaimer.html",
        context=_auth_template_context(request),
    )


@app.get("/sales-terms", response_class=HTMLResponse)
async def sales_terms(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="sales_terms.html",
        context=_auth_template_context(request),
    )


@app.get("/appointment-policy", response_class=HTMLResponse)
async def appointment_policy(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="appointment_policy.html",
        context=_auth_template_context(request),
    )


@app.get("/personal-consultation", response_class=HTMLResponse)
async def personal_consultation(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse(
        request=request,
        name="personal_consultation.html",
        context=base_context(request, db),
    )


@app.get("/personal-consultation/book")
async def personal_consultation_book(request: Request):
    if not launch_consultation_booking_enabled():
        return _coming_soon_template_response(request)
    calendly_url = str(os.getenv("CALENDLY_CONSULTATION_URL", "https://calendly.com/focusastrology/vedicastrologyreading60")).strip()
    if not calendly_url:
        calendly_url = "https://calendly.com/focusastrology/vedicastrologyreading60"
    email_config = email_utils.get_email_config()
    booking_contact_email = email_config.get("support_email") or email_config.get("from_address") or "hello@focusastrology.com"
    return templates.TemplateResponse(
        request=request,
        name="personal_consultation_book.html",
        context=_auth_template_context(
            request,
            calendly_url=calendly_url,
            booking_contact_email=booking_contact_email,
        ),
    )


@app.get("/checkout/consultation", response_class=HTMLResponse)
async def consultation_checkout_step(request: Request):
    if not launch_payments_enabled() or not launch_consultation_booking_enabled():
        return _coming_soon_template_response(request)
    notice = str(request.query_params.get("notice", "")).strip()
    return templates.TemplateResponse(
        request=request,
        name="service_checkout.html",
        context=_auth_template_context(
            request,
            order=None,
            product=CONSULTATION_PRODUCT,
            service_kind="consultation",
            payments_enabled=payments.payments_enabled(),
            provider_name=payments.payment_provider(),
            checkout_notice=notice,
            checkout_upsell=_checkout_upsell_context(service_kind="consultation"),
        ),
    )


def _consultation_checkout_context(request, order=None, notice=""):
    return _auth_template_context(
        request,
        order=order,
        product=CONSULTATION_PRODUCT,
        service_kind="consultation",
        payments_enabled=payments.payments_enabled(),
        provider_name=payments.payment_provider(),
        checkout_notice=notice,
        checkout_upsell=_checkout_upsell_context(service_kind="consultation"),
    )


@app.get("/consultation/continue", response_class=HTMLResponse)
async def consultation_continue(request: Request, db: Session = Depends(get_db)):
    if not launch_consultation_booking_enabled() or not launch_payments_enabled():
        return _coming_soon_template_response(request)
    def clean_query_value(*names):
        for name in names:
            value = str(request.query_params.get(name, "") or "").strip()
            if value:
                return unquote(value)
        return ""

    invitee_uri = clean_query_value("invitee_uri", "invitee")
    event_uri = clean_query_value("event_uri", "event")
    email = clean_query_value("email").lower()
    scheduled_start_raw = clean_query_value("event_start_time")
    scheduled_start = _parse_calendly_datetime(scheduled_start_raw) if scheduled_start_raw else None

    query = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.service_type == "consultation")
    order = None
    if invitee_uri:
        order = (
            query.filter(db_mod.ServiceOrder.calendly_invitee_uri == invitee_uri)
            .order_by(db_mod.ServiceOrder.created_at.desc())
            .first()
        )
    if not order and event_uri:
        order = (
            query.filter(db_mod.ServiceOrder.calendly_event_uri == event_uri)
            .order_by(db_mod.ServiceOrder.created_at.desc())
            .first()
        )
    if not order and email and scheduled_start:
        order = (
            query.filter(
                db_mod.ServiceOrder.customer_email == email,
                db_mod.ServiceOrder.scheduled_start == scheduled_start,
            )
            .order_by(db_mod.ServiceOrder.created_at.desc())
            .first()
        )
    if not order and email and scheduled_start:
        order = (
            query.filter(
                db_mod.ServiceOrder.customer_email == email,
                db_mod.ServiceOrder.scheduled_start.isnot(None),
            )
            .order_by(db_mod.ServiceOrder.created_at.desc())
            .first()
        )
    if not order:
        return templates.TemplateResponse(
            request=request,
            name="consultation_continue_not_found.html",
            status_code=404,
            context=_auth_template_context(request),
        )

    return RedirectResponse(url=f"/checkout/consultation/{order.order_token}", status_code=303)


def _start_consultation_payment_for_order(db, order, request):
    if order.service_type != "consultation":
        _public_error("Bu ödeme adımı danışmanlık içindir.", 404)
    if order.status in {"paid", "confirmed", "prepared", "completed"}:
        return RedirectResponse(url=f"/checkout/consultation/{order.order_token}?notice=already_paid", status_code=303)
    if order.status in {"booking_expired", "cancelled", "refunded", "no_show"}:
        return RedirectResponse(url=f"/checkout/consultation/{order.order_token}?notice=booking_inactive", status_code=303)
    order.status = "booking_pending_payment"
    try:
        session = create_consultation_payment_session(order, request=request)
    except payments.PaymentConfigurationError as exc:
        logger.warning("Consultation checkout unavailable order_id=%s detail=%s", order.id, exc)
        db.commit()
        return RedirectResponse(url=f"/checkout/consultation/{order.order_token}?notice=payments_unavailable", status_code=303)
    except payments.PaymentError as exc:
        logger.warning("Consultation checkout failed order_id=%s detail=%s", order.id, exc)
        db.commit()
        return RedirectResponse(url=f"/checkout/consultation/{order.order_token}?notice=checkout_failed", status_code=303)
    order.payment_provider = session.get("provider") or payments.payment_provider()
    order.provider_name = session.get("provider") or payments.payment_provider()
    order.provider_token = session.get("provider_token") or session.get("session_id")
    order.provider_conversation_id = session.get("provider_conversation_id") or _order_public_token(order)
    order.payment_session_id = session.get("session_id")
    db.commit()
    redirect_url = session.get("redirect_url")
    if redirect_url:
        return RedirectResponse(url=redirect_url, status_code=303)
    return RedirectResponse(url=f"/checkout/consultation/{order.order_token}?notice=missing_redirect", status_code=303)


@app.get("/checkout/consultation/{order_token}", response_class=HTMLResponse)
async def consultation_checkout_for_booking(request: Request, order_token: str, db: Session = Depends(get_db)):
    if not launch_payments_enabled() or not launch_consultation_booking_enabled():
        return _coming_soon_template_response(request)
    order = _service_order_by_token_or_404(db, order_token)
    if order.service_type != "consultation":
        _public_error("Bu ödeme adımı danışmanlık içindir.", 404)
    notice = str(request.query_params.get("notice", "")).strip()
    return templates.TemplateResponse(
        request=request,
        name="service_checkout.html",
        context=_consultation_checkout_context(request, order=order, notice=notice),
    )


@app.post("/checkout/consultation/{order_token}")
async def start_consultation_checkout_for_booking(request: Request, order_token: str, db: Session = Depends(get_db)):
    if not launch_payments_enabled() or not launch_consultation_booking_enabled():
        return _coming_soon_template_response(request)
    enforce_rate_limit(request, "consultation_checkout", limit=20, window_seconds=600)
    await validate_csrf_token(request)
    order = _service_order_by_token_or_404(db, order_token)
    return _start_consultation_payment_for_order(db, order, request)


@app.post("/checkout/consultation")
async def start_consultation_checkout(request: Request, db: Session = Depends(get_db)):
    if not launch_payments_enabled() or not launch_consultation_booking_enabled():
        return _coming_soon_template_response(request)
    enforce_rate_limit(request, "consultation_checkout", limit=20, window_seconds=600)
    await validate_csrf_token(request)
    order = db_mod.ServiceOrder(
        order_token=_generate_order_token("consult"),
        service_type="consultation",
        product_type=CONSULTATION_PRODUCT["product_type"],
        status="booking_pending_payment",
        public_token=None,
        amount=_amount_decimal(CONSULTATION_PRODUCT["price"]),
        amount_label=CONSULTATION_PRODUCT["price"],
        currency="TRY",
        payload_json=json.dumps(
            {
                "service_type": "consultation",
                "product_type": CONSULTATION_PRODUCT["product_type"],
                "submitted_at": datetime.now(pytz.UTC).isoformat(),
                "service_model": {
                    "duration": "60 dakika",
                    "sequence": "Calendly randevu seçimi sonrası iyzico ödeme adımı",
                    "cancellation": "Randevular, planlanan saatten en az 24 saat önce ücretsiz olarak iptal edilebilir veya yeniden planlanabilir.",
                },
            },
            ensure_ascii=False,
        ),
    )
    order.public_token = order.order_token
    db.add(order)
    db.commit()
    db.refresh(order)
    return _start_consultation_payment_for_order(db, order, request)


@app.get("/checkout/consultation/success")
async def consultation_checkout_success(order_token: str, session_id: str = "", db: Session = Depends(get_db)):
    order = _service_order_by_token_or_404(db, order_token)
    if order.service_type != "consultation":
        _public_error("Bu ödeme adımı danışmanlık içindir.", 404)
    return RedirectResponse(url="/checkout/consultation?notice=verification_pending", status_code=303)


@app.get("/checkout/consultation/cancel")
async def consultation_checkout_cancel(order_token: str = "", db: Session = Depends(get_db)):
    if order_token:
        order = _service_order_by_token_or_404(db, order_token)
        if order.service_type == "consultation":
            order.status = "booking_pending_payment"
            db.commit()
    return RedirectResponse(url="/checkout/consultation?notice=cancelled", status_code=303)


@app.get("/articles", response_class=HTMLResponse)
async def articles(request: Request, db: Session = Depends(get_db)):
    current_language = getattr(getattr(request, "state", None), "lang", None) or "en"
    articles_payload = get_latest_articles(db, limit=24, language=current_language)
    category_counts = {}
    for slug, label in ARTICLE_CATEGORY_LABELS.items():
        count = _published_articles_query(db, language=current_language).filter(db_mod.Article.category == slug).count()
        category_counts[slug] = {"slug": slug, "label": label, "count": count}
    return templates.TemplateResponse(
        request=request,
        name="articles.html",
        context=base_context(
            request,
            db,
            articles=articles_payload,
            active_category=None,
            category_links=list(category_counts.values()),
        ),
    )


@app.get("/articles/category/{category_slug}", response_class=HTMLResponse)
async def article_category(request: Request, category_slug: str, db: Session = Depends(get_db)):
    _seed_articles(db)
    current_language = getattr(getattr(request, "state", None), "lang", None) or "en"
    category = _article_category_meta(category_slug)
    if not category:
        _public_error("Kategori bulunamadi.", 404)
    items = (
        _published_articles_query(db, language=current_language)
        .filter(db_mod.Article.category == category["slug"])
        .order_by(db_mod.Article.published_at.desc(), db_mod.Article.created_at.desc())
        .all()
    )
    category_links = [
        {
            "slug": slug,
            "label": label,
            "count": _published_articles_query(db, language=current_language).filter(db_mod.Article.category == slug).count(),
        }
        for slug, label in ARTICLE_CATEGORY_LABELS.items()
    ]
    return templates.TemplateResponse(
        request=request,
        name="articles.html",
        context=_auth_template_context(
            request,
            articles=[_article_view(item, language=current_language) for item in items],
            active_category=category,
            category_links=category_links,
        ),
    )


@app.get("/articles/{slug}", response_class=HTMLResponse)
async def article_detail(request: Request, slug: str, db: Session = Depends(get_db)):
    _seed_articles(db)
    current_language = getattr(getattr(request, "state", None), "lang", None) or "en"
    article = _published_articles_query(db, language=current_language).filter(db_mod.Article.slug == slug).first()
    if not article:
        _public_error("Makale bulunamadi.", 404)
    article_payload = _article_view(article, language=current_language)
    return templates.TemplateResponse(
        request=request,
        name="article_detail.html",
        context=_auth_template_context(
            request,
            article=article_payload,
            related_articles=get_related_articles(db, article, limit=3, language=current_language),
        ),
    )


@app.get("/signup", response_class=HTMLResponse)
async def signup_page(request: Request):
    if request.state.current_user_id:
        return RedirectResponse(url="/dashboard", status_code=303)
    return templates.TemplateResponse(request=request, name="signup.html", context=_auth_template_context(request))


@app.post("/signup", response_class=HTMLResponse)
async def signup_submit(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    name: str = Form(default=""),
    db: Session = Depends(get_db),
):
    if not check_rate_limit(f"signup:{_client_ip(request)}", max_calls=5, window_seconds=3600):
        return templates.TemplateResponse(
            request=request,
            name="signup.html",
            context=_auth_template_context(request, error_message="Çok fazla kayıt denemesi. Lütfen daha sonra tekrar deneyin."),
            status_code=429,
        )
    normalized_email = str(email or "").strip().lower()
    clean_name = str(name or "").strip()
    errors = []
    if len(clean_name) < 2:
        errors.append("Ad en az 2 karakter olmalıdır.")
    if "@" not in normalized_email:
        errors.append("Geçerli bir e-posta adresi girin.")
    if len(password or "") < 8:
        errors.append("Şifre en az 8 karakter olmalıdır.")
    if errors:
        return templates.TemplateResponse(
            request=request,
            name="signup.html",
            context=_auth_template_context(
                request,
                error_message=" ".join(errors),
                form_data={"email": normalized_email, "name": clean_name},
            ),
            status_code=400,
        )

    existing = db.query(db_mod.AppUser).filter(db_mod.AppUser.email == normalized_email).first()
    if existing:
        logger.info("Signup failed duplicate email=%s", normalized_email)
        return templates.TemplateResponse(
            request=request,
            name="signup.html",
            context=_auth_template_context(
                request,
                error_message="Bu e-posta zaten kayitli. Giris yapmayi deneyin.",
                form_data={"email": normalized_email, "name": name},
            ),
            status_code=400,
        )

    user = db_mod.AppUser(
        email=normalized_email,
        password_hash=generate_password_hash(password),
        name=(name or "").strip() or normalized_email.split("@")[0],
        plan_code="free",
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    request.session["user_id"] = user.id
    logger.info("Signup succeeded email=%s user_id=%s", normalized_email, user.id)
    maybe_send_welcome_email(db, user)
    return RedirectResponse(url="/dashboard", status_code=303)


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if request.state.current_user_id:
        next_path = _safe_next_path(request.query_params.get("next"), default="/dashboard")
        current_user = getattr(request.state, "current_user", None)
        if next_path.startswith("/admin") and not is_admin_user(current_user):
            next_path = "/dashboard"
        return RedirectResponse(url=next_path, status_code=303)
    session_token_before = request.session.get(CSRF_SESSION_KEY, "") if "session" in getattr(request, "scope", {}) else ""
    csrf_token = ensure_csrf_token(request)
    session_has_csrf = bool(request.session.get(CSRF_SESSION_KEY, "")) if "session" in getattr(request, "scope", {}) else False
    next_path = _safe_next_path(request.query_params.get("next"), default="")
    logger.info(
        "Login page rendered csrf_generated=%s session_has_csrf=%s next_path=%s",
        bool(csrf_token and not session_token_before),
        session_has_csrf,
        next_path or "-",
    )
    return templates.TemplateResponse(
        request=request,
        name="login.html",
        context=_auth_template_context(
            request,
            csrf_token=csrf_token,
            next_path=next_path,
        ),
    )


@app.post("/login", response_class=HTMLResponse)
async def login_submit(request: Request):
    print("login-handler-version=csrf-v4-raw-entered", flush=True)
    try:
        raw_body = await request.body()
        content_type = str(request.headers.get("content-type", "") or "").lower()
        parsed_form = {}
        if "application/x-www-form-urlencoded" in content_type or not content_type:
            parsed_form = parse_qs(raw_body.decode("utf-8", errors="ignore"), keep_blank_values=True)
    except Exception as exc:
        if "session" in getattr(request, "scope", {}):
            request.session.pop(CSRF_SESSION_KEY, None)
        refreshed_csrf_token = ensure_csrf_token(request)
        logger.warning(
            "Login failed email=%s admin_email_match=%s password_configured=%s reason=raw_body_parse_failed error_type=%s",
            "-",
            False,
            _admin_password_configured(),
            type(exc).__name__,
        )
        return templates.TemplateResponse(
            request=request,
            name="login.html",
            context=_auth_template_context(
                request,
                csrf_token=refreshed_csrf_token,
                error_message=LOGIN_CSRF_ERROR_MESSAGE,
                form_data={},
                next_path=_safe_next_path(request.query_params.get("next"), default="/dashboard"),
            ),
            status_code=403,
        )

    def _raw_form_value(name, default=""):
        values = parsed_form.get(name) or []
        return str(values[0] if values else default)

    email = _raw_form_value("email")
    password = _raw_form_value("password")
    csrf_token = _raw_form_value(CSRF_FORM_FIELD)
    next_path = _raw_form_value("next_path", "/")
    normalized_email = str(email or "").strip().lower()
    admin_email_match = normalized_email in _configured_admin_emails()
    admin_email_configured = bool(_configured_admin_emails())
    password_configured = _admin_password_configured()
    submitted_next_path = str(next_path or "").strip()
    next_candidate = submitted_next_path if submitted_next_path not in {"", "/"} else request.query_params.get("next")
    safe_next = _safe_next_path(next_candidate, default="/dashboard")
    submitted_csrf = str(csrf_token or "").strip()
    session_csrf = str(request.session.get(CSRF_SESSION_KEY, "") or "") if "session" in getattr(request, "scope", {}) else ""
    session_has_csrf = bool(session_csrf)
    logger.info(
        "Login POST received csrf_present=%s csrf_value_length=%s session_has_csrf=%s session_csrf_length=%s attempted_email=%s next_path=%s admin_email_configured=%s password_configured=%s admin_email_match=%s content_type=%s body_length=%s",
        bool(submitted_csrf),
        len(submitted_csrf),
        session_has_csrf,
        len(session_csrf),
        normalized_email,
        safe_next or "-",
        admin_email_configured,
        password_configured,
        admin_email_match,
        content_type or "-",
        len(raw_body),
    )
    if not verify_csrf_token(request, submitted_csrf):
        if "session" in getattr(request, "scope", {}):
            request.session.pop(CSRF_SESSION_KEY, None)
        refreshed_csrf_token = ensure_csrf_token(request)
        logger.warning(
            "Login failed email=%s admin_email_match=%s password_configured=%s reason=csrf_invalid",
            normalized_email,
            admin_email_match,
            password_configured,
        )
        return templates.TemplateResponse(
            request=request,
            name="login.html",
            context=_auth_template_context(
                request,
                csrf_token=refreshed_csrf_token,
                error_message=LOGIN_CSRF_ERROR_MESSAGE,
                form_data={"email": normalized_email},
                next_path=safe_next,
            ),
            status_code=403,
        )

    if not check_rate_limit(f"login:{_client_ip(request)}", max_calls=10, window_seconds=300):
        logger.warning(
            "Login failed email=%s admin_email_match=%s password_configured=%s reason=rate_limited",
            normalized_email,
            admin_email_match,
            password_configured,
        )
        return templates.TemplateResponse(
            request=request,
            name="login.html",
            context=_auth_template_context(
                request,
                error_message="Çok fazla giriş denemesi. Lütfen 5 dakika bekleyin.",
                form_data={"email": normalized_email},
                next_path=safe_next,
            ),
            status_code=429,
        )
    db = db_mod.SessionLocal()
    try:
        user = db.query(db_mod.AppUser).filter(db_mod.AppUser.email == normalized_email, db_mod.AppUser.is_active.is_(True)).first()
        if not user or not check_password_hash(user.password_hash, password):
            logger.info(
                "Login failed email=%s admin_email_match=%s password_configured=%s reason=invalid_credentials",
                normalized_email,
                admin_email_match,
                password_configured,
            )
            return templates.TemplateResponse(
                request=request,
                name="login.html",
                context=_auth_template_context(
                    request,
                    error_message="E-posta veya şifre hatalı.",
                    form_data={"email": normalized_email},
                    next_path=safe_next,
                ),
                status_code=400,
            )

        request.session["user_id"] = user.id
        logger.info(
            "Login succeeded email=%s user_id=%s admin_email_match=%s password_configured=%s reason=success",
            normalized_email,
            user.id,
            admin_email_match,
            password_configured,
        )
        redirect_target = safe_next
        if redirect_target.startswith("/admin") and not is_admin_user(user):
            redirect_target = "/dashboard"
        return RedirectResponse(url=redirect_target, status_code=303)
    finally:
        db.close()


@app.get("/debug/version")
async def debug_version():
    return JSONResponse({"app_version": APP_VERSION, "login_template_version": LOGIN_TEMPLATE_VERSION})


@app.get("/logout")
async def logout(request: Request):
    if hasattr(request, "session"):
        request.session.clear()
    return RedirectResponse(url="/", status_code=303)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, db: Session = Depends(get_db)):
    user = _require_authenticated_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)

    reports_query = db.query(db_mod.GeneratedReport).filter(db_mod.GeneratedReport.user_id == user.id)
    profiles_query = db.query(db_mod.UserProfile).filter(db_mod.UserProfile.user_id == user.id)
    recent_reports = [_report_view(report) for report in reports_query.order_by(db_mod.GeneratedReport.created_at.desc()).limit(5).all()]
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context=_auth_template_context(
            request,
            dashboard_user=_public_user_view(user),
            plan_features=get_plan_features(user),
            profile_count=profiles_query.count(),
            report_count=reports_query.count(),
            recent_reports=recent_reports,
        ),
    )


@app.get("/reports", response_class=HTMLResponse)
async def reports_history(request: Request, db: Session = Depends(get_db)):
    raw_user = request.state.current_user
    user = None
    if raw_user:
        user_id = raw_user.get("id") if isinstance(raw_user, dict) else _user_id(raw_user)
        if user_id:
            user = db.query(db_mod.AppUser).filter(db_mod.AppUser.id == user_id).first()
    reports = []
    if user:
        reports = [
            _report_history_item(db, report)
            for report in db.query(db_mod.GeneratedReport).filter(
                db_mod.GeneratedReport.user_id == user.id
            ).order_by(db_mod.GeneratedReport.created_at.desc()).all()
        ]
    return templates.TemplateResponse(
        request=request,
        name="reports.html",
        context=base_context(
            request,
            db,
            dashboard_user=_public_user_view(user) if user else None,
            reports=reports,
            plan_features=get_plan_features(user) if user else PLAN_FEATURES["free"],
        ),
    )


@app.get("/reports/order/{report_type}", response_class=HTMLResponse)
async def report_order_form(request: Request, report_type: str):
    if not launch_payments_enabled():
        return _coming_soon_template_response(request)
    normalized = normalize_report_order_type(report_type)
    if not normalized:
        _public_error("Rapor türü bulunamadı.", 404)
    if normalized == "parent_child":
        return RedirectResponse(url="/reports/parent-child", status_code=303)
    product = dict(REPORT_ORDER_PRODUCTS[normalized])
    return templates.TemplateResponse(
        request=request,
        name="report_order.html",
        context=_auth_template_context(
            request,
            product=product,
            report_type=normalized,
            order_action=f"/reports/order/{normalized}",
            bundle_type="",
            form_data={},
            error_message="",
        ),
    )


@app.post("/reports/order/{report_type}", response_class=HTMLResponse)
async def submit_report_order(
    request: Request,
    report_type: str,
    full_name: str = Form(default=""),
    email: str = Form(default=""),
    birth_date: str = Form(default=""),
    birth_time: str = Form(default=""),
    birth_city: str = Form(default=""),
    selected_report_type: str = Form(default=""),
    optional_note: str = Form(default=""),
    is_gift: str = Form(default=""),
    gift_recipient_name: str = Form(default=""),
    gift_recipient_email: str = Form(default=""),
    csrf_token: str = Form(default=""),
    db: Session = Depends(get_db),
):
    if not launch_payments_enabled():
        return _coming_soon_template_response(request)
    enforce_rate_limit(request, "report_order", limit=12, window_seconds=600)
    await validate_csrf_token(request, csrf_token)
    normalized = normalize_report_order_type(selected_report_type or report_type)
    if not normalized:
        _public_error("Rapor türü bulunamadı.", 404)
    if normalized == "parent_child":
        return RedirectResponse(url="/reports/parent-child", status_code=303)

    product = dict(REPORT_ORDER_PRODUCTS[normalized])
    if not check_rate_limit(f"order:{_client_ip(request)}", max_calls=5, window_seconds=3600):
        return templates.TemplateResponse(
            request=request,
            name="report_order.html",
            status_code=429,
            context=_auth_template_context(
                request,
                product=product,
                report_type=normalized,
                order_action=f"/reports/order/{normalized}",
                bundle_type="",
                form_data={},
                error_message="Çok fazla talep gönderildi. Lütfen daha sonra tekrar deneyin.",
            ),
        )
    form_data = {
        "full_name": full_name.strip(),
        "email": email.strip(),
        "birth_date": birth_date.strip(),
        "birth_time": birth_time.strip(),
        "birth_city": birth_city.strip(),
        "optional_note": optional_note.strip(),
        "is_gift": bool(is_gift),
        "gift_recipient_name": gift_recipient_name.strip(),
        "gift_recipient_email": gift_recipient_email.strip(),
    }
    validation_errors = []
    if len(form_data["full_name"]) < 2:
        validation_errors.append("Ad soyad en az 2 karakter olmalıdır.")
    if "@" not in form_data["email"] or "." not in form_data["email"].split("@")[-1]:
        validation_errors.append("Geçerli bir e-posta adresi girin.")
    if not form_data["birth_date"]:
        validation_errors.append("Doğum tarihi zorunludur.")
    if not form_data["birth_city"] or len(form_data["birth_city"]) < 2:
        validation_errors.append("Doğum yeri zorunludur.")
    if validation_errors:
        return templates.TemplateResponse(
            request=request,
            name="report_order.html",
            status_code=400,
            context=_auth_template_context(
                request,
                product=product,
                report_type=normalized,
                order_action=f"/reports/order/{normalized}",
                bundle_type="",
                form_data=form_data,
                error_message=" ".join(validation_errors),
            ),
        )
    required_missing = [
        label for key, label in (
            ("full_name", "Ad soyad"),
            ("email", "E-posta"),
            ("birth_date", "Doğum tarihi"),
            ("birth_time", "Doğum saati"),
            ("birth_city", "Doğum yeri"),
        )
        if not form_data[key]
    ]
    if required_missing:
        return templates.TemplateResponse(
            request=request,
            name="report_order.html",
            status_code=400,
            context=_auth_template_context(
                request,
                product=product,
                report_type=normalized,
                order_action=f"/reports/order/{normalized}",
                bundle_type="",
                form_data=form_data,
                error_message="Lütfen zorunlu alanları tamamlayın: " + ", ".join(required_missing),
            ),
        )

    submitted_at = datetime.now(pytz.UTC).isoformat()
    user_lang = str(getattr(request.state, "lang", "tr") or "tr").lower()
    user_lang = user_lang if user_lang in {"tr", "en"} else "tr"
    order_data = {
        **form_data,
        "report_type": normalized,
        "report_title": product["title"],
        "user_lang": user_lang,
        "submitted_at": submitted_at,
        "source": "reports_order_form",
    }
    order = db_mod.ServiceOrder(
        order_token=_generate_order_token("report"),
        service_type="report",
        product_type=normalized,
        status="awaiting_payment",
        public_token=None,
        customer_name=order_data["full_name"],
        customer_email=order_data["email"],
        birth_date=order_data["birth_date"],
        birth_time=order_data["birth_time"],
        birth_place=order_data["birth_city"],
        user_lang=order_data["user_lang"],
        optional_note=order_data["optional_note"],
        is_gift=order_data["is_gift"],
        gift_recipient_name=order_data["gift_recipient_name"] or None,
        gift_recipient_email=order_data["gift_recipient_email"] or None,
        amount=_amount_decimal(product["price"]),
        amount_label=product["price"],
        currency="TRY",
        payload_json=json.dumps(order_data, ensure_ascii=False),
    )
    order.public_token = order.order_token
    db.add(order)
    db.commit()
    db.refresh(order)
    maybe_send_report_order_confirmation_email(db, order)

    return RedirectResponse(url=f"/checkout/report/{order.order_token}", status_code=303)


@app.get("/reports/order/bundle/{bundle_type}", response_class=HTMLResponse)
async def report_bundle_order_form(request: Request, bundle_type: str):
    if not launch_payments_enabled():
        return _coming_soon_template_response(request)
    normalized = normalize_report_bundle_type(bundle_type)
    if not normalized:
        _public_error("Paket türü bulunamadı.", 404)
    product = dict(REPORT_BUNDLE_PRODUCTS[normalized])
    return templates.TemplateResponse(
        request=request,
        name="report_order.html",
        context=_auth_template_context(
            request,
            product=product,
            report_type=normalized,
            bundle_type=normalized,
            order_action=f"/reports/order/bundle/{normalized}",
            form_data={},
            error_message="",
        ),
    )


@app.post("/reports/order/bundle/{bundle_type}", response_class=HTMLResponse)
async def submit_report_bundle_order(
    request: Request,
    bundle_type: str,
    full_name: str = Form(default=""),
    email: str = Form(default=""),
    birth_date: str = Form(default=""),
    birth_time: str = Form(default=""),
    birth_city: str = Form(default=""),
    optional_note: str = Form(default=""),
    csrf_token: str = Form(default=""),
    db: Session = Depends(get_db),
):
    if not launch_payments_enabled():
        return _coming_soon_template_response(request)
    enforce_rate_limit(request, "report_order", limit=12, window_seconds=600)
    await validate_csrf_token(request, csrf_token)
    normalized = normalize_report_bundle_type(bundle_type)
    if not normalized:
        _public_error("Paket türü bulunamadı.", 404)
    product = dict(REPORT_BUNDLE_PRODUCTS[normalized])
    if not check_rate_limit(f"order:{_client_ip(request)}", max_calls=5, window_seconds=3600):
        return templates.TemplateResponse(
            request=request,
            name="report_order.html",
            status_code=429,
            context=_auth_template_context(
                request,
                product=product,
                report_type=normalized,
                bundle_type=normalized,
                order_action=f"/reports/order/bundle/{normalized}",
                form_data={},
                error_message="Çok fazla talep gönderildi. Lütfen daha sonra tekrar deneyin.",
            ),
        )
    form_data = {
        "full_name": full_name.strip(),
        "email": email.strip(),
        "birth_date": birth_date.strip(),
        "birth_time": birth_time.strip(),
        "birth_city": birth_city.strip(),
        "optional_note": optional_note.strip(),
    }
    validation_errors = []
    if len(form_data["full_name"]) < 2:
        validation_errors.append("Ad soyad en az 2 karakter olmalıdır.")
    if "@" not in form_data["email"] or "." not in form_data["email"].split("@")[-1]:
        validation_errors.append("Geçerli bir e-posta adresi girin.")
    if not form_data["birth_date"]:
        validation_errors.append("Doğum tarihi zorunludur.")
    if not form_data["birth_city"] or len(form_data["birth_city"]) < 2:
        validation_errors.append("Doğum yeri zorunludur.")
    if validation_errors:
        return templates.TemplateResponse(
            request=request,
            name="report_order.html",
            status_code=400,
            context=_auth_template_context(
                request,
                product=product,
                report_type=normalized,
                bundle_type=normalized,
                order_action=f"/reports/order/bundle/{normalized}",
                form_data=form_data,
                error_message=" ".join(validation_errors),
            ),
        )
    required_missing = [
        label for key, label in (
            ("full_name", "Ad soyad"),
            ("email", "E-posta"),
            ("birth_date", "Doğum tarihi"),
            ("birth_time", "Doğum saati"),
            ("birth_city", "Doğum yeri"),
        )
        if not form_data[key]
    ]
    if required_missing:
        return templates.TemplateResponse(
            request=request,
            name="report_order.html",
            status_code=400,
            context=_auth_template_context(
                request,
                product=product,
                report_type=normalized,
                bundle_type=normalized,
                order_action=f"/reports/order/bundle/{normalized}",
                form_data=form_data,
                error_message="Lütfen zorunlu alanları tamamlayın: " + ", ".join(required_missing),
            ),
        )

    submitted_at = datetime.now(pytz.UTC).isoformat()
    user_lang = str(getattr(request.state, "lang", "tr") or "tr").lower()
    user_lang = user_lang if user_lang in {"tr", "en"} else "tr"
    included_products = product.get("included_products", [])
    order_data = {
        **form_data,
        "report_type": normalized,
        "bundle_type": normalized,
        "included_products": included_products,
        "report_title": product["title"],
        "user_lang": user_lang,
        "submitted_at": submitted_at,
        "source": "reports_bundle_order_form",
    }
    order = db_mod.ServiceOrder(
        order_token=_generate_order_token("bundle"),
        service_type="report",
        product_type=normalized,
        bundle_type=normalized,
        included_products_json=json.dumps(included_products, ensure_ascii=False),
        bundle_price=_amount_decimal(product["price"]),
        status="awaiting_payment",
        public_token=None,
        customer_name=order_data["full_name"],
        customer_email=order_data["email"],
        birth_date=order_data["birth_date"],
        birth_time=order_data["birth_time"],
        birth_place=order_data["birth_city"],
        user_lang=order_data["user_lang"],
        optional_note=order_data["optional_note"],
        amount=_amount_decimal(product["price"]),
        amount_label=product["price"],
        currency="TRY",
        payload_json=json.dumps(order_data, ensure_ascii=False),
    )
    order.public_token = order.order_token
    db.add(order)
    db.commit()
    db.refresh(order)
    maybe_send_report_order_confirmation_email(db, order)

    return RedirectResponse(url=f"/checkout/report/{order.order_token}", status_code=303)


@app.get("/checkout/report/{order_token}", response_class=HTMLResponse)
async def report_checkout_step(request: Request, order_token: str, db: Session = Depends(get_db)):
    if not launch_payments_enabled():
        return _coming_soon_template_response(request)
    order = _service_order_by_token_or_404(db, order_token)
    if order.service_type != "report":
        _public_error("Bu ödeme adımı rapor siparişleri içindir.", 404)
    product = _service_order_product(order)
    notice = str(request.query_params.get("notice", "")).strip()
    return templates.TemplateResponse(
        request=request,
        name="service_checkout.html",
        context=_auth_template_context(
            request,
            order=order,
            product=product,
            service_kind="report",
            payments_enabled=payments.payments_enabled(),
            provider_name=payments.payment_provider(),
            checkout_notice=notice,
            checkout_upsell=_checkout_upsell_context(order),
        ),
    )


@app.post("/checkout/report/{order_token}")
async def start_report_checkout(request: Request, order_token: str, db: Session = Depends(get_db)):
    if not launch_payments_enabled():
        return _coming_soon_template_response(request)
    enforce_rate_limit(request, "report_checkout", limit=20, window_seconds=600)
    await validate_csrf_token(request)
    order = _service_order_by_token_or_404(db, order_token)
    if order.service_type != "report":
        _public_error("Bu ödeme adımı rapor siparişleri içindir.", 404)
    try:
        session = create_report_payment_session(order, request=request)
    except payments.PaymentConfigurationError as exc:
        logger.warning("Report service checkout unavailable order_id=%s detail=%s", order.id, exc)
        return RedirectResponse(url=f"/checkout/report/{order.order_token}?notice=payments_unavailable", status_code=303)
    except payments.PaymentError as exc:
        logger.warning("Report service checkout failed order_id=%s detail=%s", order.id, exc)
        return RedirectResponse(url=f"/checkout/report/{order.order_token}?notice=checkout_failed", status_code=303)
    order.payment_provider = session.get("provider") or payments.payment_provider()
    order.provider_name = session.get("provider") or payments.payment_provider()
    order.provider_token = session.get("provider_token") or session.get("session_id")
    order.provider_conversation_id = session.get("provider_conversation_id") or _order_public_token(order)
    order.payment_session_id = session.get("session_id")
    order.status = "awaiting_payment"
    db.commit()
    redirect_url = session.get("redirect_url")
    if redirect_url:
        return RedirectResponse(url=redirect_url, status_code=303)
    return RedirectResponse(url=f"/checkout/report/{order.order_token}?notice=missing_redirect", status_code=303)


@app.get("/checkout/report/{order_token}/success", response_class=HTMLResponse)
async def report_checkout_success(
    request: Request,
    order_token: str,
    session_id: str = "",
    db: Session = Depends(get_db),
):
    order = _service_order_by_token_or_404(db, order_token)
    if order.service_type != "report":
        _public_error("Bu ödeme adımı rapor siparişleri içindir.", 404)
    return RedirectResponse(url=f"/checkout/report/{order.order_token}?notice=verification_pending", status_code=303)


@app.get("/checkout/report/{order_token}/cancel")
async def report_checkout_cancel(order_token: str, db: Session = Depends(get_db)):
    order = _service_order_by_token_or_404(db, order_token)
    if order.service_type != "report":
        _public_error("Bu ödeme adımı rapor siparişleri içindir.", 404)
    order.status = "awaiting_payment"
    db.commit()
    return RedirectResponse(url=f"/checkout/report/{order.order_token}?notice=cancelled", status_code=303)


async def _extract_payment_token(request):
    token = str(request.query_params.get("token", "") or "").strip()
    if token:
        return token
    content_type = str(request.headers.get("content-type", "")).lower()
    if "application/json" in content_type:
        try:
            payload = await request.json()
            return str((payload or {}).get("token", "") or "").strip()
        except Exception:
            return ""
    try:
        form = await request.form()
        return str(form.get("token", "") or "").strip()
    except Exception:
        return ""


def _order_by_provider_token_or_404(db, token, service_type=None):
    query = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.provider_token == str(token or ""))
    if service_type:
        query = query.filter(db_mod.ServiceOrder.service_type == service_type)
    order = query.first()
    if not order:
        _public_error("Ödeme oturumu bulunamadı.", 404)
    return order


def _retrieve_iyzico_payment_for_order(order, token):
    provider = payments.get_payment_provider()
    if getattr(provider, "provider_name", "") != "iyzico" or not hasattr(provider, "retrieve_checkout_form"):
        raise payments.PaymentConfigurationError("Iyzico retrieve API is not configured.")
    conversation_id = getattr(order, "provider_conversation_id", None) or _order_public_token(order)
    return provider.retrieve_checkout_form(token, conversation_id)


async def _handle_iyzico_callback(request, db, service_type):
    token = await _extract_payment_token(request)
    if not token:
        _public_error("Iyzico callback token is missing.", 400)
    order = _order_by_provider_token_or_404(db, token, service_type=service_type)
    try:
        retrieve_payload = _retrieve_iyzico_payment_for_order(order, token)
        result = process_verified_service_payment(db, order, retrieve_payload)
    except payments.PaymentError as exc:
        logger.warning("Iyzico callback rejected order_id=%s detail=%s", getattr(order, "id", None), exc)
        if "fraudStatus" in str(exc):
            mark_payment_under_review(db, order, retrieve_payload if "retrieve_payload" in locals() else {}, actor="iyzico_callback")
        if service_type == "report":
            return RedirectResponse(url=f"/checkout/report/{order.order_token}?notice=verification_failed", status_code=303)
        return RedirectResponse(url="/checkout/consultation?notice=verification_failed", status_code=303)
    if service_type == "report":
        payload = _order_data_from_service_order(order)
        return templates.TemplateResponse(
            request=request,
            name="report_order_submitted.html",
            context=_auth_template_context(
                request,
                product=_service_order_product(order),
                order=payload,
                delivery_days=get_setting(db, "site_report_delivery_days", "7"),
            ),
        )
    return RedirectResponse(url="/personal-consultation/book?paid=1", status_code=303)


@app.post("/payments/iyzico/callback/report", response_class=HTMLResponse)
async def iyzico_report_callback(request: Request, db: Session = Depends(get_db)):
    return await _handle_iyzico_callback(request, db, "report")


@app.post("/payments/iyzico/callback/consultation", response_class=HTMLResponse)
async def iyzico_consultation_callback(request: Request, db: Session = Depends(get_db)):
    return await _handle_iyzico_callback(request, db, "consultation")


def _iyzico_webhook_signature_valid(payload, signature):
    signature = str(signature or "").strip()
    signature_required = _payment_env_flag("IYZICO_WEBHOOK_SIGNATURE_REQUIRED", default=False)
    secret = str(os.getenv("IYZICO_WEBHOOK_SECRET", "") or os.getenv("IYZICO_SECRET_KEY", "")).strip()
    if not signature:
        return not signature_required
    if not secret:
        return False
    return payments.IyzicoProvider.verify_hpp_webhook_signature(payload or {}, signature, secret_key=secret)


@app.post("/payments/iyzico/webhook")
async def iyzico_webhook(request: Request, db: Session = Depends(get_db)):
    content_type = str(request.headers.get("content-type", "")).lower()
    if "application/json" not in content_type:
        _public_error("Iyzico webhook requires application/json.", 415)
    body = await request.body()
    try:
        payload = json.loads(body.decode("utf-8") or "{}")
    except Exception:
        _public_error("Iyzico webhook payload is not valid JSON.", 400)
    signature = request.headers.get("x-iyz-signature-v3") or request.headers.get("X-IYZ-SIGNATURE-V3", "")
    if not _iyzico_webhook_signature_valid(payload, signature):
        logger.warning(
            "Iyzico webhook signature rejected event_type=%s payment_id=%s token=%s",
            payload.get("iyziEventType"),
            payload.get("iyziPaymentId") or payload.get("paymentId"),
            payload.get("token"),
        )
        _public_error("Iyzico webhook signature rejected.", 400)
    if not signature:
        logger.warning("Iyzico webhook accepted without signature because signature requirement is disabled.")
    token = str(payload.get("token") or request.query_params.get("token", "") or "").strip()
    if not token:
        _public_error("Iyzico webhook token is missing.", 400)
    order = _order_by_provider_token_or_404(db, token)
    retrieve_payload = _retrieve_iyzico_payment_for_order(order, token)
    try:
        result = process_verified_service_payment(db, order, retrieve_payload)
    except payments.PaymentVerificationError as exc:
        if "fraudStatus" in str(exc):
            result = mark_payment_under_review(db, order, retrieve_payload, actor="iyzico_webhook")
        else:
            raise
    return {"status": "ok", "changed": bool(result.get("changed")), "order_id": order.id}


@app.post("/webhooks/calendly")
async def calendly_webhook(request: Request, db: Session = Depends(get_db)):
    body = await request.body()
    if not verify_calendly_webhook_signature(body, request.headers):
        _public_error("Calendly webhook signature rejected.", 400)
    try:
        payload_body = json.loads(body.decode("utf-8") or "{}")
    except Exception:
        logger.warning("Calendly webhook ignored malformed JSON.")
        return {"ok": True, "ignored": True}
    event_type = str(payload_body.get("event") or "").strip()
    payload = payload_body.get("payload") if isinstance(payload_body.get("payload"), Mapping) else {}
    if event_type not in {"invitee.created", "invitee.canceled"}:
        return {"ok": True, "ignored": True, "event": event_type or None}
    try:
        result = process_calendly_webhook_event(db, event_type, payload)
    except Exception as exc:
        logger.exception("Calendly webhook processing failed event=%s detail=%s", event_type, exc)
        _public_error("Calendly webhook could not be processed.", 500)
    order = result.get("order")
    return {
        "ok": True,
        "event": event_type,
        "action": result.get("action"),
        "order_token": getattr(order, "order_token", None),
        "checkout_url": result.get("checkout_url"),
    }


@app.get("/reports/parent-child", response_class=HTMLResponse)
async def parent_child_report_form(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="parent_child_form.html",
        context={"request": request},
    )


@app.get("/reports/{report_id}", response_class=HTMLResponse)
async def report_revisit(request: Request, report_id: int, db: Session = Depends(get_db)):
    user = _require_authenticated_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)

    report = _owned_report_or_404(db, user, report_id)
    current_language = _result_language(request, user)

    payload = _safe_json_loads(report.result_payload_json, {})
    if not isinstance(payload, dict):
        payload = {}
    interpretation_context = _safe_json_loads(report.interpretation_context_json, {})
    interpretation_context = _localize_result_layer_text(interpretation_context or payload.get("interpretation_context") or {}, current_language)
    payload["language"] = current_language
    payload["generated_report_id"] = report.id
    payload["interpretation_context"] = interpretation_context
    payload["full_name"] = payload.get("full_name") or report.full_name
    payload["birth_date"] = payload.get("birth_date") or report.birth_date
    payload["birth_time"] = payload.get("birth_time") or report.birth_time
    payload["birth_city"] = payload.get("birth_city") or report.birth_city
    payload["normalized_birth_place"] = payload.get("normalized_birth_place") or report.normalized_birth_place
    payload["timezone"] = payload.get("timezone") or report.timezone
    payload["report_type"] = payload.get("report_type") or report.report_type
    payload["payload_json"] = _serialize_temporal_values(payload)
    payload["revisit_context"] = _build_revisit_context(db, user, report)
    checkout_state = str(request.query_params.get("checkout", "")).strip().lower()
    if checkout_state == "cancelled":
        payload["checkout_notice"] = "Checkout was cancelled. Your preview is still here whenever you want to continue."
    elif checkout_state == "verification-failed":
        payload["checkout_notice"] = "We could not verify the payment session yet. Please retry from your report history."
    payload = _apply_report_access_context(
        payload,
        report,
        current_user=user,
        unlock_success=str(request.query_params.get("unlocked", "0")) == "1",
    )
    payload["related_articles"] = [] if payload.get("report_type") == "parent_child" else _match_related_articles_for_result(db, interpretation_context, language=current_language)
    return templates.TemplateResponse(request=request, name="result.html", context=payload)


@app.post("/api/v1/reports/{report_id}/checkout")
async def create_report_checkout(request: Request, report_id: int, db: Session = Depends(get_db)):
    enforce_rate_limit(request, "legacy_report_checkout", limit=20, window_seconds=600)
    user = _require_authenticated_user(request, db)
    if not user:
        return JSONResponse(status_code=401, content={"ok": False, "error": "authentication_required"})
    report = _owned_report_or_404(db, user, report_id)
    if can_view_full_report(report):
        return json_ok(
            {
                "report": {
                    "id": report.id,
                    "access_state": get_report_access_state(report),
                },
                "redirect_url": f"/reports/{report.id}",
            }
        )
    if can_use_beta_free_unlock(user):
        return json_ok(
            {
                "report": {
                    "id": report.id,
                    "access_state": get_report_access_state(report),
                },
                "redirect_url": f"/reports/{report.id}",
                "mode": "beta",
            }
        )
    try:
        success_url, cancel_url = _report_checkout_urls(report, request=request)
        provider = payments.get_payment_provider()
        session = provider.create_checkout_session(report, user, success_url=success_url, cancel_url=cancel_url)
    except payments.PaymentConfigurationError as exc:
        logger.warning("Checkout configuration error report_id=%s user_id=%s detail=%s", report.id, user.id, exc)
        return JSONResponse(status_code=503, content={"ok": False, "error": "payments_unavailable", "detail": str(exc)})
    except payments.PaymentError as exc:
        logger.warning("Checkout creation failed report_id=%s user_id=%s detail=%s", report.id, user.id, exc)
        return JSONResponse(status_code=400, content={"ok": False, "error": "checkout_failed", "detail": str(exc)})
    return json_ok(
        {
            "report": {
                "id": report.id,
                "access_state": get_report_access_state(report),
            },
            "checkout_session_id": session.get("session_id"),
            "redirect_url": session.get("redirect_url"),
            "mode": "payment",
        }
    )


@app.post("/api/v1/reports/{report_id}/beta-unlock")
async def beta_unlock_report(request: Request, report_id: int, db: Session = Depends(get_db)):
    user = _require_authenticated_user(request, db)
    if not user:
        return JSONResponse(status_code=401, content={"ok": False, "error": "authentication_required"})
    report = _owned_report_or_404(db, user, report_id)
    if not can_use_beta_free_unlock(user):
        return JSONResponse(status_code=403, content={"ok": False, "error": "beta_unlock_not_allowed"})
    mark_report_as_unlocked(report, payment_reference="beta-free-unlock")
    mark_email_capture_converted(db, report=report, email=getattr(user, "email", None))
    db.commit()
    db.refresh(report)
    return json_ok(
        {
            "report": {
                "id": report.id,
                "access_state": get_report_access_state(report),
                "payment_reference": report.payment_reference,
                "can_view_full_report": can_view_full_report(report),
                "can_download_pdf": can_download_pdf(report),
            },
            "redirect_url": f"/reports/{report.id}?unlocked=1",
        }
    )


@app.get("/checkout/success")
async def checkout_success(request: Request, report_id: int, session_id: str, db: Session = Depends(get_db)):
    user = _require_authenticated_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    report = _owned_report_or_404(db, user, report_id)
    try:
        provider = payments.get_payment_provider()
        payment_data = provider.verify_payment(session_id)
    except payments.PaymentError as exc:
        logger.warning("Checkout success verification failed report_id=%s user_id=%s session_id=%s detail=%s", report_id, user.id, session_id, exc)
        return RedirectResponse(url=f"/reports/{report.id}?checkout=verification-failed", status_code=303)
    if int(payment_data.get("report_id", -1)) != report.id or int(payment_data.get("user_id", -1)) != _user_id(user):
        logger.warning("Checkout success ownership mismatch report_id=%s user_id=%s session_id=%s", report.id, _user_id(user), session_id)
        return RedirectResponse(url=f"/reports/{report.id}?checkout=verification-failed", status_code=303)
    _finalize_report_purchase(report, payment_data)
    mark_email_capture_converted(db, report=report, email=getattr(user, "email", None))
    db.commit()
    return RedirectResponse(url=f"/reports/{report.id}?unlocked=1", status_code=303)


@app.get("/checkout/cancel")
async def checkout_cancel(request: Request, report_id: int, db: Session = Depends(get_db)):
    user = _require_authenticated_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    report = _owned_report_or_404(db, user, report_id)
    return RedirectResponse(url=f"/reports/{report.id}?checkout=cancelled", status_code=303)


@app.get("/account", response_class=HTMLResponse)
async def account_page(request: Request, db: Session = Depends(get_db)):
    user = _require_authenticated_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)

    return templates.TemplateResponse(
        request=request,
        name="account.html",
        context=_auth_template_context(request, dashboard_user=_public_user_view(user), plan_features=get_plan_features(user)),
    )


@app.post("/account/plan")
async def update_plan(request: Request, plan_code: str = Form(...), db: Session = Depends(get_db)):
    user = _require_authenticated_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)

    previous_plan = normalize_plan_code(user.plan_code)
    next_plan = normalize_plan_code(plan_code)
    user.plan_code = next_plan
    user.plan_started_at = datetime.utcnow()
    db.commit()
    logger.info("Plan updated user_id=%s plan=%s", user.id, next_plan)
    if previous_plan != next_plan:
        if PLAN_ORDER.get(next_plan, 0) > PLAN_ORDER.get(previous_plan, 0):
            maybe_send_plan_activation_email(db, user, previous_plan, next_plan, event_key=f"manual-plan:{user.id}:{next_plan}:{datetime.utcnow().date().isoformat()}")
        elif next_plan != previous_plan:
            maybe_send_cancellation_email(db, user, previous_plan, next_plan, event_key=f"manual-downgrade:{user.id}:{next_plan}:{datetime.utcnow().date().isoformat()}")
    return RedirectResponse(url="/account", status_code=303)


@app.get("/admin", response_class=HTMLResponse)
@admin_required
async def admin_home(request: Request, db: Session = Depends(get_db)):
    try:
        total_users = db.query(db_mod.AppUser).count()
        total_articles = db.query(func.count(db_mod.Article.id)).scalar() or 0
        week_ago = datetime.utcnow() - timedelta(days=7)
        users_this_week = db.query(db_mod.AppUser).filter(db_mod.AppUser.created_at >= week_ago).count()
        pending_reports = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.service_type == "report", db_mod.ServiceOrder.status == "pending").count()
        delivered_reports_count = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.service_type == "report", db_mod.ServiceOrder.status == "delivered").count()
        total_consultations = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.service_type == "consultation").count()
        pending_consultations = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.service_type == "consultation", db_mod.ServiceOrder.status == "pending").count()
        recent_users = db.query(db_mod.AppUser).order_by(db_mod.AppUser.created_at.desc()).limit(5).all()
        recent_reports = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.service_type == "report").order_by(db_mod.ServiceOrder.created_at.desc()).limit(10).all()
        return templates.TemplateResponse(
            request=request,
            name="admin/dashboard.html",
            context=_auth_template_context(
                request,
                dashboard_user=request.state.admin_user,
                active_page="dashboard",
                total_users=total_users,
                users_this_week=users_this_week,
                pending_reports=pending_reports,
                delivered_reports_count=delivered_reports_count,
                total_consultations=total_consultations,
                pending_consultations=pending_consultations,
                total_articles=total_articles,
                recent_reports=[_service_report_row(order) for order in recent_reports],
                recent_users=recent_users,
            ),
        )
    except Exception:
        logger.exception("Admin home failed")
        _public_error("Admin paneli yuklenemedi.", 500)


@app.get("/admin/login", response_class=HTMLResponse)
async def admin_login_page(request: Request):
    return RedirectResponse(url="/login", status_code=303)


@app.get("/admin/logout")
async def admin_logout(request: Request):
    if hasattr(request, "session"):
        request.session.clear()
    return RedirectResponse(url="/admin/login", status_code=303)


@app.get("/admin/debug/whoami")
@admin_required
async def admin_debug_whoami(request: Request, db: Session = Depends(get_db)):
    user = get_request_user(request, db)
    email = str(getattr(user, "email", "") or "").strip().lower()
    return JSONResponse(
        {
            "authenticated_email": email,
            "user_id": _safe_model_id(user),
            "is_admin": is_admin_user(user),
            "in_admin_allowlist": email in _admin_email_allowlist(),
            "route_path": request.url.path,
            "template_hint": "admin/dashboard.html" if request.url.path in {"/admin", "/admin/dashboard"} else "admin/debug/whoami",
        }
    )


@app.get("/admin/debug/storage")
@admin_required
async def admin_debug_storage(request: Request, db: Session = Depends(get_db)):
    return JSONResponse(_storage_diagnostics(db))


def _admin_order_view(order):
    return {
        "id": order.id,
        "created_at": order.created_at,
        "customer_name": order.customer_name or "-",
        "customer_email": order.customer_email or "-",
        "service_type": order.service_type,
        "product_type": order.product_type,
        "status": order.status,
        "amount": order.amount_label or order.amount or "-",
        "paid_at": order.paid_at,
    }


def _service_report_row(order):
    payload = _service_order_payload(order)
    return {
        "id": order.id,
        "full_name": order.customer_name or payload.get("full_name") or "-",
        "email": order.customer_email or payload.get("email") or "-",
        "report_type": order.product_type or payload.get("report_type") or "-",
        "birth_city": order.birth_place or payload.get("birth_city") or "-",
        "admin_note": getattr(order, "admin_note", None) or getattr(order, "internal_notes", None) or "",
        "created_at": order.created_at,
        "customer_name": order.customer_name or payload.get("full_name") or "-",
        "customer_email": order.customer_email or payload.get("email") or "-",
        "birth_date": order.birth_date or payload.get("birth_date") or "-",
        "birth_time": order.birth_time or payload.get("birth_time") or "-",
        "birth_place": order.birth_place or payload.get("birth_place") or payload.get("birth_city") or "-",
        "product_type": order.product_type or payload.get("report_type") or "-",
        "status": order.status,
        "amount": order.amount_label or order.amount or "-",
        "paid_at": order.paid_at,
        "delivered_at": order.delivered_at,
        "pdf_ready": bool(order.final_pdf_path and order.pdf_status in {"completed", "ready"}),
        "is_gift": bool(getattr(order, "is_gift", False)),
        "gift_recipient_name": getattr(order, "gift_recipient_name", None),
        "gift_recipient_email": getattr(order, "gift_recipient_email", None),
    }


def _consultation_order_row(order):
    paid = bool(order.paid_at and order.status in SUCCESSFUL_PAID_ORDER_STATUSES)
    return {
        "id": order.id,
        "name": order.customer_name or "-",
        "email": order.customer_email or "-",
        "booking_date": order.scheduled_start,
        "payment_status": "paid" if paid else "pending",
        "notes": order.internal_notes or order.optional_note or "",
        "created_at": order.created_at,
        "customer_name": order.customer_name or "-",
        "customer_email": order.customer_email or "-",
        "scheduled_start": order.scheduled_start,
        "scheduled_end": order.scheduled_end,
        "paid": paid,
        "paid_label": "Yes" if paid else "No",
        "status": order.status,
        "calendly_event_uri": order.calendly_event_uri,
    }


def _article_admin_view(article):
    return {
        "id": article.id,
        "title": article.title,
        "slug": article.slug,
        "content": article.body or "",
        "status": "published" if article.is_published else "draft",
        "updated_at": article.updated_at.strftime("%Y-%m-%d %H:%M") if article.updated_at else "-",
    }


ADMIN_DASHBOARD_RANGE_OPTIONS = {
    "1d": "Last 24 Hours",
    "7d": "Last 7 Days",
    "30d": "Last 30 Days",
    "all": "All Time",
}
ADMIN_DASHBOARD_STATUS_KEYS = [
    "awaiting_payment",
    "booking_pending_payment",
    "paid",
    "draft_ready",
    "under_review",
    "ready_to_send",
    "delivered",
    "confirmed",
    "prepared",
    "completed",
    "refunded",
    "cancelled",
    "no_show",
    "payment_under_review",
]
PAID_OR_LATER_STATUSES = {
    "paid",
    "draft_pending",
    "draft_sent_to_admin",
    "draft_ready",
    "under_review",
    "ready_to_send",
    "delivered",
    "confirmed",
    "prepared",
    "completed",
    "refunded",
    "partially_refunded",
}
SUCCESSFUL_PAID_ORDER_STATUSES = PAID_OR_LATER_STATUSES


def _admin_dashboard_range(range_key):
    normalized = str(range_key or "7d").strip().lower()
    if normalized == "today":
        normalized = "1d"
    if normalized not in ADMIN_DASHBOARD_RANGE_OPTIONS:
        normalized = "7d"
    now = datetime.utcnow()
    if normalized == "1d":
        start = now - timedelta(days=1)
    elif normalized == "7d":
        start = now - timedelta(days=7)
    elif normalized == "30d":
        start = now - timedelta(days=30)
    else:
        start = None
    return {
        "key": normalized,
        "label": ADMIN_DASHBOARD_RANGE_OPTIONS[normalized],
        "start": start,
        "end": now,
    }


def resolve_range(range_key):
    return _admin_dashboard_range(range_key)


def _decimal_amount(value):
    if value in (None, ""):
        return Decimal("0.00")
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return Decimal("0.00")


def _money_label(value, currency="TRY"):
    amount = _decimal_amount(value)
    prefix = "TRY " if str(currency or "TRY").upper() == "TRY" else f"{currency} "
    return f"{prefix}{amount:,.2f}"


def _dashboard_date_filter(query, column, date_range):
    start = date_range.get("start")
    end = date_range.get("end")
    if start is not None:
        query = query.filter(column >= start)
    if end is not None:
        query = query.filter(column <= end)
    return query


def _paid_orders_query(db):
    # Paid/revenue metrics use server-verified paid_at plus paid-or-later workflow states.
    # Pending, failed, cancelled, and no-show orders are excluded from paid-order counts.
    return db.query(db_mod.ServiceOrder).filter(
        db_mod.ServiceOrder.paid_at.isnot(None),
        db_mod.ServiceOrder.status.in_(PAID_OR_LATER_STATUSES),
    )


def get_revenue_metrics(db, date_range):
    paid_orders = _dashboard_date_filter(_paid_orders_query(db), db_mod.ServiceOrder.paid_at, date_range).all()
    refund_query = db.query(db_mod.ServiceOrder).filter(
        db_mod.ServiceOrder.refunded_at.isnot(None),
        db_mod.ServiceOrder.refund_status.in_(("refunded", "partially_refunded")),
    )
    refunded_orders = _dashboard_date_filter(refund_query, db_mod.ServiceOrder.refunded_at, date_range).all()

    # Gross revenue is successful verified payments in the range.
    # Refunded amount is refunds issued in the range.
    # Net revenue is gross paid revenue minus actual refunds. Platform/payment fees are not deducted
    # because fee data is not stored on ServiceOrder.
    gross_revenue = sum((_decimal_amount(order.amount) for order in paid_orders), Decimal("0.00"))
    refunded_amount = sum((_decimal_amount(order.refund_amount) for order in refunded_orders), Decimal("0.00"))
    paid_count = len(paid_orders)
    average_order_value = (gross_revenue / paid_count).quantize(Decimal("0.01")) if paid_count else Decimal("0.00")
    return {
        "gross_revenue": gross_revenue,
        "refunded_amount": refunded_amount,
        "net_revenue": gross_revenue - refunded_amount,
        "paid_order_count": paid_count,
        "average_order_value": average_order_value,
        "refund_count": len(refunded_orders),
        "gross_revenue_label": _money_label(gross_revenue),
        "refunded_amount_label": _money_label(refunded_amount),
        "net_revenue_label": _money_label(gross_revenue - refunded_amount),
        "average_order_value_label": _money_label(average_order_value),
    }


def _paid_orders_for_range(db, date_range):
    return _dashboard_date_filter(_paid_orders_query(db), db_mod.ServiceOrder.paid_at, date_range).all()


def get_consultation_conversion(db, date_range=None):
    query = db.query(db_mod.ServiceOrder).filter(
        db_mod.ServiceOrder.service_type == "consultation",
        or_(
            db_mod.ServiceOrder.booking_source == "calendly",
            db_mod.ServiceOrder.calendly_event_uri.isnot(None),
            db_mod.ServiceOrder.calendly_invitee_uri.isnot(None),
        ),
    )
    if date_range and date_range.get("start") is not None:
        query = _dashboard_date_filter(query, db_mod.ServiceOrder.created_at, date_range)
    bookings = query.all()
    total_bookings = len(bookings)
    paid_consultations = sum(
        1
        for order in bookings
        if order.paid_at is not None and order.status in PAID_OR_LATER_STATUSES
    )
    conversion_rate = (paid_consultations / total_bookings) if total_bookings else 0.0
    return {
        "total_bookings": total_bookings,
        "paid_consultations": paid_consultations,
        "conversion_rate": conversion_rate,
        "conversion_rate_label": f"{round(conversion_rate * 100, 1)}%",
    }


def get_product_performance(db, date_range):
    rows = {}
    for order in _paid_orders_for_range(db, date_range):
        key = order.bundle_type or order.product_type or order.service_type or "unknown"
        product = rows.setdefault(
            key,
            {
                "product_type": key,
                "product_name": _service_order_product(order).get("title", key),
                "paid_order_count": 0,
                "revenue": Decimal("0.00"),
                "delivered_count": 0,
                "completed_count": 0,
            },
        )
        product["paid_order_count"] += 1
        product["revenue"] += max(_decimal_amount(order.amount) - _decimal_amount(order.refund_amount), Decimal("0.00"))
        if order.service_type == "report" and order.status == "delivered":
            product["delivered_count"] += 1
        if order.service_type == "consultation" and order.status == "completed":
            product["completed_count"] += 1

    performance = list(rows.values())
    performance.sort(key=lambda item: item["revenue"], reverse=True)
    for item in performance:
        item["revenue_label"] = _money_label(item["revenue"])
    return performance


def get_status_breakdown(db, date_range=None):
    query = db.query(db_mod.ServiceOrder)
    if date_range and date_range.get("start") is not None:
        query = _dashboard_date_filter(query, db_mod.ServiceOrder.created_at, date_range)
    counts = {status: 0 for status in ADMIN_DASHBOARD_STATUS_KEYS}
    for order in query.all():
        if order.status in counts:
            counts[order.status] += 1
    return [{"status": status, "count": count} for status, count in counts.items()]


def get_revenue_timeseries(db, date_range):
    start = date_range.get("start")
    end = date_range.get("end") or datetime.utcnow()
    if start is None:
        start = end - timedelta(days=30)

    days = []
    cursor = datetime(start.year, start.month, start.day)
    end_day = datetime(end.year, end.month, end.day)
    while cursor <= end_day:
        days.append(cursor.date())
        cursor += timedelta(days=1)

    rows = {
        day: {
            "date": day.isoformat(),
            "gross_revenue": Decimal("0.00"),
            "paid_orders": 0,
            "refunded_amount": Decimal("0.00"),
        }
        for day in days
    }
    for order in _dashboard_date_filter(_paid_orders_query(db), db_mod.ServiceOrder.paid_at, {"start": start, "end": end}).all():
        day = order.paid_at.date()
        if day in rows:
            rows[day]["gross_revenue"] += _decimal_amount(order.amount)
            rows[day]["paid_orders"] += 1
    refund_query = db.query(db_mod.ServiceOrder).filter(
        db_mod.ServiceOrder.refunded_at.isnot(None),
        db_mod.ServiceOrder.refund_status.in_(("refunded", "partially_refunded")),
    )
    for order in _dashboard_date_filter(refund_query, db_mod.ServiceOrder.refunded_at, {"start": start, "end": end}).all():
        day = order.refunded_at.date()
        if day in rows:
            rows[day]["refunded_amount"] += _decimal_amount(order.refund_amount)

    output = []
    for row in rows.values():
        row["net_revenue"] = row["gross_revenue"] - row["refunded_amount"]
        row["gross_revenue_label"] = _money_label(row["gross_revenue"])
        row["refunded_amount_label"] = _money_label(row["refunded_amount"])
        row["net_revenue_label"] = _money_label(row["net_revenue"])
        output.append(row)
    return output


def get_recent_activity(db, limit=20):
    logs = db.query(db_mod.AdminActionLog).order_by(db_mod.AdminActionLog.created_at.desc()).limit(limit).all()
    activity = []
    for log in logs:
        order = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.id == log.order_id).first()
        activity.append(
            {
                "timestamp": log.created_at,
                "order_id": log.order_id,
                "customer": getattr(order, "customer_email", None) or getattr(order, "customer_name", None) or "-",
                "action": log.action,
                "metadata": log.metadata_json or "",
            }
        )
    if activity:
        return activity

    fallback_statuses = {"paid", "delivered", "refunded", "partially_refunded", "completed", "payment_under_review"}
    orders = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.status.in_(fallback_statuses)).order_by(db_mod.ServiceOrder.updated_at.desc()).limit(limit).all()
    return [
        {
            "timestamp": order.updated_at or order.created_at,
            "order_id": order.id,
            "customer": order.customer_email or order.customer_name or "-",
            "action": order.status,
            "metadata": order.product_type,
        }
        for order in orders
    ]


def get_dashboard_metrics(db, range_key="7d"):
    date_range = resolve_range(range_key)
    today_range = {
        "key": "today",
        "label": "Today",
        "start": datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0),
        "end": datetime.utcnow(),
    }
    selected_revenue = get_revenue_metrics(db, date_range)
    today_revenue = get_revenue_metrics(db, today_range)
    conversion = get_consultation_conversion(db, date_range)
    active_bookings = db.query(db_mod.ServiceOrder).filter(
        db_mod.ServiceOrder.service_type == "consultation",
        db_mod.ServiceOrder.status == "booking_pending_payment",
    ).count()
    reports_delivered_query = db.query(db_mod.ServiceOrder).filter(
        db_mod.ServiceOrder.service_type == "report",
        db_mod.ServiceOrder.status == "delivered",
    )
    consultations_completed_query = db.query(db_mod.ServiceOrder).filter(
        db_mod.ServiceOrder.service_type == "consultation",
        db_mod.ServiceOrder.status == "completed",
    )
    if date_range.get("start") is not None:
        reports_delivered_query = _dashboard_date_filter(
            reports_delivered_query,
            db_mod.ServiceOrder.delivered_at,
            date_range,
        )
        consultations_completed_query = _dashboard_date_filter(
            consultations_completed_query,
            db_mod.ServiceOrder.completed_at,
            date_range,
        )
    reports_delivered = reports_delivered_query.count()
    consultations_completed = consultations_completed_query.count()
    under_review_count = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.status == "payment_under_review").count()
    return {
        "range": date_range,
        "range_options": ADMIN_DASHBOARD_RANGE_OPTIONS,
        "selected_revenue": selected_revenue,
        "consultation_conversion": conversion,
        "kpis": [
            {"label": "Revenue Today", "value": today_revenue["gross_revenue_label"], "note": "Verified paid orders with paid_at today"},
            {"label": "Net Revenue", "value": selected_revenue["net_revenue_label"], "note": f"Gross paid revenue minus actual refunds in {date_range['label']}; platform fees not deducted"},
            {"label": "Paid Orders", "value": selected_revenue["paid_order_count"], "note": f"Verified paid orders in {date_range['label']}"},
            {"label": "Average Order Value", "value": selected_revenue["average_order_value_label"], "note": f"{date_range['label']} gross revenue / paid orders"},
            {"label": "Active Bookings", "value": active_bookings, "note": "Consultation orders in booking_pending_payment"},
            {"label": "Consultation Conversion Rate", "value": conversion["conversion_rate_label"], "note": f"{conversion['paid_consultations']} paid / {conversion['total_bookings']} Calendly bookings"},
            {"label": "Reports Delivered", "value": reports_delivered, "note": "Report orders with delivered status"},
            {"label": "Consultations Completed", "value": consultations_completed, "note": "Consultation orders with completed status"},
            {"label": "Refund Count", "value": selected_revenue["refund_count"], "note": f"Refunds issued in {date_range['label']}"},
            {"label": "Orders Under Review", "value": under_review_count, "note": "Current payment_under_review queue"},
        ],
        "product_performance": get_product_performance(db, date_range),
        "status_breakdown": get_status_breakdown(db, date_range),
        "recent_activity": get_recent_activity(db, limit=20),
        "timeseries": get_revenue_timeseries(db, date_range),
    }


@app.get("/admin/dashboard", response_class=HTMLResponse)
@admin_required
async def admin_dashboard(
    request: Request,
    db: Session = Depends(get_db),
    range_key: str = Query(default="7d", alias="range"),
):
    dashboard = get_dashboard_metrics(db, range_key)
    pending_reports = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.service_type == "report", db_mod.ServiceOrder.status == "pending").count()
    delivered_reports_count = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.service_type == "report", db_mod.ServiceOrder.status == "delivered").count()
    pending_consultations = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.service_type == "consultation", db_mod.ServiceOrder.status == "pending").count()
    recent_report_orders = (
        db.query(db_mod.ServiceOrder)
        .filter(db_mod.ServiceOrder.service_type == "report")
        .order_by(db_mod.ServiceOrder.created_at.desc())
        .limit(10)
        .all()
    )
    recent_users = (
        db.query(db_mod.AppUser)
        .order_by(db_mod.AppUser.created_at.desc())
        .limit(5)
        .all()
    )
    return templates.TemplateResponse(
        request=request,
        name="admin/dashboard.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            active_page="dashboard",
            dashboard=dashboard,
            pending_reports=pending_reports,
            delivered_reports_count=delivered_reports_count,
            pending_consultations=pending_consultations,
            recent_reports=[_service_report_row(order) for order in recent_report_orders],
            recent_users=recent_users,
        ),
    )


@app.get("/admin/orders", response_class=HTMLResponse)
@admin_required
async def admin_orders(request: Request, db: Session = Depends(get_db), order_type: str = "", product_type: str = "", status: str = ""):
    active_statuses = {
        "paid", "draft_ready", "under_review", "ready_to_send", "delivered",
        "confirmed", "prepared", "completed", "payment_under_review",
        "cancelled", "no_show", "refunded", "partially_refunded",
    }
    query = db.query(db_mod.ServiceOrder)
    if order_type.strip():
        query = query.filter(db_mod.ServiceOrder.service_type == order_type.strip())
    if not status.strip():
        query = query.filter(db_mod.ServiceOrder.status.in_(active_statuses))
    if product_type.strip():
        query = query.filter(db_mod.ServiceOrder.product_type == product_type.strip())
    if status.strip():
        query = query.filter(db_mod.ServiceOrder.status == status.strip())
    orders = query.order_by(db_mod.ServiceOrder.created_at.desc()).limit(200).all()
    return templates.TemplateResponse(
        request=request,
        name="admin/orders.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            orders=[_admin_order_view(order) for order in orders],
            filters={"order_type": order_type, "product_type": product_type, "status": status},
            statuses=sorted(active_statuses),
            product_types=sorted(REPORT_ORDER_PRODUCTS.keys()) + sorted(REPORT_BUNDLE_PRODUCTS.keys()) + [CONSULTATION_PRODUCT["product_type"]],
        ),
    )


@app.get("/admin/orders/{order_id}", response_class=HTMLResponse)
@admin_required
async def admin_order_detail(request: Request, order_id: int, db: Session = Depends(get_db), notice: str = "", error: str = ""):
    order = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.id == order_id).first()
    if not order:
        _public_error("Order not found.", 404)
    logs = db.query(db_mod.AdminActionLog).filter(db_mod.AdminActionLog.order_id == order.id).order_by(db_mod.AdminActionLog.created_at.desc()).all()
    return templates.TemplateResponse(
        request=request,
        name="admin/order_detail.html",
        context=_auth_template_context(request, dashboard_user=request.state.admin_user, order=order, product=_service_order_product(order), payload=_service_order_payload(order), logs=logs, notice=notice, error=error),
    )


@app.post("/admin/orders/{order_id}/notes")
@admin_required
async def admin_order_save_notes(request: Request, order_id: int, internal_notes: str = Form(default=""), db: Session = Depends(get_db)):
    order = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.id == order_id).first()
    if not order:
        _public_error("Order not found.", 404)
    save_order_internal_notes(db, order, internal_notes, actor=_admin_actor(request))
    return RedirectResponse(url=f"/admin/orders/{order.id}?notice=notes_saved", status_code=303)


@app.post("/admin/orders/{order_id}/transition")
@admin_required
async def admin_order_transition(request: Request, order_id: int, action: str = Form(...), db: Session = Depends(get_db)):
    order = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.id == order_id).first()
    if not order:
        _public_error("Order not found.", 404)
    try:
        apply_admin_order_transition(db, order, action, actor=_admin_actor(request))
    except ValueError as exc:
        return RedirectResponse(url=f"/admin/orders/{order.id}?error={urlencode({'message': str(exc)})}", status_code=303)
    return RedirectResponse(url=f"/admin/orders/{order.id}?notice={action}", status_code=303)


@app.post("/admin/orders/{order_id}/send-report")
@admin_required
async def admin_send_report(request: Request, order_id: int, db: Session = Depends(get_db)):
    order = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.id == order_id).first()
    if not order:
        _public_error("Order not found.", 404)
    try:
        tasks = enqueue_final_report_delivery_tasks(order)
        log_admin_action(db, order, "send_final_report_queued", actor=_admin_actor(request), metadata={"tasks": tasks})
        db.commit()
    except ValueError as exc:
        return RedirectResponse(url=f"/admin/orders/{order.id}?error={urlencode({'message': str(exc)})}", status_code=303)
    return RedirectResponse(url=f"/admin/orders/{order.id}?notice=report_queued", status_code=303)


@app.post("/admin/orders/{order_id}/refund")
@admin_required
async def admin_refund_order(
    request: Request,
    order_id: int,
    refund_amount: str = Form(default=""),
    refund_reason: str = Form(default=""),
    refund_mode: str = Form(default="provider"),
    db: Session = Depends(get_db),
):
    order = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.id == order_id).first()
    if not order:
        _public_error("Order not found.", 404)
    try:
        request_order_refund(db, order, refund_amount=refund_amount, reason=refund_reason, actor=_admin_actor(request), refund_mode=refund_mode)
    except (ValueError, payments.PaymentError) as exc:
        return RedirectResponse(url=f"/admin/orders/{order.id}?error={urlencode({'message': str(exc)})}", status_code=303)
    return RedirectResponse(url=f"/admin/orders/{order.id}?notice=refunded", status_code=303)


@app.post("/admin/orders/{order_id}/cancel")
@admin_required
async def admin_cancel_order(
    request: Request,
    order_id: int,
    cancellation_reason: str = Form(default=""),
    admin_override: str = Form(default=""),
    db: Session = Depends(get_db),
):
    order = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.id == order_id).first()
    if not order:
        _public_error("Order not found.", 404)
    try:
        cancel_service_order(db, order, reason=cancellation_reason, actor=_admin_actor(request), admin_override=admin_override == "1")
    except ValueError as exc:
        return RedirectResponse(url=f"/admin/orders/{order.id}?error={urlencode({'message': str(exc)})}", status_code=303)
    return RedirectResponse(url=f"/admin/orders/{order.id}?notice=cancelled", status_code=303)


@app.post("/admin/orders/{order_id}/mark-no-show")
@admin_required
async def admin_mark_no_show(request: Request, order_id: int, no_show_reason: str = Form(default=""), db: Session = Depends(get_db)):
    order = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.id == order_id).first()
    if not order:
        _public_error("Order not found.", 404)
    try:
        mark_consultation_no_show(db, order, reason=no_show_reason, actor=_admin_actor(request))
    except ValueError as exc:
        return RedirectResponse(url=f"/admin/orders/{order.id}?error={urlencode({'message': str(exc)})}", status_code=303)
    return RedirectResponse(url=f"/admin/orders/{order.id}?notice=no_show", status_code=303)


@app.post("/admin/orders/{order_id}/reconcile-payment")
@admin_required
async def admin_reconcile_payment(
    request: Request,
    order_id: int,
    payment_token: str = Form(default=""),
    payment_id: str = Form(default=""),
    conversation_id: str = Form(default=""),
    db: Session = Depends(get_db),
):
    order = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.id == order_id).first()
    if not order:
        _public_error("Order not found.", 404)
    try:
        reconcile_order_payment(db, order, token=payment_token, payment_id=payment_id, conversation_id=conversation_id, actor=_admin_actor(request))
    except (ValueError, payments.PaymentError) as exc:
        return RedirectResponse(url=f"/admin/orders/{order.id}?error={urlencode({'message': str(exc)})}", status_code=303)
    return RedirectResponse(url=f"/admin/orders/{order.id}?notice=reconciled", status_code=303)


@app.get("/admin/users", response_class=HTMLResponse)
@admin_required
async def admin_users(request: Request, db: Session = Depends(get_db), q: str = "", plan: str = "", status: str = "", page: int = 1):
    query = db.query(db_mod.AppUser)
    if q.strip():
        like = f"%{q.strip().lower()}%"
        query = query.filter(or_(db_mod.AppUser.email.ilike(like), db_mod.AppUser.name.ilike(like)))
    if plan.strip():
        query = query.filter(db_mod.AppUser.plan_code == normalize_plan_code(plan))
    if status.strip():
        query = query.filter(db_mod.AppUser.subscription_status == status.strip())
    total = query.count()
    page = max(int(page or 1), 1)
    users = query.order_by(db_mod.AppUser.created_at.desc()).offset((page - 1) * 50).limit(50).all()
    for user in users:
        user.report_count = db.query(db_mod.GeneratedReport).filter(db_mod.GeneratedReport.user_id == user.id).count()
    return templates.TemplateResponse(
        request=request,
        name="admin/users.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            active_page="users",
            users=users,
            total=total,
            q=q,
            plan=plan,
            page=page,
            filters={"q": q, "plan": plan, "status": status},
        ),
    )


@app.get("/admin/users/{user_id}", response_class=HTMLResponse)
@admin_required
async def admin_user_detail(request: Request, user_id: int, db: Session = Depends(get_db)):
    user = db.query(db_mod.AppUser).filter(db_mod.AppUser.id == user_id).first()
    if not user:
        _public_error("Kullanici bulunamadi.", 404)
    reports = db.query(db_mod.GeneratedReport).filter(db_mod.GeneratedReport.user_id == user.id).order_by(db_mod.GeneratedReport.created_at.desc()).limit(10).all()
    email_logs = db.query(db_mod.EmailLog).filter(db_mod.EmailLog.recipient_email == user.email).order_by(db_mod.EmailLog.created_at.desc()).limit(10).all()
    return templates.TemplateResponse(
        request=request,
        name="admin/user_detail.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            active_page="users",
            detail_user=user,
            user_reports=[
                {
                    "report_type": report.report_type,
                    "created_at": report.created_at,
                    "status": "delivered" if report.delivered_at else ("paid" if report.is_paid else report.access_state),
                }
                for report in reports
            ],
            recent_emails=email_logs,
        ),
    )


@app.post("/admin/users/{user_id}")
@admin_required
async def admin_user_update(
    request: Request,
    user_id: int,
    name: str = Form(default=""),
    plan_code: str = Form(default="free"),
    is_admin: str = Form(default=""),
    db: Session = Depends(get_db),
):
    user = db.query(db_mod.AppUser).filter(db_mod.AppUser.id == user_id).first()
    if not user:
        _public_error("Kullanici bulunamadi.", 404)
    user.name = name.strip() or None
    user.plan_code = normalize_plan_code(plan_code)
    user.is_admin = bool(is_admin)
    user.updated_at = datetime.utcnow()
    log_action(db, _admin_actor(request), "user_update", "user", user.id, f"plan={user.plan_code} is_admin={user.is_admin}")
    db.commit()
    return RedirectResponse(url=f"/admin/users/{user.id}?notice=updated", status_code=303)


@app.post("/admin/users/{user_id}/delete")
@admin_required
async def admin_user_delete(request: Request, user_id: int, db: Session = Depends(get_db)):
    user = db.query(db_mod.AppUser).filter(db_mod.AppUser.id == user_id).first()
    if not user:
        _public_error("Kullanici bulunamadi.", 404)
    user.is_active = False
    user.updated_at = datetime.utcnow()
    log_action(db, _admin_actor(request), "user_soft_delete", "user", user.id, user.email)
    db.commit()
    return RedirectResponse(url="/admin/users?notice=deleted", status_code=303)


@app.get("/admin/reports", response_class=HTMLResponse)
@admin_required
async def admin_reports(request: Request, db: Session = Depends(get_db), report_type: str = "", user_email: str = "", status: str = "", q: str = "", page: int = 1):
    order_query = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.service_type == "report")
    if report_type.strip():
        order_query = order_query.filter(db_mod.ServiceOrder.product_type == normalize_report_type(report_type))
    if user_email.strip():
        order_query = order_query.filter(db_mod.ServiceOrder.customer_email.ilike(f"%{user_email.strip().lower()}%"))
    search = (q or user_email or "").strip()
    if search:
        like = f"%{search.lower()}%"
        order_query = order_query.filter(or_(db_mod.ServiceOrder.customer_email.ilike(like), db_mod.ServiceOrder.customer_name.ilike(like)))
    if status.strip():
        order_query = order_query.filter(db_mod.ServiceOrder.status == status.strip())
    page = max(int(page or 1), 1)
    total = order_query.count()
    service_reports = order_query.order_by(db_mod.ServiceOrder.created_at.desc()).offset((page - 1) * 50).limit(50).all()

    legacy_query = db.query(db_mod.GeneratedReport)
    if report_type.strip():
        legacy_query = legacy_query.filter(db_mod.GeneratedReport.report_type == normalize_report_type(report_type))
    if user_email.strip():
        legacy_query = legacy_query.join(db_mod.AppUser, db_mod.GeneratedReport.user_id == db_mod.AppUser.id).filter(db_mod.AppUser.email.ilike(f"%{user_email.strip().lower()}%"))
    legacy_reports = legacy_query.order_by(db_mod.GeneratedReport.created_at.desc()).limit(50).all()
    users = {user.id: user for user in db.query(db_mod.AppUser).filter(db_mod.AppUser.id.in_([report.user_id for report in legacy_reports] or [0])).all()}
    legacy_rows = []
    for report in legacy_reports:
        row = _report_view(report)
        row["user_email"] = users.get(report.user_id).email if users.get(report.user_id) else "-"
        row["profile_name"] = report.profile.profile_name if report.profile else "-"
        legacy_rows.append(row)
    return templates.TemplateResponse(
        request=request,
        name="admin/reports.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            active_page="reports",
            reports=[_service_report_row(order) for order in service_reports],
            legacy_reports=legacy_rows,
            total=total,
            q=search,
            status=status,
            report_type=report_type,
            page=page,
            filters={"report_type": report_type, "user_email": user_email, "q": search, "status": status},
        ),
    )


@app.get("/admin/reports/{report_id}", response_class=HTMLResponse)
@admin_required
async def admin_report_detail(request: Request, report_id: int, db: Session = Depends(get_db), notice: str = "", error: str = ""):
    order = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.id == report_id, db_mod.ServiceOrder.service_type == "report").first()
    if order:
        logs = db.query(db_mod.AdminActionLog).filter(db_mod.AdminActionLog.order_id == order.id).order_by(db_mod.AdminActionLog.created_at.desc()).limit(50).all()
        return templates.TemplateResponse(
            request=request,
            name="admin/report_detail.html",
            context=_auth_template_context(
                request,
                dashboard_user=request.state.admin_user,
                active_page="reports",
                order=order,
                report=_service_report_row(order),
                product=_service_order_product(order),
                payload=_service_order_payload(order),
                logs=logs,
                pdf_exists=os.path.exists(f"static/reports/{order.id}.pdf"),
                notice=notice,
                error=error,
                is_service_order=True,
            ),
        )
    report = db.query(db_mod.GeneratedReport).filter(db_mod.GeneratedReport.id == report_id).first()
    if not report:
        _public_error("Rapor bulunamadi.", 404)
    report_view = _report_view(report)
    report_view["user_email"] = report.user.email if report.user else "-"
    report_view["email"] = report.user.email if report.user else "-"
    report_view["profile_name"] = report.profile.profile_name if report.profile else "-"
    report_view["full_name"] = report.profile.profile_name if report.profile else "-"
    report_view["birth_date"] = getattr(report.profile, "birth_date", "") if report.profile else ""
    report_view["birth_time"] = getattr(report.profile, "birth_time", "") if report.profile else ""
    report_view["birth_city"] = getattr(report.profile, "birth_place", "") if report.profile else ""
    report_view["is_gift"] = False
    report_view["admin_note"] = ""
    report_view["optional_note"] = ""
    return templates.TemplateResponse(
        request=request,
        name="admin/report_detail.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            report=report_view,
            payload_summary=_report_detail_payload(report.result_payload_json),
            interpretation_summary=_report_detail_payload(report.interpretation_context_json),
            pdf_exists=os.path.exists(f"static/reports/{report.id}.pdf"),
            is_service_order=False,
            notice=notice,
            error=error,
        ),
    )


def _pdf_context_for_service_order(order):
    product = _service_order_product(order)
    payload = _service_order_payload(order)
    lang = str(getattr(order, "user_lang", None) or payload.get("user_lang") or payload.get("lang") or payload.get("language") or "tr").lower()
    lang = lang if lang in {"tr", "en"} else "tr"
    draft_text = (getattr(order, "ai_draft_text", None) or "").strip()
    blocks = []
    if draft_text:
        for paragraph in re.split(r"\n\s*\n", draft_text):
            paragraph = paragraph.strip()
            if paragraph:
                blocks.append({"type": "paragraph", "text": paragraph})
    else:
        blocks.append({"type": "paragraph", "text": "This section will be filled with report content." if lang == "en" else "Bu bölüm rapor içeriğiyle doldurulacaktır."})
    planet_names = (
        ["Sun", "Moon", "Mars", "Mercury", "Jupiter", "Venus", "Saturn", "Rahu", "Ketu"]
        if lang == "en"
        else ["Güneş", "Ay", "Mars", "Merkür", "Jüpiter", "Venüs", "Satürn", "Rahu", "Ketu"]
    )
    return {
        "lang": lang,
        "person_name": order.customer_name or order.customer_email,
        "birth_date": str(order.birth_date or ""),
        "birth_time": str(order.birth_time) if order.birth_time else None,
        "birth_city": order.birth_place or "",
        "report_label": product.get("title") or order.product_type,
        "prepared_date": date.today().strftime("%d %B %Y") if lang == "en" else date.today().strftime("%d.%m.%Y"),
        "chart": {
            "lagna": "ââ‚¬â€",
            "moon_sign": "ââ‚¬â€",
            "moon_house": "ââ‚¬â€",
            "sun_sign": "ââ‚¬â€",
            "sun_house": "ââ‚¬â€",
            "atmakaraka": "ââ‚¬â€",
            "mahadasha": "ââ‚¬â€",
            "mahadasha_end": "ââ‚¬â€",
            "antardasha": "ââ‚¬â€",
            "planets": [
                {"name": planet, "sign": "ââ‚¬â€", "house": "ââ‚¬â€", "retrograde": False}
                for planet in planet_names
            ],
        },
        "report_sections": [
            {
                "kicker": "Analysis" if lang == "en" else "Analiz",
                "title": "Report Content" if lang == "en" else "Rapor İçeriği",
                "lead": order.optional_note or "",
                "blocks": blocks,
            }
        ],
        "closing_summary": "Read through all sections carefully." if lang == "en" else "Raporunun tüm bölümlerini dikkatlice oku.",
        "key_points": [],
    }


@app.post("/admin/reports/{report_id}/generate-pdf")
@admin_required
async def admin_generate_pdf(request: Request, report_id: int, db: Session = Depends(get_db)):
    form_data = await request.form()
    if not verify_csrf_token(request, form_data.get(CSRF_FORM_FIELD, "")):
        raise HTTPException(status_code=403, detail="Invalid CSRF token")
    order = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.id == report_id, db_mod.ServiceOrder.service_type == "report").first()
    if not order:
        raise HTTPException(status_code=404)
    try:
        output_path = generate_report_pdf(report_id, _pdf_context_for_service_order(order))
        order.final_pdf_path = output_path
        order.pdf_status = "ready"
        log_action(db, _admin_actor(request), "generate_pdf", "report_order", report_id, f"PDF oluşturuldu: {output_path}")
        db.commit()
        set_flash(request, "PDF taslağı oluşturuldu.")
    except Exception as exc:
        db.rollback()
        logger.exception("Admin PDF generation failed report_id=%s", report_id)
        set_flash(request, f"PDF oluşturulamadı: {exc}", "error")
    return RedirectResponse(f"/admin/reports/{report_id}", status_code=303)


@app.get("/admin/reports/{report_id}/pdf-download")
@admin_required
async def admin_pdf_download(request: Request, report_id: int, db: Session = Depends(get_db)):
    pdf_path = f"static/reports/{report_id}.pdf"
    if not os.path.exists(pdf_path):
        raise HTTPException(status_code=404)
    return FileResponse(pdf_path, media_type="application/pdf", filename=f"FocusAstrology_Rapor_{report_id}.pdf")


@app.post("/admin/reports/{report_id}/status")
@admin_required
async def admin_report_status(request: Request, report_id: int, status: str = Form(...), admin_note: str = Form(default=""), db: Session = Depends(get_db)):
    allowed = {"pending", "in_progress", "delivered", "refunded", "paid", "draft_ready", "under_review", "ready_to_send"}
    if status not in allowed:
        _public_error("Invalid report status.", 400)
    order = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.id == report_id, db_mod.ServiceOrder.service_type == "report").first()
    if not order:
        _public_error("Report order not found.", 404)
    previous = order.status
    order.status = status
    order.admin_note = admin_note.strip() or None
    order.internal_notes = admin_note.strip() or order.internal_notes
    if status == "delivered" and not order.delivered_at:
        order.delivered_at = datetime.utcnow()
    if status == "delivered" and previous != "delivered":
        report_type_label = _service_order_product(order).get("title") or order.product_type
        pdf_path = f"static/reports/{order.id}.pdf"
        html = render_email(
            "report_delivery.html",
            name=order.customer_name or order.customer_email,
            report_type=report_type_label,
            consultation_url="https://focusastrology.com/personal-consultation",
            astrologer_name=get_astrologer_name(db),
        )
        _send_logged_email(
            db,
            email_type="report_delivery",
            to_email=order.customer_email,
            subject=f"Raporunuz hazÃ„Â±r ââ‚¬â€ {report_type_label}",
            html_body=html,
            event_type="report_delivery",
            event_key=f"report_delivery:{order.id}",
            attachment_path=pdf_path if os.path.exists(pdf_path) else None,
            attachment_filename=f"FocusAstrology_{order.customer_name or order.id}.pdf",
        )
    log_admin_action(db, order, "report_status_update", actor=_admin_actor(request), metadata={"from": previous, "to": status})
    log_action(db, _admin_actor(request), "report_status_update", "report", order.id, f"{previous}->{status}")
    db.commit()
    set_flash(request, "Rapor durumu güncellendi.")
    return RedirectResponse(url=f"/admin/reports/{order.id}?notice=status_updated", status_code=303)


@app.post("/admin/reports/bulk-status")
@admin_required
async def admin_reports_bulk_status(request: Request, db: Session = Depends(get_db)):
    form_data = await request.form()
    bulk_status = str(form_data.get("bulk_status", "")).strip()
    report_ids: list[str] = []
    for raw_id in form_data.getlist("report_ids"):
        for value in str(raw_id).replace("[", "").replace("]", "").replace("'", "").replace('"', "").split(","):
            value = value.strip()
            if value:
                report_ids.append(value)
    valid_statuses = {"pending", "in_progress", "delivered", "refunded"}
    if not bulk_status or bulk_status not in valid_statuses or not report_ids:
        set_flash(request, "Toplu güncelleme için rapor ve durum seçin.", "error")
        return RedirectResponse("/admin/reports", status_code=303)

    updated_count = 0
    for rid in report_ids:
        try:
            order = (
                db.query(db_mod.ServiceOrder)
                .filter(db_mod.ServiceOrder.id == int(rid), db_mod.ServiceOrder.service_type == "report")
                .first()
            )
            if order:
                order.status = bulk_status
                if bulk_status == "delivered" and not order.delivered_at:
                    order.delivered_at = datetime.utcnow()
                updated_count += 1
        except (TypeError, ValueError):
            continue
    log_action(db, _admin_actor(request), "bulk_status_update", "report_order", None, f"{updated_count} siparişin durumu '{bulk_status}' olarak güncellendi")
    db.commit()
    set_flash(request, f"{updated_count} sipariş güncellendi.")
    return RedirectResponse("/admin/reports", status_code=303)


@app.post("/admin/reports/{report_id}/approve")
@admin_required
async def admin_report_approve(request: Request, report_id: int, db: Session = Depends(get_db)):
    order = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.id == report_id, db_mod.ServiceOrder.service_type == "report").first()
    if not order:
        _public_error("Report order not found.", 404)
    try:
        _validate_order_paid(order)
        if order.status not in {"paid", "draft_pending", "draft_sent_to_admin", "draft_ready", "under_review", "ready_to_send"}:
            raise ValueError(f"Report cannot be approved from {order.status}.")
        now = datetime.utcnow()
        order.status = "ready_to_send"
        order.review_started_at = order.review_started_at or now
        order.ready_to_send_at = order.ready_to_send_at or now
        log_admin_action(db, order, "approve_report", actor=_admin_actor(request), metadata={"to": "ready_to_send"})
        log_action(db, _admin_actor(request), "approve_report", "report", order.id, "ready_to_send")
        db.commit()
    except ValueError as exc:
        return RedirectResponse(url=f"/admin/reports/{order.id}?error={urlencode({'message': str(exc)})}", status_code=303)
    return RedirectResponse(url=f"/admin/reports/{order.id}?notice=approved", status_code=303)


@app.post("/admin/reports/{report_id}/send")
@admin_required
async def admin_report_send(request: Request, report_id: int, db: Session = Depends(get_db)):
    order = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.id == report_id, db_mod.ServiceOrder.service_type == "report").first()
    if not order:
        _public_error("Report order not found.", 404)
    try:
        tasks = enqueue_final_report_delivery_tasks(order)
        log_admin_action(db, order, "send_final_report_queued", actor=_admin_actor(request), metadata={"tasks": tasks})
        log_action(db, _admin_actor(request), "send_final_report_queued", "report", order.id, json.dumps(tasks, default=str))
        db.commit()
    except ValueError as exc:
        return RedirectResponse(url=f"/admin/reports/{order.id}?error={urlencode({'message': str(exc)})}", status_code=303)
    return RedirectResponse(url=f"/admin/reports/{order.id}?notice=report_queued", status_code=303)


@app.post("/admin/reports/{report_id}/regenerate")
@admin_required
async def admin_report_regenerate(request: Request, report_id: int, db: Session = Depends(get_db)):
    order = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.id == report_id, db_mod.ServiceOrder.service_type == "report").first()
    if not order:
        _public_error("Report order not found.", 404)
    try:
        _validate_order_paid(order)
        from report_tasks import generate_ai_draft_task

        order.ai_draft_status = "pending"
        order.last_task_error = None
        task = generate_ai_draft_task.delay(order.id)
        log_admin_action(db, order, "regenerate_report_queued", actor=_admin_actor(request), metadata={"task_id": task.id})
        log_action(db, _admin_actor(request), "regenerate_report_queued", "report", order.id, task.id)
        db.commit()
    except ValueError as exc:
        return RedirectResponse(url=f"/admin/reports/{order.id}?error={urlencode({'message': str(exc)})}", status_code=303)
    return RedirectResponse(url=f"/admin/reports/{order.id}?notice=regeneration_queued", status_code=303)


@app.get("/admin/consultations", response_class=HTMLResponse)
@admin_required
async def admin_consultations(request: Request, db: Session = Depends(get_db), status: str = "", q: str = "", page: int = 1):
    query = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.service_type == "consultation")
    if status.strip():
        query = query.filter(db_mod.ServiceOrder.status == status.strip())
    if q.strip():
        like = f"%{q.strip().lower()}%"
        query = query.filter(or_(db_mod.ServiceOrder.customer_email.ilike(like), db_mod.ServiceOrder.customer_name.ilike(like)))
    page = max(int(page or 1), 1)
    total = query.count()
    consultations = query.order_by(db_mod.ServiceOrder.scheduled_start.desc().nullslast(), db_mod.ServiceOrder.created_at.desc()).offset((page - 1) * 50).limit(50).all()
    return templates.TemplateResponse(
        request=request,
        name="admin/consultations.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            active_page="consultations",
            consultations=[_consultation_order_row(order) for order in consultations],
            total=total,
            q=q,
            status=status,
            page=page,
            filters={"status": status},
        ),
    )


@app.post("/admin/consultations/{consultation_id}/status")
@admin_required
async def admin_consultation_status(request: Request, consultation_id: int, status: str = Form(...), db: Session = Depends(get_db)):
    allowed = {"pending", "confirmed", "completed", "cancelled", "booking_pending_payment", "paid", "prepared"}
    if status not in allowed:
        _public_error("Invalid consultation status.", 400)
    order = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.id == consultation_id, db_mod.ServiceOrder.service_type == "consultation").first()
    if not order:
        _public_error("Consultation not found.", 404)
    previous = order.status
    order.status = status
    now = datetime.utcnow()
    if status == "confirmed" and not order.confirmed_at:
        order.confirmed_at = now
    if status == "prepared" and not order.prepared_at:
        order.prepared_at = now
    if status == "completed" and not order.completed_at:
        order.completed_at = now
    log_admin_action(db, order, "consultation_status_update", actor=_admin_actor(request), metadata={"from": previous, "to": status})
    log_action(db, _admin_actor(request), "consultation_status_update", "consultation", order.id, f"{previous}->{status}")
    db.commit()
    return RedirectResponse(url="/admin/consultations?notice=status_updated", status_code=303)


@app.get("/admin/content", response_class=HTMLResponse)
@admin_required
async def admin_content(request: Request, db: Session = Depends(get_db)):
    articles = db.query(db_mod.Article).order_by(db_mod.Article.updated_at.desc(), db_mod.Article.created_at.desc()).limit(250).all()
    return templates.TemplateResponse(
        request=request,
        name="admin/content_list.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            articles=[_article_admin_view(article) for article in articles],
        ),
    )


@app.get("/admin/content/new", response_class=HTMLResponse)
@admin_required
async def admin_content_new(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse(
        request=request,
        name="admin/content_form.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            article={"id": None, "title": "", "slug": "", "content": "", "status": "draft"},
            mode="new",
            error="",
        ),
    )


@app.post("/admin/content/new")
@admin_required
async def admin_content_create(
    request: Request,
    title: str = Form(...),
    slug: str = Form(default=""),
    content: str = Form(default=""),
    status: str = Form(default="draft"),
    db: Session = Depends(get_db),
):
    normalized_status = "published" if status == "published" else "draft"
    article = db_mod.Article(
        title=title.strip(),
        slug=_unique_article_slug(db, slug.strip() or title.strip()),
        category="general",
        excerpt=(content or "").strip()[:220],
        body=(content or "").strip(),
        is_published=normalized_status == "published",
        published_at=datetime.utcnow() if normalized_status == "published" else None,
        author_name="Focus Astrology",
        language="tr",
    )
    db.add(article)
    db.commit()
    return RedirectResponse(url="/admin/content", status_code=303)


@app.get("/admin/content/{article_id}/edit", response_class=HTMLResponse)
@admin_required
async def admin_content_edit(request: Request, article_id: int, db: Session = Depends(get_db)):
    article = db.query(db_mod.Article).filter(db_mod.Article.id == article_id).first()
    if not article:
        _public_error("Content not found.", 404)
    return templates.TemplateResponse(
        request=request,
        name="admin/content_form.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            article=_article_admin_view(article),
            mode="edit",
            error="",
        ),
    )


@app.post("/admin/content/{article_id}/edit")
@admin_required
async def admin_content_update(
    request: Request,
    article_id: int,
    title: str = Form(...),
    slug: str = Form(default=""),
    content: str = Form(default=""),
    status: str = Form(default="draft"),
    db: Session = Depends(get_db),
):
    article = db.query(db_mod.Article).filter(db_mod.Article.id == article_id).first()
    if not article:
        _public_error("Content not found.", 404)
    normalized_status = "published" if status == "published" else "draft"
    article.title = title.strip()
    article.slug = _unique_article_slug(db, slug.strip() or title.strip(), article_id=article.id)
    article.body = (content or "").strip()
    article.excerpt = article.body[:220]
    article.is_published = normalized_status == "published"
    if article.is_published and not article.published_at:
        article.published_at = datetime.utcnow()
    article.updated_at = datetime.utcnow()
    db.commit()
    return RedirectResponse(url="/admin/content", status_code=303)


@app.get("/admin/articles", response_class=HTMLResponse)
@admin_required
async def admin_articles(request: Request, db: Session = Depends(get_db)):
    query = db.query(db_mod.Article)
    total = query.count()
    articles = query.order_by(db_mod.Article.updated_at.desc(), db_mod.Article.created_at.desc()).limit(250).all()
    return templates.TemplateResponse(
        request=request,
        name="admin/articles.html",
        context=_auth_template_context(request, dashboard_user=request.state.admin_user, active_page="articles", articles=articles, total=total),
    )


def _admin_article_categories(db):
    slugs = [
        row[0]
        for row in db.query(db_mod.Article.category).distinct().order_by(db_mod.Article.category.asc()).all()
        if row[0]
    ]
    for fallback in ("general", "vedic-astrology", "transits"):
        if fallback not in slugs:
            slugs.append(fallback)
    return [{"id": slug, "slug": slug} for slug in slugs]


@app.get("/admin/articles/new", response_class=HTMLResponse)
@admin_required
async def admin_article_new(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse(
        request=request,
        name="admin/article_form.html",
        context=_auth_template_context(request, dashboard_user=request.state.admin_user, active_page="articles", article=None, mode="new", categories=_admin_article_categories(db)),
    )


@app.post("/admin/articles/new")
@admin_required
async def admin_article_create(
    request: Request,
    title: str = Form(...),
    slug: str = Form(default=""),
    excerpt: str = Form(default=""),
    content: str = Form(default=""),
    category_id: str = Form(default="general"),
    cover_image: str = Form(default=""),
    reading_time: int = Form(default=4),
    meta_title: str = Form(default=""),
    meta_description: str = Form(default=""),
    is_published: str = Form(default=""),
    db: Session = Depends(get_db),
):
    article = db_mod.Article(
        title=title.strip(),
        slug=_unique_article_slug(db, slug.strip() or title.strip()),
        category=category_id.strip() or "general",
        excerpt=excerpt.strip(),
        body=content.strip(),
        content=content.strip(),
        cover_image=cover_image.strip() or None,
        reading_time=reading_time,
        meta_title=meta_title.strip() or None,
        meta_description=meta_description.strip() or None,
        is_published=bool(is_published),
        published_at=datetime.utcnow() if is_published else None,
        author_name="Focus Astrology",
        language="tr",
    )
    db.add(article)
    db.flush()
    log_action(db, _admin_actor(request), "article_create", "article", article.id, article.title)
    db.commit()
    return RedirectResponse(url="/admin/articles?notice=created", status_code=303)


@app.get("/admin/articles/{article_id}/edit", response_class=HTMLResponse)
@admin_required
async def admin_article_edit(request: Request, article_id: int, db: Session = Depends(get_db)):
    article = db.query(db_mod.Article).filter(db_mod.Article.id == article_id).first()
    if not article:
        _public_error("Article not found.", 404)
    return templates.TemplateResponse(
        request=request,
        name="admin/article_form.html",
        context=_auth_template_context(request, dashboard_user=request.state.admin_user, active_page="articles", article=article, mode="edit", categories=_admin_article_categories(db)),
    )


@app.post("/admin/articles/{article_id}/edit")
@admin_required
async def admin_article_update(
    request: Request,
    article_id: int,
    title: str = Form(...),
    slug: str = Form(default=""),
    excerpt: str = Form(default=""),
    content: str = Form(default=""),
    category_id: str = Form(default="general"),
    cover_image: str = Form(default=""),
    reading_time: int = Form(default=4),
    meta_title: str = Form(default=""),
    meta_description: str = Form(default=""),
    is_published: str = Form(default=""),
    db: Session = Depends(get_db),
):
    article = db.query(db_mod.Article).filter(db_mod.Article.id == article_id).first()
    if not article:
        _public_error("Article not found.", 404)
    article.title = title.strip()
    article.slug = _unique_article_slug(db, slug.strip() or title.strip(), article_id=article.id)
    article.category = category_id.strip() or "general"
    article.excerpt = excerpt.strip()
    article.body = content.strip()
    article.content = content.strip()
    article.cover_image = cover_image.strip() or None
    article.reading_time = reading_time
    article.meta_title = meta_title.strip() or None
    article.meta_description = meta_description.strip() or None
    article.is_published = bool(is_published)
    if article.is_published and not article.published_at:
        article.published_at = datetime.utcnow()
    article.updated_at = datetime.utcnow()
    log_action(db, _admin_actor(request), "article_update", "article", article.id, article.title)
    db.commit()
    return RedirectResponse(url="/admin/articles?notice=updated", status_code=303)


@app.post("/admin/articles/{article_id}/delete")
@admin_required
async def admin_article_delete(request: Request, article_id: int, db: Session = Depends(get_db)):
    article = db.query(db_mod.Article).filter(db_mod.Article.id == article_id).first()
    if not article:
        _public_error("Article not found.", 404)
    log_action(db, _admin_actor(request), "article_delete", "article", article.id, article.title)
    db.delete(article)
    db.commit()
    return RedirectResponse(url="/admin/articles?notice=deleted", status_code=303)


@app.get("/admin/billing", response_class=HTMLResponse)
@admin_required
async def admin_billing(request: Request, db: Session = Depends(get_db)):
    paid_users = db.query(db_mod.AppUser).filter(db_mod.AppUser.plan_code.in_(["basic", "premium", "elite"])).order_by(db_mod.AppUser.plan_started_at.desc()).all()
    billing_emails = db.query(db_mod.EmailLog).filter(db_mod.EmailLog.related_event_type.isnot(None)).order_by(db_mod.EmailLog.created_at.desc()).limit(50).all()
    return templates.TemplateResponse(
        request=request,
        name="admin/billing.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            paid_users=[_user_admin_view(user, db.query(db_mod.GeneratedReport).filter(db_mod.GeneratedReport.user_id == user.id).count()) for user in paid_users],
            billing_logs=[_email_log_view(log) for log in billing_emails],
            plan_distribution={plan: db.query(db_mod.AppUser).filter(db_mod.AppUser.plan_code == plan).count() for plan in PLAN_FEATURES},
        ),
    )


@app.get("/admin/emails", response_class=HTMLResponse)
@admin_required
async def admin_emails(request: Request, db: Session = Depends(get_db), status: str = "", email_type: str = "", recipient: str = ""):
    query = db.query(db_mod.EmailLog)
    if status.strip():
        query = query.filter(db_mod.EmailLog.status == status.strip())
    if email_type.strip():
        query = query.filter(db_mod.EmailLog.email_type == email_type.strip())
    if recipient.strip():
        query = query.filter(db_mod.EmailLog.recipient_email.ilike(f"%{recipient.strip().lower()}%"))
    logs = query.order_by(db_mod.EmailLog.created_at.desc()).limit(250).all()
    return templates.TemplateResponse(
        request=request,
        name="admin/emails.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            email_logs=[_email_log_admin_view(db, log) for log in logs],
            filters={"status": status, "email_type": email_type, "recipient": recipient},
        ),
    )


@app.get("/admin/faq", response_class=HTMLResponse)
@admin_required
async def admin_faq(request: Request, db: Session = Depends(get_db)):
    items = db.query(db_mod.FAQItem).order_by(db_mod.FAQItem.category.asc(), db_mod.FAQItem.sort_order.asc(), db_mod.FAQItem.id.asc()).all()
    faq_by_category = {}
    for item in items:
        faq_by_category.setdefault(item.category, []).append(item)
    return templates.TemplateResponse(
        request=request,
        name="admin/faq.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            active_page="faq",
            faq_items=items,
            faq_by_category=faq_by_category,
            total=len(items),
        ),
    )


@app.get("/admin/faq/new", response_class=HTMLResponse)
@admin_required
async def admin_faq_new(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse(request=request, name="admin/faq_form.html", context=_auth_template_context(request, dashboard_user=request.state.admin_user, active_page="faq", item=None, mode="new"))


@app.post("/admin/faq/new")
@admin_required
async def admin_faq_create(
    request: Request,
    category: str = Form(...),
    question_tr: str = Form(...),
    question_en: str = Form(...),
    answer_tr: str = Form(...),
    answer_en: str = Form(...),
    sort_order: int = Form(default=0),
    is_published: str = Form(default=""),
    db: Session = Depends(get_db),
):
    item = db_mod.FAQItem(category=category, question_tr=question_tr, question_en=question_en, answer_tr=answer_tr, answer_en=answer_en, sort_order=sort_order, is_published=bool(is_published))
    db.add(item)
    db.flush()
    log_action(db, _admin_actor(request), "faq_create", "faq", item.id, item.question_tr[:120])
    db.commit()
    return RedirectResponse(url="/admin/faq?notice=created", status_code=303)


@app.get("/admin/faq/{faq_id}/edit", response_class=HTMLResponse)
@admin_required
async def admin_faq_edit(request: Request, faq_id: int, db: Session = Depends(get_db)):
    item = db.query(db_mod.FAQItem).filter(db_mod.FAQItem.id == faq_id).first()
    if not item:
        _public_error("FAQ not found.", 404)
    return templates.TemplateResponse(request=request, name="admin/faq_form.html", context=_auth_template_context(request, dashboard_user=request.state.admin_user, active_page="faq", item=item, mode="edit"))


@app.post("/admin/faq/{faq_id}/edit")
@admin_required
async def admin_faq_update(
    request: Request,
    faq_id: int,
    category: str = Form(...),
    question_tr: str = Form(...),
    question_en: str = Form(...),
    answer_tr: str = Form(...),
    answer_en: str = Form(...),
    sort_order: int = Form(default=0),
    is_published: str = Form(default=""),
    db: Session = Depends(get_db),
):
    item = db.query(db_mod.FAQItem).filter(db_mod.FAQItem.id == faq_id).first()
    if not item:
        _public_error("FAQ not found.", 404)
    item.category = category
    item.question_tr = question_tr
    item.question_en = question_en
    item.answer_tr = answer_tr
    item.answer_en = answer_en
    item.sort_order = sort_order
    item.is_published = bool(is_published)
    item.updated_at = datetime.utcnow()
    log_action(db, _admin_actor(request), "faq_update", "faq", item.id, item.question_tr[:120])
    db.commit()
    return RedirectResponse(url="/admin/faq?notice=updated", status_code=303)


@app.post("/admin/faq/{faq_id}/delete")
@admin_required
async def admin_faq_delete(request: Request, faq_id: int, db: Session = Depends(get_db)):
    item = db.query(db_mod.FAQItem).filter(db_mod.FAQItem.id == faq_id).first()
    if not item:
        _public_error("FAQ not found.", 404)
    log_action(db, _admin_actor(request), "faq_delete", "faq", item.id, item.question_tr[:120])
    db.delete(item)
    db.commit()
    return RedirectResponse(url="/admin/faq?notice=deleted", status_code=303)


@app.get("/admin/gifts", response_class=HTMLResponse)
@admin_required
async def admin_gifts(request: Request, db: Session = Depends(get_db)):
    gifts = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.service_type == "report", db_mod.ServiceOrder.is_gift.is_(True)).order_by(db_mod.ServiceOrder.created_at.desc()).limit(250).all()
    return templates.TemplateResponse(
        request=request,
        name="admin/gifts.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            active_page="gifts",
            gifts=[_service_report_row(order) for order in gifts],
            total=len(gifts),
        ),
    )


def _accounting_context(request: Request, **extra):
    return _auth_template_context(
        request,
        dashboard_user=request.state.admin_user,
        active_page="accounting",
        accounting_disclaimer="Operational tracking and tax estimates only. This does not replace official accounting, e-invoice, or tax filing advice.",
        **extra,
    )


def _parse_date_filter(value: str, end_of_day: bool = False):
    value = str(value or "").strip()
    if not value:
        return None
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d")
        if end_of_day:
            parsed = parsed.replace(hour=23, minute=59, second=59)
        return parsed
    except ValueError:
        return None


def _sync_accounting(db):
    transaction_service.sync_paid_orders(db)
    reminders_service.create_operational_reminders(db)
    db.commit()


def _accounting_dashboard_trends(transactions):
    today = datetime.utcnow().date()
    rows = {
        today - timedelta(days=offset): {"date": today - timedelta(days=offset), "revenue": Decimal("0.00"), "transactions": 0, "refunds": Decimal("0.00")}
        for offset in range(29, -1, -1)
    }
    for transaction in transactions:
        paid_at = getattr(transaction, "paid_at", None)
        if not paid_at:
            continue
        day = paid_at.date()
        if day not in rows:
            continue
        rows[day]["revenue"] += _decimal_amount(getattr(transaction, "net_amount", 0))
        rows[day]["transactions"] += 1
        rows[day]["refunds"] += _decimal_amount(getattr(transaction, "refunded_amount", 0))
    return list(rows.values())


def _accounting_product_performance(transactions):
    grouped = defaultdict(lambda: {"product_type": "", "count": 0, "gross": Decimal("0.00"), "net": Decimal("0.00")})
    for transaction in transactions:
        key = getattr(transaction, "product_type", None) or getattr(transaction, "service_type", None) or "unknown"
        grouped[key]["product_type"] = key
        grouped[key]["count"] += 1
        grouped[key]["gross"] += _decimal_amount(getattr(transaction, "gross_amount", 0))
        grouped[key]["net"] += _decimal_amount(getattr(transaction, "net_amount", 0))
    return sorted(grouped.values(), key=lambda row: row["net"], reverse=True)


@app.get("/admin/accounting", response_class=HTMLResponse)
@admin_required
async def admin_accounting_dashboard(request: Request, db: Session = Depends(get_db)):
    _sync_accounting(db)
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    month_start = today.replace(day=1)
    previous_month_day = month_start - timedelta(days=1)
    trend_start = today - timedelta(days=29)
    transactions = db.query(db_mod.Transaction).all()
    trend_transactions = db.query(db_mod.Transaction).filter(db_mod.Transaction.paid_at >= trend_start).all()
    today_transactions = db.query(db_mod.Transaction).filter(db_mod.Transaction.paid_at >= today).all()
    month_transactions = db.query(db_mod.Transaction).filter(db_mod.Transaction.paid_at >= month_start).all()
    tax = tax_service.tax_overview(db, month_transactions)
    paid_count = db.query(db_mod.Transaction).filter(db_mod.Transaction.payment_status == "paid").count()
    invoiced_count = db.query(db_mod.Transaction).filter(db_mod.Transaction.invoice_status.in_(["issued", "sent"])).count()
    invoice_queue_rows, _invoice_queue_summary = invoice_service.invoice_queue_rows(db, tab="ready")
    missing_billing_customers = (
        db.query(db_mod.Customer)
        .join(db_mod.Transaction, db_mod.Transaction.customer_id == db_mod.Customer.id)
        .filter(db_mod.Transaction.payment_status == "paid")
        .filter(or_(db_mod.Customer.tax_id.is_(None), db_mod.Customer.billing_address.is_(None)))
        .distinct()
        .limit(8)
        .all()
    )
    recent_transactions = db.query(db_mod.Transaction).order_by(db_mod.Transaction.paid_at.desc().nullslast(), db_mod.Transaction.created_at.desc()).limit(8).all()
    recent_expenses = db.query(db_mod.Expense).order_by(db_mod.Expense.expense_date.desc()).limit(6).all()
    return templates.TemplateResponse(
        request=request,
        name="admin/accounting/dashboard.html",
        context=_accounting_context(
            request,
            today_revenue=tax_service.tax_overview(db, today_transactions)["net_sales"],
            monthly_revenue=tax["net_sales"],
            total_transactions=len(transactions),
            refunded_transactions=db.query(db_mod.Transaction).filter(db_mod.Transaction.payment_status == "refunded").count(),
            issued_invoices=db.query(db_mod.Invoice).filter(db_mod.Invoice.status.in_(["issued", "sent"])).count(),
            pending_invoices=db.query(db_mod.Invoice).filter(db_mod.Invoice.status.in_(["draft", "issued"])).count(),
            invoices_awaiting_send=db.query(db_mod.Invoice).filter(db_mod.Invoice.status.in_(["issued", "sent"]), db_mod.Invoice.pdf_status == "ready", db_mod.Invoice.send_status.in_(["not_sent", "failed"])).count(),
            uninvoiced_paid_transactions=db.query(db_mod.Transaction).filter(db_mod.Transaction.payment_status == "paid", db_mod.Transaction.invoice_status == "uninvoiced").count(),
            estimated_vat=tax["estimated_vat"],
            estimated_tax=tax["estimated_tax"],
            commission_totals=tax["commission_totals"],
            net_retained_amount=tax["net_retained"],
            open_reminders=db.query(db_mod.Reminder).filter(db_mod.Reminder.status == "open").count(),
            successful_transactions=db.query(db_mod.Transaction).filter(db_mod.Transaction.payment_status == "paid").count(),
            invoice_coverage=invoice_coverage_ratio(paid_count, invoiced_count),
            collection_ratio=collection_ratio(tax["gross_sales"], tax["net_sales"]),
            reminders=db.query(db_mod.Reminder).filter(db_mod.Reminder.status == "open").order_by(db_mod.Reminder.due_date.asc().nullslast()).limit(8).all(),
            trend_rows=_accounting_dashboard_trends(trend_transactions),
            invoice_queue=invoice_queue_rows[:8],
            missing_billing_customers=missing_billing_customers,
            product_performance=_accounting_product_performance(month_transactions)[:8],
            recent_transactions=recent_transactions,
            recent_expenses=recent_expenses,
            current_month_review=db.query(db_mod.TaxPeriod).filter(db_mod.TaxPeriod.period_key == month_close_service.period_key(today.year, today.month)).first(),
            previous_month_review=db.query(db_mod.TaxPeriod).filter(db_mod.TaxPeriod.period_key == month_close_service.period_key(previous_month_day.year, previous_month_day.month)).first(),
            current_month_blockers=len([c for c in month_close_service.readiness_checklist(db, today.year, today.month) if c["state"] == "blocker"]),
        ),
    )


@app.get("/admin/accounting/month-close", response_class=HTMLResponse)
@admin_required
async def admin_accounting_month_close(request: Request, db: Session = Depends(get_db), year: int | None = None, month: int | None = None):
    _sync_accounting(db)
    now = datetime.utcnow()
    target_year = int(year or now.year)
    target_month = max(1, min(12, int(month or now.month)))
    period = month_close_service.get_or_create_period(db, target_year, target_month)
    summary = month_close_service.monthly_summary(db, target_year, target_month)
    checks = month_close_service.readiness_checklist(db, target_year, target_month)
    highlights = month_close_service.activity_highlights(db, target_year, target_month)
    db.commit()
    return templates.TemplateResponse(
        request=request,
        name="admin/accounting/month_close.html",
        context=_accounting_context(
            request,
            year=target_year,
            month=target_month,
            period=period,
            summary=summary,
            checks=checks,
            highlights=highlights,
            blocker_count=len([c for c in checks if c["state"] == "blocker"]),
            warning_count=len([c for c in checks if c["state"] == "warning"]),
            export_query=f"year={target_year}&month={target_month}",
        ),
    )


@app.post("/admin/accounting/month-close/{year}/{month}/mark-reviewed")
@admin_required
async def admin_accounting_month_close_mark_reviewed(request: Request, year: int, month: int, db: Session = Depends(get_db)):
    _sync_accounting(db)
    form = await request.form()
    note = str(form.get("notes", "") or "").strip()
    confirmed = str(form.get("confirm_warnings", "") or "") == "on"
    result = month_close_service.mark_reviewed(db, year, month, _admin_actor(request), note=note, confirmed=confirmed)
    if not result["ok"]:
        db.rollback()
        set_flash(request, result["reason"], "error")
        return RedirectResponse(f"/admin/accounting/month-close?year={year}&month={month}", status_code=303)
    log_action(db, _admin_actor(request), "accounting_month_reviewed", "tax_period", result["period"].id, result["period"].period_key)
    db.commit()
    set_flash(request, "Month reviewed for internal operational tracking.")
    return RedirectResponse(f"/admin/accounting/month-close?year={year}&month={month}", status_code=303)


@app.get("/admin/accounting/transactions", response_class=HTMLResponse)
@admin_required
async def admin_accounting_transactions(request: Request, db: Session = Depends(get_db), start_date: str = "", end_date: str = "", product_type: str = "", payment_status: str = "", invoice_status: str = ""):
    _sync_accounting(db)
    query = transaction_service.filtered_transactions(
        db,
        start_date=_parse_date_filter(start_date),
        end_date=_parse_date_filter(end_date, end_of_day=True),
        product_type=product_type,
        payment_status=payment_status,
        invoice_status=invoice_status,
    )
    transactions = query.limit(250).all()
    return templates.TemplateResponse(
        request=request,
        name="admin/accounting/transactions.html",
        context=_accounting_context(request, transactions=transactions, total=query.count(), start_date=start_date, end_date=end_date, product_type=product_type, payment_status=payment_status, invoice_status=invoice_status),
    )


@app.get("/admin/accounting/transactions/{transaction_id}", response_class=HTMLResponse)
@admin_required
async def admin_accounting_transaction_detail(request: Request, transaction_id: int, db: Session = Depends(get_db)):
    _sync_accounting(db)
    transaction = db.query(db_mod.Transaction).filter(db_mod.Transaction.id == transaction_id).first()
    if not transaction:
        raise HTTPException(status_code=404)
    invoices = db.query(db_mod.Invoice).filter(db_mod.Invoice.transaction_id == transaction.id).order_by(db_mod.Invoice.created_at.desc()).all()
    return templates.TemplateResponse(request=request, name="admin/accounting/transaction_detail.html", context=_accounting_context(request, transaction=transaction, invoices=invoices))


@app.get("/admin/accounting/invoices", response_class=HTMLResponse)
@admin_required
async def admin_accounting_invoices(
    request: Request,
    db: Session = Depends(get_db),
    status: str = "",
    tab: str = "all",
    start_date: str = "",
    end_date: str = "",
    q: str = "",
    product_type: str = "",
    invoice_view: str = "all",
    invoice_q: str = "",
    invoice_start_date: str = "",
    invoice_end_date: str = "",
):
    _sync_accounting(db)
    query = db.query(db_mod.Invoice).order_by(db_mod.Invoice.created_at.desc())
    if status:
        query = query.filter(db_mod.Invoice.status == status)
    if invoice_view == "draft":
        query = query.filter(db_mod.Invoice.status == "draft")
    elif invoice_view == "issued":
        query = query.filter(db_mod.Invoice.status == "issued")
    elif invoice_view == "pdf_ready":
        query = query.filter(db_mod.Invoice.pdf_status == "ready")
    elif invoice_view == "awaiting_send":
        query = query.filter(db_mod.Invoice.status.in_(["issued", "sent"]), db_mod.Invoice.pdf_status == "ready", db_mod.Invoice.send_status.in_(["not_sent", "failed"]))
    elif invoice_view == "send_failed":
        query = query.filter(db_mod.Invoice.send_status == "failed")
    elif invoice_view == "cancelled":
        query = query.filter(db_mod.Invoice.status == "cancelled")
    if invoice_start_date:
        parsed = _parse_date_filter(invoice_start_date)
        if parsed:
            query = query.filter(db_mod.Invoice.created_at >= parsed)
    if invoice_end_date:
        parsed = _parse_date_filter(invoice_end_date, end_of_day=True)
        if parsed:
            query = query.filter(db_mod.Invoice.created_at <= parsed)
    if invoice_q:
        like = f"%{invoice_q.strip().lower()}%"
        query = query.outerjoin(db_mod.Customer, db_mod.Invoice.customer_id == db_mod.Customer.id).filter(
            or_(db_mod.Invoice.invoice_number.ilike(like), db_mod.Customer.email.ilike(like), db_mod.Customer.name.ilike(like))
        )
    invoices = query.limit(250).all()
    queue_rows, queue_summary = invoice_service.invoice_queue_rows(
        db,
        tab=tab,
        start_date=_parse_date_filter(start_date),
        end_date=_parse_date_filter(end_date, end_of_day=True),
        q=q,
        product_type=product_type,
    )
    return templates.TemplateResponse(
        request=request,
        name="admin/accounting/invoices.html",
        context=_accounting_context(
            request,
            invoices=invoices,
            queue_rows=queue_rows,
            queue_summary=queue_summary,
            total=query.count(),
            status=status,
            tab=tab or "all",
            start_date=start_date,
            end_date=end_date,
            q=q,
            product_type=product_type,
            invoice_view=invoice_view or "all",
            invoice_q=invoice_q,
            invoice_start_date=invoice_start_date,
            invoice_end_date=invoice_end_date,
            awaiting_send_count=db.query(db_mod.Invoice).filter(db_mod.Invoice.status.in_(["issued", "sent"]), db_mod.Invoice.pdf_status == "ready", db_mod.Invoice.send_status.in_(["not_sent", "failed"])).count(),
            send_failed_count=db.query(db_mod.Invoice).filter(db_mod.Invoice.send_status == "failed").count(),
        ),
    )


@app.post("/admin/accounting/invoices/create-draft")
@admin_required
async def admin_accounting_invoice_create_draft(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    transaction_id = int(form.get("transaction_id", "0"))
    transaction = db.query(db_mod.Transaction).filter(db_mod.Transaction.id == transaction_id).first()
    if not transaction:
        raise HTTPException(status_code=404)
    readiness = invoice_service.invoice_readiness(db, transaction)
    if not readiness["can_create"] and not readiness.get("invoice"):
        set_flash(request, f"Draft not created: {readiness['label']} ({readiness.get('reason') or 'not ready'}).", "error")
        return RedirectResponse("/admin/accounting/invoices", status_code=303)
    invoice = invoice_service.create_draft_invoice(db, transaction_id)
    log_action(db, _admin_actor(request), "accounting_invoice_draft", "invoice", invoice.id, f"transaction={transaction_id}")
    db.commit()
    set_flash(request, "Draft invoice created.")
    return RedirectResponse(f"/admin/accounting/invoices/{invoice.id}", status_code=303)


@app.post("/admin/accounting/invoices/bulk")
@admin_required
async def admin_accounting_invoice_bulk(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    action = str(form.get("bulk_action", "")).strip()
    transaction_ids = [value for value in form.getlist("transaction_ids") if str(value).strip()]
    if action != "create_drafts" or not transaction_ids:
        set_flash(request, "Select ready transactions and a bulk action.", "error")
        return RedirectResponse("/admin/accounting/invoices", status_code=303)
    result = invoice_service.create_drafts_for_ready_transactions(db, transaction_ids)
    for invoice in result["created"]:
        log_action(db, _admin_actor(request), "accounting_invoice_bulk_draft", "invoice", invoice.id, f"transaction={invoice.transaction_id}")
    db.commit()
    set_flash(request, f"{len(result['created'])} draft invoice(s) created. {len(result['skipped'])} skipped.")
    return RedirectResponse("/admin/accounting/invoices?tab=draft_exists", status_code=303)


@app.post("/admin/accounting/transactions/{transaction_id}/billing-info")
@admin_required
async def admin_accounting_transaction_billing_info(request: Request, transaction_id: int, db: Session = Depends(get_db)):
    form = await request.form()
    return_to = str(form.get("return_to", "") or "").strip() or "/admin/accounting/invoices"
    if not return_to.startswith("/admin/accounting/invoices"):
        return_to = "/admin/accounting/invoices"
    result = invoice_service.update_customer_billing_info(db, transaction_id, form)
    if not result["ok"]:
        set_flash(request, "Billing info not saved: " + " ".join(result["errors"]), "error")
        return RedirectResponse(return_to, status_code=303)
    readiness = result["readiness"]
    log_action(db, _admin_actor(request), "accounting_customer_billing_update", "transaction", transaction_id, readiness["status"])
    db.commit()
    if readiness["can_create"]:
        set_flash(request, "Billing info saved. Transaction is now ready to invoice.")
        if "tab=missing_info" in return_to or "tab=blocked" in return_to:
            return_to = "/admin/accounting/invoices?tab=ready"
    else:
        set_flash(request, f"Billing info saved. Current state: {readiness['label']}.")
    return RedirectResponse(return_to, status_code=303)


@app.get("/admin/accounting/invoices/{invoice_id}", response_class=HTMLResponse)
@admin_required
async def admin_accounting_invoice_detail(request: Request, invoice_id: int, db: Session = Depends(get_db)):
    invoice = db.query(db_mod.Invoice).filter(db_mod.Invoice.id == invoice_id).first()
    if not invoice:
        raise HTTPException(status_code=404)
    return templates.TemplateResponse(request=request, name="admin/accounting/invoice_detail.html", context=_accounting_context(request, invoice=invoice))


@app.post("/admin/accounting/invoices/{invoice_id}/{action}")
@admin_required
async def admin_accounting_invoice_action(request: Request, invoice_id: int, action: str, db: Session = Depends(get_db)):
    flash_message = f"Invoice {action} completed."
    flash_level = "success"
    if action == "issue":
        invoice = invoice_service.issue_invoice(db, invoice_id)
        flash_message = "Invoice issued."
    elif action == "generate-pdf":
        invoice = db.query(db_mod.Invoice).filter(db_mod.Invoice.id == invoice_id).first()
        if not invoice:
            raise HTTPException(status_code=404)
        try:
            invoice_service.generate_invoice_pdf(invoice, BASE_DIR)
            flash_message = "Invoice PDF generated."
        except Exception:
            logger.exception("Invoice PDF generation failed invoice_id=%s", invoice_id)
            flash_level = "error"
            flash_message = f"PDF generation failed: {invoice.pdf_error_message or 'Unknown error'}"
    elif action in {"send", "retry-send"}:
        invoice = db.query(db_mod.Invoice).filter(db_mod.Invoice.id == invoice_id).first()
        if not invoice:
            raise HTTPException(status_code=404)
        if invoice_service.send_invoice_email(invoice, send_email):
            flash_message = "Invoice sent to customer."
        else:
            flash_level = "error"
            flash_message = f"Invoice send failed: {invoice.send_error_message or 'Unknown error'}"
    elif action == "cancel":
        invoice = invoice_service.cancel_invoice(db, invoice_id)
        flash_message = "Invoice cancelled."
    else:
        raise HTTPException(status_code=404)
    log_action(db, _admin_actor(request), f"accounting_invoice_{action}", "invoice", invoice.id, invoice.invoice_number or "")
    db.commit()
    set_flash(request, flash_message, flash_level)
    return RedirectResponse(f"/admin/accounting/invoices/{invoice.id}", status_code=303)


@app.get("/admin/accounting/invoices/{invoice_id}/download")
@admin_required
async def admin_accounting_invoice_download(request: Request, invoice_id: int, db: Session = Depends(get_db)):
    invoice = db.query(db_mod.Invoice).filter(db_mod.Invoice.id == invoice_id).first()
    if not invoice:
        raise HTTPException(status_code=404)
    if invoice.pdf_status != "ready" or not invoice.pdf_path or not os.path.exists(invoice.pdf_path):
        set_flash(request, "PDF is not ready yet. Generate the PDF first.", "error")
        return RedirectResponse(f"/admin/accounting/invoices/{invoice.id}", status_code=303)
    return FileResponse(invoice.pdf_path, media_type="application/pdf", filename=f"invoice-{invoice.id}.pdf")


@app.get("/admin/accounting/taxes", response_class=HTMLResponse)
@admin_required
async def admin_accounting_taxes(request: Request, db: Session = Depends(get_db)):
    _sync_accounting(db)
    overview = tax_service.tax_overview(db)
    return templates.TemplateResponse(request=request, name="admin/accounting/tax_overview.html", context=_accounting_context(request, overview=overview))


@app.get("/admin/accounting/expenses", response_class=HTMLResponse)
@admin_required
async def admin_accounting_expenses(request: Request, db: Session = Depends(get_db)):
    expenses = db.query(db_mod.Expense).order_by(db_mod.Expense.expense_date.desc()).limit(250).all()
    return templates.TemplateResponse(request=request, name="admin/accounting/expenses.html", context=_accounting_context(request, expenses=expenses))


@app.get("/admin/accounting/documents", response_class=HTMLResponse)
@admin_required
async def admin_accounting_documents(
    request: Request,
    db: Session = Depends(get_db),
    year: int | None = None,
    month: int | None = None,
    document_type: str = "",
    status: str = "",
    q: str = "",
    start_date: str = "",
    end_date: str = "",
    expense_only: bool = False,
    month_close_only: bool = False,
):
    _sync_accounting(db)
    documents, summary = document_service.archive_documents(
        db,
        base_dir=BASE_DIR,
        year=year,
        month=month,
        document_type=document_type,
        status=status,
        q=q,
        start_date=_parse_date_filter(start_date),
        end_date=_parse_date_filter(end_date, end_of_day=True),
        expense_only=expense_only,
        month_close_only=month_close_only,
    )
    return templates.TemplateResponse(
        request=request,
        name="admin/accounting/documents.html",
        context=_accounting_context(
            request,
            documents=documents,
            summary=summary,
            year=year or "",
            month=month or "",
            document_type=document_type,
            status=status,
            q=q,
            start_date=start_date,
            end_date=end_date,
            expense_only=expense_only,
            month_close_only=month_close_only,
        ),
    )


@app.get("/admin/accounting/documents/download")
@admin_required
async def admin_accounting_document_download(request: Request, path: str, db: Session = Depends(get_db)):
    requested = Path(path)
    if requested.is_absolute():
        file_path = requested.resolve()
    else:
        file_path = (BASE_DIR / path).resolve()
    allowed_roots = [
        (BASE_DIR / "static").resolve(),
    ]
    if not any(str(file_path).startswith(str(root)) for root in allowed_roots) or not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404)
    return FileResponse(str(file_path), filename=file_path.name)


@app.post("/admin/accounting/expenses")
@admin_required
async def admin_accounting_expense_create(request: Request, receipt: UploadFile | None = File(default=None), db: Session = Depends(get_db)):
    form = await request.form()
    receipt_path = None
    if receipt and receipt.filename:
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "-", receipt.filename).strip("-") or "receipt"
        receipt_dir = BASE_DIR / "static" / "accounting"
        receipt_dir.mkdir(parents=True, exist_ok=True)
        receipt_path_obj = receipt_dir / f"{int(time.time())}-{safe_name}"
        receipt_path_obj.write_bytes(await receipt.read())
        receipt_path = str(receipt_path_obj.relative_to(BASE_DIR)).replace("\\", "/")
    expense = db_mod.Expense(
        supplier=str(form.get("supplier", "")).strip(),
        category=str(form.get("category", "")).strip(),
        description=str(form.get("description", "")).strip(),
        amount=str(form.get("amount", "0")).strip() or "0",
        vat_amount=str(form.get("vat_amount", "0")).strip() or "0",
        expense_date=_parse_date_filter(str(form.get("expense_date", ""))) or datetime.utcnow(),
        receipt_path=receipt_path,
    )
    db.add(expense)
    db.commit()
    set_flash(request, "Expense recorded.")
    return RedirectResponse("/admin/accounting/expenses", status_code=303)


@app.get("/admin/accounting/exports", response_class=HTMLResponse)
@admin_required
async def admin_accounting_exports(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse(request=request, name="admin/accounting/exports.html", context=_accounting_context(request))


@app.get("/admin/accounting/exports/transactions.csv")
@admin_required
async def admin_accounting_export_transactions(request: Request, db: Session = Depends(get_db)):
    _sync_accounting(db)
    csv_text = accounting_exports.transactions_csv(db.query(db_mod.Transaction).order_by(db_mod.Transaction.paid_at.desc().nullslast()).all())
    return Response(csv_text, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=transactions.csv"})


@app.get("/admin/accounting/exports/monthly.zip")
@admin_required
async def admin_accounting_export_monthly(request: Request, db: Session = Depends(get_db), year: int | None = None, month: int | None = None):
    _sync_accounting(db)
    transactions_query = db.query(db_mod.Transaction)
    invoices_query = db.query(db_mod.Invoice)
    filename = "finance-export.zip"
    if year and month:
        start, end = month_close_service.month_bounds(year, month)
        transactions_query = transactions_query.filter(db_mod.Transaction.paid_at >= start, db_mod.Transaction.paid_at < end)
        invoices_query = invoices_query.filter(db_mod.Invoice.created_at >= start, db_mod.Invoice.created_at < end)
        filename = f"finance-export-{int(year):04d}-{int(month):02d}.zip"
    content = accounting_exports.monthly_finance_zip(transactions=transactions_query.all(), invoices=invoices_query.all())
    return Response(content, media_type="application/zip", headers={"Content-Disposition": f"attachment; filename={filename}"})


@app.get("/admin/accounting/settings", response_class=HTMLResponse)
@admin_required
async def admin_accounting_settings(request: Request, db: Session = Depends(get_db)):
    rates = db.query(db_mod.TaxRate).order_by(db_mod.TaxRate.tax_type.asc(), db_mod.TaxRate.name.asc()).all()
    return templates.TemplateResponse(request=request, name="admin/accounting/settings.html", context=_accounting_context(request, rates=rates))


@app.post("/admin/accounting/settings/tax-rate")
@admin_required
async def admin_accounting_tax_rate_create(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    rate = db_mod.TaxRate(name=str(form.get("name", "")).strip(), tax_type=str(form.get("tax_type", "")).strip() or "vat", rate_percent=str(form.get("rate_percent", "0")).strip() or "0")
    db.add(rate)
    db.commit()
    set_flash(request, "Tax rate saved.")
    return RedirectResponse("/admin/accounting/settings", status_code=303)


@app.get("/admin/contact", response_class=HTMLResponse)
@admin_required
async def admin_contact(request: Request, db: Session = Depends(get_db)):
    messages = db.query(db_mod.ContactMessage).order_by(db_mod.ContactMessage.created_at.desc()).limit(200).all()
    unread = db.query(db_mod.ContactMessage).filter(db_mod.ContactMessage.is_read.is_(False)).count()
    return templates.TemplateResponse(
        request=request,
        name="admin/contact.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            active_page="contact",
            messages=messages,
            unread=unread,
        ),
    )


@app.post("/admin/contact/{msg_id}/read")
@admin_required
async def admin_contact_mark_read(request: Request, msg_id: int, db: Session = Depends(get_db)):
    msg = db.query(db_mod.ContactMessage).filter(db_mod.ContactMessage.id == msg_id).first()
    if msg:
        msg.is_read = True
        db.commit()
        set_flash(request, "Mesaj okundu olarak işaretlendi.")
    return RedirectResponse("/admin/contact", status_code=303)


def _flatten_translation_rows(limit=300):
    rows = []
    en_root = TRANSLATIONS.get("en", {})
    tr_root = TRANSLATIONS.get("tr", {})
    keys = sorted({f"{group}.{key}" for root in (en_root, tr_root) for group, values in root.items() if isinstance(values, Mapping) for key in values.keys()})
    for full_key in keys[:limit]:
        group, key = full_key.split(".", 1)
        rows.append({"key": full_key, "tr": tr_root.get(group, {}).get(key, ""), "en": en_root.get(group, {}).get(key, "")})
    return rows


@app.get("/admin/translations", response_class=HTMLResponse)
@admin_required
async def admin_translations(request: Request, db: Session = Depends(get_db)):
    rows = _flatten_translation_rows()
    keys = [row["key"] for row in rows]
    tr_values = {row["key"]: row["tr"] for row in rows}
    en_values = {row["key"]: row["en"] for row in rows}
    return templates.TemplateResponse(
        request=request,
        name="admin/translations.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            active_page="translations",
            translation_rows=rows,
            keys=keys,
            tr_values=tr_values,
            en_values=en_values,
        ),
    )


@app.post("/admin/translations")
@admin_required
async def admin_translations_save(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    changed = 0
    for field, value in form.items():
        if "__" not in field:
            continue
        lang, full_key = field.split("__", 1)
        if lang not in {"tr", "en"} or "." not in full_key:
            continue
        group, key = full_key.split(".", 1)
        if group in TRANSLATIONS.get(lang, {}) and key in TRANSLATIONS[lang][group] and TRANSLATIONS[lang][group][key] != value:
            TRANSLATIONS[lang][group][key] = str(value)
            changed += 1
    log_action(db, _admin_actor(request), "translations_update", "translations", None, f"{changed} values updated in memory")
    db.commit()
    return RedirectResponse(url="/admin/translations?notice=updated", status_code=303)


def _media_rows():
    rows = []
    root = BASE_DIR / "static" / "articles"
    if not root.exists():
        return rows
    for path in sorted(root.iterdir(), key=lambda item: item.name.lower()):
        if path.is_file():
            size_kb = max(1, path.stat().st_size // 1024)
            rows.append({"name": path.name, "extension": path.suffix.lower().lstrip(".") or "file", "size_kb": size_kb})
    return rows


@app.get("/admin/media", response_class=HTMLResponse)
@admin_required
async def admin_media(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse(request=request, name="admin/media.html", context=_auth_template_context(request, dashboard_user=request.state.admin_user, active_page="media", files=_media_rows()))


@app.post("/admin/media/upload")
@admin_required
async def admin_media_upload(request: Request, file: UploadFile = File(...), db: Session = Depends(get_db)):
    raw_name = Path(file.filename or "upload.bin").name
    safe_name = re.sub(r"[^a-zA-Z0-9_.-]+", "-", raw_name).strip("-") or "upload.bin"
    target_dir = BASE_DIR / "static" / "articles"
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / safe_name
    content = await file.read()
    target.write_bytes(content)
    log_action(db, _admin_actor(request), "media_upload", "media", None, safe_name)
    db.commit()
    return RedirectResponse(url="/admin/media?notice=uploaded", status_code=303)


@app.get("/admin/logs", response_class=HTMLResponse)
@admin_required
async def admin_logs(request: Request, db: Session = Depends(get_db)):
    logs = db.query(db_mod.ActivityLog).order_by(db_mod.ActivityLog.created_at.desc()).limit(200).all()
    return templates.TemplateResponse(request=request, name="admin/logs.html", context=_auth_template_context(request, dashboard_user=request.state.admin_user, active_page="logs", logs=logs))


ADMIN_SETTING_LABELS = {
    "site_instagram_url": "Instagram URL",
    "site_maintenance_mode": "Maintenance mode",
    "site_contact_email": "Contact email",
    "astrologer_name": "Astrologer name",
    "site_report_delivery_days": "Report delivery days",
    "site_usd_try_rate": "USD/TRY rate",
}


@app.get("/admin/settings", response_class=HTMLResponse)
@admin_required
async def admin_settings(request: Request, db: Session = Depends(get_db)):
    existing = {row.key: row.value for row in db.query(db_mod.SiteSetting).all()}
    defaults = {"site_instagram_url": "", "site_maintenance_mode": "false", "site_contact_email": "info@focusastrology.com", "astrologer_name": "", "site_report_delivery_days": "7", "site_usd_try_rate": ""}
    settings = {key: existing.get(key, defaults[key]) for key in ADMIN_SETTING_LABELS}
    return templates.TemplateResponse(request=request, name="admin/settings.html", context=_auth_template_context(request, dashboard_user=request.state.admin_user, active_page="settings", settings=settings, settings_labels=ADMIN_SETTING_LABELS))


@app.post("/admin/settings")
@admin_required
async def admin_settings_save(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    for key in ADMIN_SETTING_LABELS:
        row = db.query(db_mod.SiteSetting).filter(db_mod.SiteSetting.key == key).first()
        if not row:
            row = db_mod.SiteSetting(key=key)
            db.add(row)
        if key == "site_maintenance_mode":
            row.value = "true" if key in form else "false"
        else:
            row.value = str(form.get(key, ""))
        row.updated_at = datetime.utcnow()
    log_action(db, _admin_actor(request), "settings_update", "settings", None, "site settings updated")
    db.commit()
    return RedirectResponse(url="/admin/settings?notice=updated", status_code=303)


@app.get("/admin/analytics", response_class=HTMLResponse)
@admin_required
async def admin_analytics(request: Request, db: Session = Depends(get_db)):
    now = datetime.utcnow()
    report_counts = []
    signup_counts = []
    for offset in range(6, -1, -1):
        day_start = (now - timedelta(days=offset)).replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
        report_counts.append({
            "date": day_start.strftime("%Y-%m-%d"),
            "count": db.query(db_mod.GeneratedReport).filter(db_mod.GeneratedReport.created_at >= day_start, db_mod.GeneratedReport.created_at < day_end).count(),
        })
        signup_counts.append({
            "date": day_start.strftime("%Y-%m-%d"),
            "count": db.query(db_mod.AppUser).filter(db_mod.AppUser.created_at >= day_start, db_mod.AppUser.created_at < day_end).count(),
        })
    total_users = db.query(db_mod.AppUser).count()
    total_reports = db.query(db_mod.GeneratedReport).count()
    paid_users = db.query(db_mod.AppUser).filter(db_mod.AppUser.plan_code.in_(["basic", "premium", "elite"])).count()
    free_users = db.query(db_mod.AppUser).filter(db_mod.AppUser.plan_code == "free").count()
    reports_by_type = {rtype: db.query(db_mod.GeneratedReport).filter(db_mod.GeneratedReport.report_type == rtype).count() for rtype in REPORT_TYPES}
    plan_distribution = {plan: db.query(db_mod.AppUser).filter(db_mod.AppUser.plan_code == plan).count() for plan in PLAN_FEATURES}
    average_reports_per_user = round(total_reports / total_users, 2) if total_users else 0
    return templates.TemplateResponse(
        request=request,
        name="admin/analytics.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            totals={
                "total_users": total_users,
                "total_reports": total_reports,
                "paid_users": paid_users,
                "free_users": free_users,
                "average_reports_per_user": average_reports_per_user,
            },
            report_counts=report_counts,
            signup_counts=signup_counts,
            reports_by_type=reports_by_type,
            plan_distribution=plan_distribution,
        ),
    )


@app.get("/admin/revenue", response_class=HTMLResponse)
@admin_required
async def admin_revenue(request: Request, db: Session = Depends(get_db)):
    now = datetime.utcnow()
    week_ago = now - timedelta(days=7)
    month_ago = now - timedelta(days=30)

    total_users = db.query(db_mod.AppUser).count()
    free_users = db.query(db_mod.AppUser).filter(db_mod.AppUser.plan_code == "free").count()
    paid_users = db.query(db_mod.AppUser).filter(db_mod.AppUser.plan_code != "free").count()
    total_reports = db.query(db_mod.GeneratedReport).count()
    reports_last_30_days = db.query(db_mod.GeneratedReport).filter(db_mod.GeneratedReport.created_at >= month_ago).count()
    paid_reports_last_30_days = (
        db.query(db_mod.GeneratedReport)
        .join(db_mod.AppUser, db_mod.GeneratedReport.user_id == db_mod.AppUser.id)
        .filter(db_mod.GeneratedReport.created_at >= month_ago, db_mod.AppUser.plan_code != "free")
        .count()
    )
    free_reports_last_30_days = max(reports_last_30_days - paid_reports_last_30_days, 0)

    conversion_rate = round((paid_users / total_users) * 100, 2) if total_users else 0.0
    avg_reports_per_user = round(total_reports / total_users, 2) if total_users else 0.0
    avg_reports_per_paid_user = round(paid_reports_last_30_days / paid_users, 2) if paid_users else 0.0
    avg_reports_per_free_user = round(free_reports_last_30_days / free_users, 2) if free_users else 0.0

    plan_distribution = {
        plan: db.query(db_mod.AppUser).filter(db_mod.AppUser.plan_code == plan).count()
        for plan in PLAN_FEATURES
    }
    reports_by_type = {
        report_type: db.query(db_mod.GeneratedReport).filter(db_mod.GeneratedReport.report_type == report_type).count()
        for report_type in REPORT_TYPES
    }
    recent_paid_users = (
        db.query(db_mod.AppUser)
        .filter(db_mod.AppUser.plan_code != "free")
        .order_by(db_mod.AppUser.plan_started_at.desc(), db_mod.AppUser.created_at.desc())
        .limit(10)
        .all()
    )
    billing_email_stats = {
        "payment_success_30d": db.query(db_mod.EmailLog).filter(
            db_mod.EmailLog.email_type == "payment_success",
            db_mod.EmailLog.created_at >= month_ago,
        ).count(),
        "payment_failed_30d": db.query(db_mod.EmailLog).filter(
            db_mod.EmailLog.email_type == "payment_failed",
            db_mod.EmailLog.created_at >= month_ago,
        ).count(),
        "recovery_30d": db.query(db_mod.EmailLog).filter(
            db_mod.EmailLog.email_type == "payment_recovery",
            db_mod.EmailLog.created_at >= month_ago,
        ).count(),
        "upgrades_30d": db.query(db_mod.EmailLog).filter(
            db_mod.EmailLog.email_type == "plan_upgraded",
            db_mod.EmailLog.created_at >= month_ago,
        ).count(),
    }
    recent_revenue_signals = (
        db.query(db_mod.EmailLog)
        .filter(
            db_mod.EmailLog.email_type.in_(["plan_upgraded", "payment_success", "payment_failed", "payment_recovery", "cancellation"]),
            db_mod.EmailLog.created_at >= week_ago,
        )
        .order_by(db_mod.EmailLog.created_at.desc())
        .limit(12)
        .all()
    )

    return templates.TemplateResponse(
        request=request,
        name="admin/revenue.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            metrics={
                "total_users": total_users,
                "free_users": free_users,
                "paid_users": paid_users,
                "conversion_rate": conversion_rate,
                "total_reports": total_reports,
                "reports_last_30_days": reports_last_30_days,
                "paid_reports_last_30_days": paid_reports_last_30_days,
                "avg_reports_per_user": avg_reports_per_user,
                "avg_reports_per_paid_user": avg_reports_per_paid_user,
                "avg_reports_per_free_user": avg_reports_per_free_user,
            },
            plan_distribution=plan_distribution,
            reports_by_type=reports_by_type,
            billing_email_stats=billing_email_stats,
            recent_paid_users=[_user_admin_view(user, db.query(db_mod.GeneratedReport).filter(db_mod.GeneratedReport.user_id == user.id).count()) for user in recent_paid_users],
            recent_revenue_signals=[_email_log_view(log) for log in recent_revenue_signals],
        ),
    )


@app.get("/admin/summary", response_class=HTMLResponse)
@admin_required
async def admin_summary(request: Request, db: Session = Depends(get_db)):
    try:
        executive = compute_executive_kpis(db)
        watchlist_items = build_watchlist_items(
            executive["kpis"],
            executive["trends"],
            executive["revenue"],
            executive["segment_context"],
        )
        weekly_summary = build_weekly_executive_summary(executive["kpis"], executive["trends"], watchlist_items)
        logger.info("Admin executive summary viewed")
        return templates.TemplateResponse(
            request=request,
            name="admin/summary.html",
            context=_auth_template_context(
                request,
                dashboard_user=request.state.admin_user,
                kpis=executive["kpis"],
                scorecards=executive["scorecards"],
                trends=executive["trends"],
                watchlist_items=watchlist_items,
                weekly_summary=weekly_summary,
            ),
        )
    except Exception:
        logger.exception("Admin summary failed")
        _public_error("Admin summary sayfasi yuklenemedi.", 500)


@app.get("/admin/birthplace-analytics", response_class=HTMLResponse)
@admin_required
async def admin_birthplace_analytics(request: Request, db: Session = Depends(get_db), period: str = "all"):
    try:
        summary = get_birthplace_observability_summary(db, time_window=period)
        logger.info("Admin birthplace analytics viewed period=%s", summary["time_window"])
        return templates.TemplateResponse(
            request=request,
            name="admin/birthplace_analytics.html",
            context=_auth_template_context(
                request,
                dashboard_user=request.state.admin_user,
                summary=summary,
                selected_period=summary["time_window"],
            ),
        )
    except Exception:
        logger.exception("Admin birthplace analytics failed")
        _public_error("Birthplace analytics sayfasi yuklenemedi.", 500)


@app.get("/api/admin/summary")
@admin_api_required
async def api_admin_summary(request: Request, db: Session = Depends(get_db)):
    try:
        return json_ok(build_admin_summary_api_payload(db), endpoint=request)
    except Exception:
        logger.exception("Admin summary API failed")
        return json_admin_error("summary_build_failed", 500, endpoint=request)


@app.get("/api/v1/birthplace-suggestions")
async def api_birthplace_suggestions(q: str = "", db: Session = Depends(get_db)):
    query = str(q or "").strip()
    if len(query) < 2:
        return JSONResponse(content=[])
    try:
        suggestions = search_birth_places(query, limit=5)
        try:
            _log_birthplace_event(
                db,
                "suggestion_results_returned",
                provider=suggestions[0].get("provider") if suggestions else "nominatim",
                outcome="success",
                location_source="suggestion_lookup",
                suggestion_count=len(suggestions),
            )
        except Exception:
            logger.exception("Failed to log birthplace suggestion results")
        return JSONResponse(content=suggestions)
    except BirthPlaceResolutionError as exc:
        logger.warning("Birthplace suggestion lookup failed: %s", exc)
        try:
            _log_birthplace_event(
                db,
                "resolved_birthplace_failure",
                provider="nominatim",
                outcome=exc.code,
                location_source="suggestion_lookup",
            )
        except Exception:
            logger.exception("Failed to log birthplace suggestion failure")
        return JSONResponse(content=[])
    except Exception:
        logger.exception("Birthplace suggestion endpoint failed")
        return JSONResponse(content=[], status_code=200)


@app.get("/api/v1/birthplace-observe")
async def api_birthplace_observe(
    event: str = "",
    provider: str = "",
    location_source: str = "",
    outcome: str = "",
    confidence: str = "",
    db: Session = Depends(get_db),
):
    event_name = str(event or "").strip()
    if event_name not in ALLOWED_BIRTHPLACE_EVENTS:
        return JSONResponse(content={"ok": False, "error": "invalid_event"}, status_code=400)
    try:
        _log_birthplace_event(
            db,
            event_name,
            provider=provider or None,
            outcome=outcome or "client_observed",
            location_source=location_source or None,
            confidence=float(confidence) if confidence not in ("", None) else None,
        )
    except Exception:
        logger.exception("Failed to record birthplace observe event")
        return JSONResponse(content={"ok": False}, status_code=500)
    return JSONResponse(content={"ok": True})


@app.post("/api/v1/interpretation-feedback")
async def api_interpretation_feedback(request: Request, db: Session = Depends(get_db)):
    current_user = get_request_user(request, db)
    if not current_user:
        return JSONResponse(content={"ok": False, "error": "authentication_required"}, status_code=401)
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse(content={"ok": False, "error": "invalid_json"}, status_code=400)

    report_id = payload.get("report_id") or payload.get("reading_id")
    try:
        report_id = int(report_id)
    except (TypeError, ValueError):
        return JSONResponse(content={"ok": False, "error": "invalid_report_id"}, status_code=400)

    report = db.query(db_mod.GeneratedReport).filter(
        db_mod.GeneratedReport.id == report_id,
        db_mod.GeneratedReport.user_id == current_user.id,
    ).first()
    if not report:
        return JSONResponse(content={"ok": False, "error": "report_not_found"}, status_code=404)

    try:
        saved = save_interpretation_feedback(db, payload, report=report, user=current_user)
        return JSONResponse(content={"ok": True, "feedback": saved})
    except ValueError as exc:
        logger.warning("Interpretation feedback validation failed user_id=%s error=%s", current_user.id, exc)
        return JSONResponse(content={"ok": False, "error": str(exc)}, status_code=400)
    except Exception:
        logger.exception("Interpretation feedback save failed user_id=%s", current_user.id)
        return JSONResponse(content={"ok": False, "error": "feedback_save_failed"}, status_code=500)


@app.post("/api/v1/recommendation-feedback")
async def api_recommendation_feedback(request: Request, db: Session = Depends(get_db)):
    current_user = get_request_user(request, db)
    if not current_user:
        return JSONResponse(content={"ok": False, "error": "authentication_required"}, status_code=401)
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse(content={"ok": False, "error": "invalid_json"}, status_code=400)

    report_id = payload.get("report_id")
    try:
        report_id = int(report_id)
    except (TypeError, ValueError):
        return JSONResponse(content={"ok": False, "error": "invalid_report_id"}, status_code=400)

    report = db.query(db_mod.GeneratedReport).filter(
        db_mod.GeneratedReport.id == report_id,
        db_mod.GeneratedReport.user_id == current_user.id,
    ).first()
    if not report:
        return JSONResponse(content={"ok": False, "error": "report_not_found"}, status_code=404)

    try:
        saved = _save_recommendation_feedback(db, payload, report=report, user=current_user)
        return JSONResponse(content={"ok": True, "feedback": saved})
    except ValueError as exc:
        logger.warning("Recommendation feedback validation failed user_id=%s error=%s", current_user.id, exc)
        return JSONResponse(content={"ok": False, "error": str(exc)}, status_code=400)
    except Exception:
        logger.exception("Recommendation feedback save failed user_id=%s", current_user.id)
        return JSONResponse(content={"ok": False, "error": "feedback_save_failed"}, status_code=500)


@app.get("/api/v1/recommendation-followups")
async def api_recommendation_followups(request: Request, db: Session = Depends(get_db)):
    current_user = get_request_user(request, db)
    if not current_user:
        return JSONResponse(content={"ok": False, "error": "authentication_required"}, status_code=401)
    try:
        return JSONResponse(content={"ok": True, "followups": get_pending_followups(db, current_user.id)})
    except Exception:
        logger.exception("Recommendation followup retrieval failed user_id=%s", current_user.id)
        return JSONResponse(content={"ok": False, "error": "followup_fetch_failed"}, status_code=500)


@app.post("/api/v1/recommendation-followups/{followup_id}/complete")
async def api_complete_recommendation_followup(
    followup_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    current_user = get_request_user(request, db)
    if not current_user:
        return JSONResponse(content={"ok": False, "error": "authentication_required"}, status_code=401)
    followup = db.query(db_mod.RecommendationFollowup).filter(
        db_mod.RecommendationFollowup.id == followup_id,
        db_mod.RecommendationFollowup.user_id == current_user.id,
    ).first()
    if not followup:
        return JSONResponse(content={"ok": False, "error": "followup_not_found"}, status_code=404)
    if followup.status != "pending":
        return JSONResponse(content={"ok": False, "error": "followup_not_pending"}, status_code=400)
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse(content={"ok": False, "error": "invalid_json"}, status_code=400)

    report = db.query(db_mod.GeneratedReport).filter(
        db_mod.GeneratedReport.id == followup.report_id,
        db_mod.GeneratedReport.user_id == current_user.id,
    ).first()
    if not report:
        return JSONResponse(content={"ok": False, "error": "report_not_found"}, status_code=404)

    feedback_label = str(payload.get("feedback_label") or "").strip().lower()
    if feedback_label not in VALID_RECOMMENDATION_FOLLOWUP_LABELS:
        return JSONResponse(content={"ok": False, "error": "invalid_feedback_label"}, status_code=400)

    try:
        completed = _complete_recommendation_followup(db, payload, followup=followup, report=report, user=current_user)
        return JSONResponse(content={"ok": True, "followup": completed})
    except ValueError as exc:
        logger.warning("Recommendation followup completion validation failed user_id=%s error=%s", current_user.id, exc)
        return JSONResponse(content={"ok": False, "error": str(exc)}, status_code=400)
    except Exception:
        logger.exception("Recommendation followup completion failed user_id=%s followup_id=%s", current_user.id, followup_id)
        return JSONResponse(content={"ok": False, "error": "followup_complete_failed"}, status_code=500)


@app.get("/api/admin/revenue")
@admin_api_required
async def api_admin_revenue(request: Request, db: Session = Depends(get_db)):
    try:
        return json_ok(build_admin_revenue_api_payload(db), endpoint=request)
    except Exception:
        logger.exception("Admin revenue API failed")
        return json_admin_error("revenue_build_failed", 500, endpoint=request)


@app.get("/api/admin/insights")
@admin_api_required
async def api_admin_insights(request: Request, db: Session = Depends(get_db)):
    try:
        return json_ok(build_admin_insights_api_payload(db), endpoint=request)
    except Exception:
        logger.exception("Admin insights API failed")
        return json_admin_error("insights_build_failed", 500, endpoint=request)


@app.get("/api/admin/segments")
@admin_api_required
async def api_admin_segments(
    request: Request,
    db: Session = Depends(get_db),
    segment: str = "",
    group: str = "",
    priority: str = "",
    channel: str = "",
    message_type: str = "",
    limit: str = "",
):
    try:
        parsed_limit = None
        if str(limit or "").strip():
            if not str(limit).isdigit():
                logger.warning("Admin segments API invalid limit=%s", limit)
                return json_admin_error("invalid_limit", 400, endpoint=request)
            parsed_limit = int(limit)
            if parsed_limit <= 0 or parsed_limit > 500:
                logger.warning("Admin segments API out of range limit=%s", limit)
                return json_admin_error("invalid_limit", 400, endpoint=request)

        payload, error_message = build_admin_segments_api_payload(
            db,
            segment=segment,
            group=group,
            priority=priority,
            channel=channel,
            message_type=message_type,
            limit=parsed_limit,
        )
        if error_message:
            logger.warning(
                "Admin segments API invalid filters segment=%s group=%s priority=%s channel=%s message_type=%s",
                segment, group, priority, channel, message_type,
            )
            return json_admin_error("invalid_filters", 400, endpoint=request)
        return json_ok(payload, endpoint=request)
    except Exception:
        logger.exception("Admin segments API failed")
        return json_admin_error("segments_build_failed", 500, endpoint=request)


@app.get("/api/admin/segments/export-metadata")
@admin_api_required
async def api_admin_segments_export_metadata(request: Request, db: Session = Depends(get_db)):
    try:
        return json_ok(build_admin_segments_export_metadata_payload(), endpoint=request)
    except Exception:
        logger.exception("Admin segment export metadata API failed")
        return json_admin_error("export_metadata_build_failed", 500, endpoint=request)


@app.get("/api/admin/health")
@admin_api_required
async def api_admin_health(request: Request, db: Session = Depends(get_db)):
    try:
        executive = compute_executive_kpis(db)
        email_failure_count = executive["kpis"]["email_failed_count"]
        payment_failed_signal_count = executive["kpis"]["payment_failed_signal_count"]
        inactive_paid_users_count = executive["kpis"]["inactive_paid_users"]
        signals = {
            "email_failures": email_failure_count,
            "email_failure_count": email_failure_count,
            "payment_failed_signals": payment_failed_signal_count,
            "payment_failed_signal_count": payment_failed_signal_count,
            "inactive_paid_users": inactive_paid_users_count,
            "inactive_paid_users_count": inactive_paid_users_count,
            "conversion_risk": executive["kpis"]["conversion_rate"] < 0.03,
            "ops_warning": executive["kpis"]["email_failure_rate"] > 0.08 or executive["kpis"]["email_skipped_count"] > executive["kpis"]["email_failed_count"],
        }
        return json_ok({"signals": signals}, endpoint=request)
    except Exception:
        logger.exception("Admin health API failed")
        return json_admin_error("health_build_failed", 500, endpoint=request)


@app.get("/api/admin/docs")
@admin_api_required
async def api_admin_docs(request: Request, db: Session = Depends(get_db)):
    try:
        logger.info("Admin API docs viewed admin_id=%s path=%s", request.state.admin_user.get("id"), request.url.path)
        return json_ok(build_admin_api_docs_payload(), endpoint=request)
    except Exception:
        logger.exception("Admin API docs failed")
        return json_admin_error("docs_build_failed", 500, endpoint=request)


@app.get("/admin/insights", response_class=HTMLResponse)
@admin_required
async def admin_insights(request: Request, db: Session = Depends(get_db)):
    try:
        insights_context = generate_admin_insights(db)
        return templates.TemplateResponse(
            request=request,
            name="admin/insights.html",
            context=_auth_template_context(
                request,
                dashboard_user=request.state.admin_user,
                **insights_context,
            ),
        )
    except Exception:
        logger.exception("Admin insights failed")
        _public_error("Admin insights sayfasi yuklenemedi.", 500)


@app.get("/admin/ai-rules", response_class=HTMLResponse)
@admin_required
async def admin_ai_rules(request: Request, db: Session = Depends(get_db)):
    ai_behavior_rules.ensure_default_rule_set(db, admin_user=get_request_user(request, db))
    db.commit()
    rule_set = ai_behavior_rules.get_active_rule_set(db)
    rules = db.query(db_mod.AiBehaviorRule).filter(db_mod.AiBehaviorRule.rule_set_id == rule_set.id).order_by(db_mod.AiBehaviorRule.sort_order.asc(), db_mod.AiBehaviorRule.id.asc()).all()
    return templates.TemplateResponse(
        request=request,
        name="admin/ai_rules.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            active_page="ai_rules",
            rule_set=rule_set,
            sections=ai_behavior_rules.group_rules_by_section(rules),
            immutable_rules=ai_behavior_rules.IMMUTABLE_GROUNDING_RULES,
            prompt_block=ai_behavior_rules.build_prompt_rule_blocks(active_rules=[rule for rule in rules if rule.is_enabled])["prompt_block"],
        ),
    )


@app.post("/admin/ai-rules", response_class=HTMLResponse)
@admin_required
async def admin_ai_rules_update(request: Request, db: Session = Depends(get_db), csrf_token: str = Form(default="")):
    await validate_csrf_token(request, csrf_token)
    form = await request.form()
    ai_behavior_rules.update_rules_from_form(db, form)
    db.commit()
    rule_set = ai_behavior_rules.get_active_rule_set(db)
    rules = db.query(db_mod.AiBehaviorRule).filter(db_mod.AiBehaviorRule.rule_set_id == rule_set.id).order_by(db_mod.AiBehaviorRule.sort_order.asc(), db_mod.AiBehaviorRule.id.asc()).all()
    return templates.TemplateResponse(
        request=request,
        name="admin/ai_rules.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            active_page="ai_rules",
            rule_set=rule_set,
            sections=ai_behavior_rules.group_rules_by_section(rules),
            immutable_rules=ai_behavior_rules.IMMUTABLE_GROUNDING_RULES,
            prompt_block=ai_behavior_rules.build_prompt_rule_blocks(active_rules=[rule for rule in rules if rule.is_enabled])["prompt_block"],
            notice="AI behavior rules updated.",
        ),
    )


def _workspace_report_options():
    return [
        {"value": key, "label": label}
        for key, label in astro_workspace.PUBLIC_WORKSPACE_REPORT_TYPES.items()
    ]


def _workspace_profile_payload(prefix, form):
    key_prefix = f"{prefix}_" if prefix else ""
    return {
        "full_name": form.get(f"{key_prefix}full_name"),
        "gender": form.get(f"{key_prefix}gender"),
        "birth_date": form.get(f"{key_prefix}birth_date"),
        "birth_time": form.get(f"{key_prefix}birth_time"),
        "birth_place_label": form.get(f"{key_prefix}birth_place_label"),
        "birth_country": form.get(f"{key_prefix}birth_country"),
        "birth_city": form.get(f"{key_prefix}birth_city") or form.get(f"{key_prefix}birth_place_label"),
        "resolved_birth_place": form.get(f"{key_prefix}resolved_birth_place") or form.get(f"{key_prefix}birth_place_label"),
        "resolved_latitude": form.get(f"{key_prefix}resolved_latitude"),
        "resolved_longitude": form.get(f"{key_prefix}resolved_longitude"),
        "resolved_timezone": form.get(f"{key_prefix}resolved_timezone"),
        "resolved_geocode_provider": form.get(f"{key_prefix}resolved_geocode_provider"),
        "resolved_geocode_confidence": form.get(f"{key_prefix}resolved_geocode_confidence"),
        "notes": form.get(f"{key_prefix}notes"),
        "is_favorite": form.get(f"{key_prefix}is_favorite") in {"1", "true", "on", "yes"},
    }


def _workspace_profile_from_saved(profile):
    return {
        "full_name": profile.full_name,
        "gender": profile.gender,
        "birth_date": profile.birth_date,
        "birth_time": profile.birth_time,
        "birth_place_label": profile.birth_place_label,
        "birth_country": profile.birth_country,
        "birth_city": profile.birth_city or profile.birth_place_label,
        "notes": profile.notes,
        "is_favorite": bool(profile.is_favorite),
    }


def _workspace_form_flag(form, key):
    return str(form.get(key) or "").strip().lower() in {"1", "true", "on", "yes"}


def _workspace_birth_context_from_profile_payload(payload, existing_profile=None):
    date_value = f"{payload['birth_date']}T{payload['birth_time']}"
    if existing_profile and existing_profile.birth_lat is not None and existing_profile.birth_lng is not None and existing_profile.birth_timezone:
        return _build_birth_context_from_saved_fields(
            date_value,
            raw_birth_place_input=existing_profile.birth_place_label,
            normalized_birth_place=existing_profile.birth_city or existing_profile.birth_place_label,
            latitude=existing_profile.birth_lat,
            longitude=existing_profile.birth_lng,
            timezone=existing_profile.birth_timezone,
            geocode_provider="internal_profile",
            geocode_confidence=None,
            fallback_place_text=existing_profile.birth_place_label,
        )
    has_resolved_payload = all(
        value not in (None, "")
        for value in (
            payload.get("resolved_birth_place"),
            payload.get("resolved_latitude"),
            payload.get("resolved_longitude"),
            payload.get("resolved_timezone"),
        )
    )
    if not has_resolved_payload:
        raise ValueError("Select the birth location from the dropdown suggestions to resolve coordinates and timezone.")
    return _build_birth_context_from_saved_fields(
        date_value,
        raw_birth_place_input=payload.get("birth_city") or payload.get("birth_place_label"),
        normalized_birth_place=payload.get("resolved_birth_place") or payload.get("birth_place_label"),
        latitude=float(payload["resolved_latitude"]),
        longitude=float(payload["resolved_longitude"]),
        timezone=str(payload["resolved_timezone"]).strip(),
        geocode_provider=(payload.get("resolved_geocode_provider") or "suggestion_selection").strip(),
        geocode_confidence=float(payload["resolved_geocode_confidence"]) if payload.get("resolved_geocode_confidence") not in (None, "") else None,
        fallback_place_text=payload.get("birth_place_label") or payload.get("birth_city"),
    )


def _prepare_admin_workspace_generation(db, form, admin_user, language="tr"):
    report_type = astro_workspace.normalize_workspace_report_type(form.get("report_type"))
    save_profile_requested = _workspace_form_flag(form, "save_profile")
    debug_signals_requested = _workspace_form_flag(form, "debug_signals")
    primary_existing = None
    secondary_existing = None
    primary_profile_id = str(form.get("profile_id") or "").strip()
    secondary_profile_id = str(form.get("secondary_profile_id") or "").strip()
    if primary_profile_id:
        primary_existing = db.query(db_mod.InternalProfile).filter(db_mod.InternalProfile.id == int(primary_profile_id)).first()
        if not primary_existing:
            _public_error("Internal profile not found.", 404)
        primary_payload = _workspace_profile_from_saved(primary_existing)
    else:
        primary_payload = astro_workspace.normalize_profile_input(_workspace_profile_payload("", form))

    if report_type == "parent_child":
        if secondary_profile_id:
            secondary_existing = db.query(db_mod.InternalProfile).filter(db_mod.InternalProfile.id == int(secondary_profile_id)).first()
            if not secondary_existing:
                _public_error("Secondary internal profile not found.", 404)
            secondary_payload = _workspace_profile_from_saved(secondary_existing)
        else:
            secondary_raw_payload = _workspace_profile_payload("secondary", form)
            has_secondary_input = any(
                str(secondary_raw_payload.get(field) or "").strip()
                for field in ("full_name", "birth_date", "birth_time", "birth_city", "birth_place_label")
            )
            if not has_secondary_input:
                raise ValueError("Parent-child report requires a secondary child profile with resolved birth details.")
            secondary_payload = astro_workspace.normalize_profile_input(_workspace_profile_payload("secondary", form))
    else:
        secondary_payload = None

    primary_birth_context = _workspace_birth_context_from_profile_payload(primary_payload, primary_existing)
    primary_bundle = _calculate_chart_bundle_from_birth_context(primary_birth_context)
    primary_bundle["name"] = primary_payload.get("full_name") or "Primary Profile"
    primary_profile_record = primary_existing
    secondary_profile_record = secondary_existing

    if save_profile_requested and not primary_profile_record:
        primary_profile_record = astro_workspace.create_or_update_internal_profile(
            db,
            primary_payload,
            admin_user=admin_user,
            location_payload=primary_birth_context,
        )

    secondary_bundle = None
    secondary_birth_context = None
    interpretation_context = primary_bundle.get("interpretation_context") or {}
    if report_type == "parent_child":
        secondary_birth_context = _workspace_birth_context_from_profile_payload(secondary_payload, secondary_existing)
        secondary_bundle = _calculate_chart_bundle_from_birth_context(secondary_birth_context)
        secondary_bundle["name"] = secondary_payload.get("full_name") or "Secondary Profile"
        interpretation_context = build_parent_child_interpretation(primary_bundle, secondary_bundle)
        interpretation_context = _localize_result_layer_text(interpretation_context, language)
    interpretation_context = dict(interpretation_context or {})
    if debug_signals_requested:
        interpretation_context["debug_signals"] = True

    payload = astro_workspace.build_ai_payload(
        report_type=report_type,
        primary_profile=primary_payload,
        primary_bundle=primary_bundle,
        secondary_profile=secondary_payload,
        secondary_bundle=secondary_bundle,
        interpretation_context=interpretation_context,
        language=language,
    )
    return {
        "report_type": report_type,
        "primary_payload": primary_payload,
        "secondary_payload": secondary_payload,
        "primary_profile": primary_profile_record,
        "secondary_profile": secondary_profile_record,
        "primary_birth_context": primary_birth_context,
        "secondary_birth_context": secondary_birth_context,
        "payload": _serialize_temporal_values(payload),
    }


def _internal_pdf_context(request, payload, interpretation_text):
    pdf_payload = dict(payload or {})
    if request is not None:
        pdf_payload["language"] = pdf_payload.get("language") or _result_language(request)
    pdf_payload["ai_interpretation"] = interpretation_text
    context = _filter_report_context(_prepare_report_context(pdf_payload))
    context["request"] = request
    context["show_ai_interpretation"] = True
    context["show_pdf_download"] = False
    context["report_access"] = {
        "is_preview": False,
        "show_unlock_cta": False,
        "can_view_full_report": True,
        "can_download_pdf": True,
        "show_login_hint": False,
        "unlock_success": False,
        "access_label": "Internal" if context.get("language") == "en" else "İç kullanım",
    }
    return context


def _workspace_chat_form_from_profiles(profile, secondary_profile=None, report_type="career"):
    form = {
        "report_type": "parent_child" if secondary_profile else astro_workspace.normalize_workspace_report_type(report_type),
        "profile_id": str(profile.id),
    }
    if secondary_profile:
        form["secondary_profile_id"] = str(secondary_profile.id)
    return form


def _workspace_chat_context_payload(db, session, admin_user):
    profile = getattr(session, "profile", None)
    if not profile:
        raise ValueError("Select an internal profile before asking a chart question.")
    prepared = _prepare_admin_workspace_generation(
        db,
        _workspace_chat_form_from_profiles(
            profile,
            getattr(session, "secondary_profile", None),
            getattr(session, "report_type", None) or "career",
        ),
        admin_user,
    )
    payload = dict(prepared["payload"])
    payload["chat_session_id"] = session.id
    payload["chat_mode"] = astro_chat.normalize_chat_mode(getattr(session, "mode", None))
    return payload


def _chat_session_or_404(db, session_id):
    session = db.query(db_mod.InternalChatSession).filter(db_mod.InternalChatSession.id == int(session_id)).first()
    if not session:
        _public_error("Internal chat session not found.", 404)
    return session


def _create_chat_session_from_form(db, form, admin_user):
    profile_id = str(form.get("profile_id") or "").strip()
    if not profile_id:
        _public_error("Select an internal profile before asking a chart question.", 400)
    profile = db.query(db_mod.InternalProfile).filter(db_mod.InternalProfile.id == int(profile_id)).first()
    if not profile:
        _public_error("Internal profile not found.", 404)
    secondary_profile = None
    secondary_profile_id = str(form.get("secondary_profile_id") or "").strip()
    if secondary_profile_id:
        secondary_profile = db.query(db_mod.InternalProfile).filter(db_mod.InternalProfile.id == int(secondary_profile_id)).first()
        if not secondary_profile:
            _public_error("Secondary internal profile not found.", 404)
    session = astro_chat.create_chat_session(
        db,
        profile=profile,
        secondary_profile=secondary_profile,
        title=form.get("title"),
        report_type=form.get("report_type") or ("parent_child" if secondary_profile else "career"),
        mode=form.get("mode"),
        admin_user=admin_user,
    )
    db.flush()
    return session


def _render_admin_astro_chat(request, db, *, session=None, selected_profile=None, result=None):
    profiles = astro_workspace.list_internal_profiles(db)
    sessions = db.query(db_mod.InternalChatSession).order_by(db_mod.InternalChatSession.updated_at.desc()).limit(30).all()
    return templates.TemplateResponse(
        request=request,
        name="admin/astro_workspace_chat.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            active_page="astro_workspace",
            profiles=profiles,
            sessions=sessions,
            chat_session=session,
            selected_profile=selected_profile or getattr(session, "profile", None),
            chat_modes=astro_chat.CHAT_MODES,
            report_options=_workspace_report_options(),
            result=result,
        ),
    )


def _generate_chat_reply_for_session(db, request, session, question, admin_user):
    question = str(question or "").strip()
    if not question:
        _public_error("Chat question is required.", 400)
    chart_payload = _workspace_chat_context_payload(db, session, admin_user)
    behavior_rules = ai_behavior_rules.load_active_rules(db)
    return astro_chat.create_grounded_reply(
        db,
        session=session,
        question=question,
        chart_payload=chart_payload,
        admin_user=admin_user,
        behavior_rules=behavior_rules,
    )


@app.get("/admin/astro-workspace", response_class=HTMLResponse)
@admin_required
async def admin_astro_workspace(request: Request, db: Session = Depends(get_db), profile_id: int | None = None):
    selected_profile = db.query(db_mod.InternalProfile).filter(db_mod.InternalProfile.id == profile_id).first() if profile_id else None
    profiles = astro_workspace.list_internal_profiles(db)
    current_language = _result_language(request, getattr(request.state, "admin_user", None))
    return templates.TemplateResponse(
        request=request,
        name="admin/astro_workspace.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            active_page="astro_workspace",
            report_options=_workspace_report_options(),
            profiles=profiles,
            form_data=astro_workspace.profile_to_form(selected_profile),
            gender_options=astro_workspace.gender_options(current_language),
            current_language=current_language,
            selected_profile=selected_profile,
            result=None,
        ),
    )


@app.post("/admin/astro-workspace/generate", response_class=HTMLResponse)
@admin_required
async def admin_astro_workspace_generate(request: Request, db: Session = Depends(get_db), csrf_token: str = Form(default="")):
    await validate_csrf_token(request, csrf_token)
    form = await request.form()
    admin_user = get_request_user(request, db)
    current_language = _result_language(request, admin_user)
    try:
        save_profile_requested = _workspace_form_flag(form, "save_profile")
        save_report_requested = _workspace_form_flag(form, "save_report") or _workspace_form_flag(form, "save_result")
        prepared = _prepare_admin_workspace_generation(db, form, admin_user, language=current_language)
        behavior_rules = ai_behavior_rules.load_active_rules(db)
        interpretation_text = astro_workspace.generate_workspace_interpretation(
            prepared["payload"],
            behavior_rules=behavior_rules,
            db=db,
        )
        if save_profile_requested and prepared["primary_profile"]:
            prepared["primary_profile"] = astro_workspace.create_or_update_internal_profile(
                db,
                prepared["primary_payload"],
                profile=prepared["primary_profile"],
                admin_user=admin_user,
                location_payload=prepared.get("primary_birth_context"),
            )
        saved_interpretation = None
        notices = []
        if save_profile_requested:
            notices.append("Profile saved.")
        if save_report_requested:
            if not str(interpretation_text or "").strip():
                raise ValueError("Interpretation must be generated before the report can be saved.")
            if not prepared["primary_profile"]:
                prepared["primary_profile"] = astro_workspace.create_or_update_internal_profile(
                    db,
                    prepared["primary_payload"],
                    admin_user=admin_user,
                    location_payload=prepared.get("primary_birth_context"),
                )
                notices.append("Report saved with internal profile.")
            if prepared.get("secondary_payload") and not prepared["secondary_profile"]:
                prepared["secondary_profile"] = astro_workspace.create_or_update_internal_profile(
                    db,
                    prepared["secondary_payload"],
                    admin_user=admin_user,
                    location_payload=prepared.get("secondary_birth_context"),
                )
            saved_interpretation = astro_workspace.save_internal_interpretation(
                db,
                profile=prepared["primary_profile"],
                secondary_profile=prepared["secondary_profile"],
                report_type=prepared["report_type"],
                payload=prepared["payload"],
                render_context=_internal_pdf_context(request, prepared["payload"], interpretation_text),
                interpretation_text=interpretation_text,
                admin_user=admin_user,
                generation_mode="saved",
            )
            if "Report saved with internal profile." not in notices:
                notices.append("Report saved.")
        db.commit()
        if saved_interpretation:
            db.refresh(saved_interpretation)
        if prepared.get("primary_profile"):
            db.refresh(prepared["primary_profile"])
        profiles = astro_workspace.list_internal_profiles(db)
        return templates.TemplateResponse(
            request=request,
            name="admin/astro_workspace_result.html",
            context=_auth_template_context(
                request,
                dashboard_user=request.state.admin_user,
                active_page="astro_workspace",
                report_options=_workspace_report_options(),
                profiles=profiles,
                form_data=dict(form),
                selected_profile=prepared["primary_profile"],
                gender_options=astro_workspace.gender_options(current_language),
                current_language=current_language,
                result={
                    "report_type": prepared["report_type"],
                    "payload_json": json.dumps(prepared["payload"], ensure_ascii=False, default=str),
                    "interpretation_text": interpretation_text,
                    "interpretation": saved_interpretation,
                    "notice": " ".join(dict.fromkeys(notices)),
                },
            ),
        )
    except ValueError as exc:
        profiles = astro_workspace.list_internal_profiles(db)
        return templates.TemplateResponse(
            request=request,
            name="admin/astro_workspace.html",
            status_code=400,
            context=_auth_template_context(
                request,
                dashboard_user=request.state.admin_user,
                active_page="astro_workspace",
                report_options=_workspace_report_options(),
                profiles=profiles,
                form_data=dict(form),
                gender_options=astro_workspace.gender_options(current_language),
                current_language=current_language,
                error=str(exc),
                result=None,
            ),
        )


@app.post("/admin/astro-workspace/export")
@admin_required
async def admin_astro_workspace_export(
    request: Request,
    interpretation_id: int | None = Form(default=None),
    payload_json: str = Form(default=""),
    interpretation_text: str = Form(default=""),
    csrf_token: str = Form(default=""),
    db: Session = Depends(get_db),
):
    await validate_csrf_token(request, csrf_token)
    interpretation = None
    if interpretation_id:
        interpretation = db.query(db_mod.InternalInterpretation).filter(db_mod.InternalInterpretation.id == interpretation_id).first()
        if not interpretation:
            _public_error("Internal interpretation not found.", 404)
        payload = json.loads(interpretation.input_payload_json)
        interpretation_text = interpretation.interpretation_text
    else:
        payload = json.loads(payload_json or "{}")
    context = _internal_pdf_context(request, payload, interpretation_text)
    pdf_bytes = _generate_pdf_bytes_from_report(context)
    if interpretation:
        output_path = astro_workspace.internal_pdf_output_path(BASE_DIR, interpretation.id)
        output_path.write_bytes(pdf_bytes)
        astro_workspace.attach_pdf_path(interpretation, output_path)
        db.commit()
    client_name = _sanitize_download_name(context.get("client_name") or "internal-profile", fallback="internal-profile")
    headers = {"Content-Disposition": f'attachment; filename="{client_name}_internal_astro_{datetime.now().strftime("%Y%m%d")}.pdf"'}
    return Response(content=pdf_bytes, media_type="application/pdf", headers=headers)


@app.get("/admin/astro-workspace/chat", response_class=HTMLResponse)
@admin_required
async def admin_astro_workspace_chat(
    request: Request,
    db: Session = Depends(get_db),
    session_id: int | None = None,
    profile_id: int | None = None,
):
    session = _chat_session_or_404(db, session_id) if session_id else None
    selected_profile = db.query(db_mod.InternalProfile).filter(db_mod.InternalProfile.id == profile_id).first() if profile_id else getattr(session, "profile", None)
    return _render_admin_astro_chat(request, db, session=session, selected_profile=selected_profile)


@app.post("/admin/astro-workspace/chat", response_class=HTMLResponse)
@admin_required
async def admin_astro_workspace_chat_submit(request: Request, db: Session = Depends(get_db), csrf_token: str = Form(default="")):
    await validate_csrf_token(request, csrf_token)
    form = await request.form()
    admin_user = get_request_user(request, db)
    question = str(form.get("question") or "").strip()
    if not question:
        _public_error("Chat question is required.", 400)
    session = None
    session_id = str(form.get("session_id") or "").strip()
    if session_id:
        session = _chat_session_or_404(db, session_id)
        session.mode = astro_chat.normalize_chat_mode(form.get("mode") or session.mode)
    else:
        session = _create_chat_session_from_form(db, form, admin_user)

    try:
        reply = _generate_chat_reply_for_session(db, request, session, question, admin_user)
        db.commit()
        db.refresh(session)
        return _render_admin_astro_chat(request, db, session=session, result=reply)
    except ValueError as exc:
        _public_error(str(exc), 400)


@app.post("/admin/astro-workspace/chat/sessions", response_class=HTMLResponse)
@admin_required
async def admin_astro_workspace_chat_create_session(request: Request, db: Session = Depends(get_db), csrf_token: str = Form(default="")):
    await validate_csrf_token(request, csrf_token)
    form = await request.form()
    admin_user = get_request_user(request, db)
    session = _create_chat_session_from_form(db, form, admin_user)
    question = str(form.get("question") or "").strip()
    result = None
    if question:
        result = _generate_chat_reply_for_session(db, request, session, question, admin_user)
    db.commit()
    db.refresh(session)
    return _render_admin_astro_chat(request, db, session=session, result=result)


@app.get("/admin/astro-workspace/chat/sessions/{session_id}", response_class=HTMLResponse)
@admin_required
async def admin_astro_workspace_chat_session_detail(request: Request, session_id: int, db: Session = Depends(get_db)):
    session = _chat_session_or_404(db, session_id)
    return _render_admin_astro_chat(request, db, session=session)


@app.post("/admin/astro-workspace/chat/sessions/{session_id}/message", response_class=HTMLResponse)
@admin_required
async def admin_astro_workspace_chat_session_message(
    request: Request,
    session_id: int,
    db: Session = Depends(get_db),
    csrf_token: str = Form(default=""),
):
    await validate_csrf_token(request, csrf_token)
    form = await request.form()
    admin_user = get_request_user(request, db)
    session = _chat_session_or_404(db, session_id)
    session.mode = astro_chat.normalize_chat_mode(form.get("mode") or session.mode)
    result = _generate_chat_reply_for_session(db, request, session, form.get("question"), admin_user)
    db.commit()
    db.refresh(session)
    return _render_admin_astro_chat(request, db, session=session, result=result)


@app.get("/admin/astro-workspace/quality/insights", response_class=HTMLResponse)
@admin_required
async def admin_astro_workspace_quality_insights(request: Request, db: Session = Depends(get_db), notice: str = ""):
    insights = quality_svc.list_prompt_insights(db)
    grouped = defaultdict(list)
    for item in insights:
        grouped[item.insight_type].append(item)
    return templates.TemplateResponse(
        request=request,
        name="admin/astro_workspace_quality_insights.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            active_page="astro_workspace",
            insights=insights,
            insights_by_type=dict(grouped),
            valid_insight_types=sorted(quality_svc.VALID_INSIGHT_TYPES),
            report_type_options=_workspace_report_options(),
            notice=notice,
        ),
    )


@app.post("/admin/astro-workspace/quality/insights/new", response_class=HTMLResponse)
@admin_required
async def admin_astro_workspace_quality_insights_new(request: Request, db: Session = Depends(get_db), csrf_token: str = Form(default="")):
    await validate_csrf_token(request, csrf_token)
    form = await request.form()
    admin_user = get_request_user(request, db)
    try:
        quality_svc.save_prompt_insight(db, dict(form), admin_user=admin_user)
        db.commit()
        return RedirectResponse(url="/admin/astro-workspace/quality/insights?notice=saved", status_code=303)
    except ValueError as exc:
        insights = quality_svc.list_prompt_insights(db)
        grouped = defaultdict(list)
        for item in insights:
            grouped[item.insight_type].append(item)
        return templates.TemplateResponse(
            request=request,
            name="admin/astro_workspace_quality_insights.html",
            status_code=400,
            context=_auth_template_context(
                request,
                dashboard_user=request.state.admin_user,
                active_page="astro_workspace",
                insights=insights,
                insights_by_type=dict(grouped),
                valid_insight_types=sorted(quality_svc.VALID_INSIGHT_TYPES),
                report_type_options=_workspace_report_options(),
                error=str(exc),
                form_data=dict(form),
            ),
        )


@app.get("/admin/astro-workspace/quality", response_class=HTMLResponse)
@admin_required
async def admin_astro_workspace_quality(request: Request, db: Session = Depends(get_db), notice: str = ""):
    dashboard = quality_svc.build_quality_dashboard(db)
    interpretations = db.query(db_mod.InternalInterpretation).order_by(db_mod.InternalInterpretation.created_at.desc()).limit(30).all()
    review_map = {review.interpretation_id: review for review in quality_svc.list_reviews(db, limit=200)}
    rows = []
    for interpretation in interpretations:
        profile_name = None
        if interpretation.profile:
            profile_name = interpretation.profile.full_name
        rows.append(
            {
                "interpretation": interpretation,
                "review": review_map.get(interpretation.id),
                "profile_name": profile_name or "Internal profile",
            }
        )
    return templates.TemplateResponse(
        request=request,
        name="admin/astro_workspace_quality.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            active_page="astro_workspace",
            dashboard=dashboard,
            interpretations=rows,
            notice=notice,
        ),
    )


@app.get("/admin/astro-workspace/quality/{interpretation_id}/review", response_class=HTMLResponse)
@admin_required
async def admin_astro_workspace_quality_review(request: Request, interpretation_id: int, db: Session = Depends(get_db), notice: str = ""):
    interpretation = db.query(db_mod.InternalInterpretation).filter(db_mod.InternalInterpretation.id == interpretation_id).first()
    if not interpretation:
        _public_error("Internal interpretation not found.", 404)
    review = quality_svc.get_review_for_interpretation(db, interpretation_id)
    return templates.TemplateResponse(
        request=request,
        name="admin/astro_workspace_quality_review.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            active_page="astro_workspace",
            interpretation=interpretation,
            review=review,
            valid_sections=sorted(quality_svc.VALID_SECTION_NAMES),
            valid_safety_flags=sorted(quality_svc.VALID_SAFETY_FLAGS),
            valid_statuses=sorted(quality_svc.VALID_STATUSES),
            quality_eval_data=_json_loads_safe(getattr(review, "quality_eval_json", None), {}),
            section_coverage_data=_json_loads_safe(getattr(review, "section_coverage_json", None), {}),
            missing_entities_data=_json_loads_safe(getattr(review, "missing_entities_json", None), []),
            notice=notice,
        ),
    )


@app.post("/admin/astro-workspace/quality/{interpretation_id}/review", response_class=HTMLResponse)
@admin_required
async def admin_astro_workspace_quality_review_save(
    request: Request,
    interpretation_id: int,
    db: Session = Depends(get_db),
    csrf_token: str = Form(default=""),
):
    await validate_csrf_token(request, csrf_token)
    form = await request.form()
    admin_user = get_request_user(request, db)
    try:
        quality_svc.save_review(db, interpretation_id, dict(form), admin_user=admin_user)
        db.commit()
        return RedirectResponse(url=f"/admin/astro-workspace/quality/{interpretation_id}/review?notice=saved", status_code=303)
    except ValueError as exc:
        interpretation = db.query(db_mod.InternalInterpretation).filter(db_mod.InternalInterpretation.id == interpretation_id).first()
        if not interpretation:
            _public_error("Internal interpretation not found.", 404)
        review = quality_svc.get_review_for_interpretation(db, interpretation_id)
        return templates.TemplateResponse(
            request=request,
            name="admin/astro_workspace_quality_review.html",
            status_code=400,
            context=_auth_template_context(
                request,
                dashboard_user=request.state.admin_user,
                active_page="astro_workspace",
                interpretation=interpretation,
                review=review,
                valid_sections=sorted(quality_svc.VALID_SECTION_NAMES),
                valid_safety_flags=sorted(quality_svc.VALID_SAFETY_FLAGS),
                valid_statuses=sorted(quality_svc.VALID_STATUSES),
                quality_eval_data=_json_loads_safe(getattr(review, "quality_eval_json", None), {}),
                section_coverage_data=_json_loads_safe(getattr(review, "section_coverage_json", None), {}),
                missing_entities_data=_json_loads_safe(getattr(review, "missing_entities_json", None), []),
                error=str(exc),
                form_data=dict(form),
            ),
        )


@app.get("/admin/astro-workspace/quality/{interpretation_id}/knowledge", response_class=HTMLResponse)
@admin_required
async def admin_astro_workspace_quality_knowledge_trace(request: Request, interpretation_id: int, db: Session = Depends(get_db), notice: str = ""):
    interpretation = db.query(db_mod.InternalInterpretation).filter(db_mod.InternalInterpretation.id == interpretation_id).first()
    if not interpretation:
        _public_error("Internal interpretation not found.", 404)
    trace = coverage_svc.get_knowledge_trace_for_interpretation(db, interpretation)
    suggested_tasks = coverage_svc.build_suggested_training_tasks_from_trace(trace)
    return templates.TemplateResponse(
        request=request,
        name="admin/interpretation_knowledge_trace.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            active_page="astro_workspace",
            interpretation=interpretation,
            trace=trace,
            suggested_tasks=suggested_tasks,
            notice=notice,
        ),
    )


@app.get("/admin/astro-workspace/profiles", response_class=HTMLResponse)
@admin_required
async def admin_astro_workspace_profiles(request: Request, db: Session = Depends(get_db), q: str = ""):
    profiles = astro_workspace.list_internal_profiles(db, q)
    return templates.TemplateResponse(
        request=request,
        name="admin/astro_workspace_profiles.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            active_page="astro_workspace",
            profiles=profiles,
            q=q,
        ),
    )


@app.get("/admin/astro-workspace/profiles/{profile_id}", response_class=HTMLResponse)
@admin_required
async def admin_astro_workspace_profile_detail(request: Request, profile_id: int, db: Session = Depends(get_db)):
    profile = db.query(db_mod.InternalProfile).filter(db_mod.InternalProfile.id == profile_id).first()
    if not profile:
        _public_error("Internal profile not found.", 404)
    interpretations = db.query(db_mod.InternalInterpretation).filter(
        or_(
            db_mod.InternalInterpretation.profile_id == profile.id,
            db_mod.InternalInterpretation.secondary_profile_id == profile.id,
        )
    ).order_by(db_mod.InternalInterpretation.created_at.desc()).all()
    return templates.TemplateResponse(
        request=request,
        name="admin/astro_workspace_profile_detail.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            active_page="astro_workspace",
            profile=profile,
            interpretations=astro_workspace.interpretation_history_views(interpretations),
        ),
    )


@app.post("/admin/astro-workspace/profiles/{profile_id}/favorite")
@admin_required
async def admin_astro_workspace_profile_favorite(request: Request, profile_id: int, csrf_token: str = Form(default=""), db: Session = Depends(get_db)):
    await validate_csrf_token(request, csrf_token)
    profile = db.query(db_mod.InternalProfile).filter(db_mod.InternalProfile.id == profile_id).first()
    if not profile:
        _public_error("Internal profile not found.", 404)
    profile.is_favorite = not bool(profile.is_favorite)
    db.commit()
    return RedirectResponse(url="/admin/astro-workspace/profiles", status_code=303)


@app.post("/admin/astro-workspace/profiles/{profile_id}/delete")
@admin_required
async def admin_astro_workspace_profile_delete(request: Request, profile_id: int, csrf_token: str = Form(default=""), db: Session = Depends(get_db)):
    await validate_csrf_token(request, csrf_token)
    profile = db.query(db_mod.InternalProfile).filter(db_mod.InternalProfile.id == profile_id).first()
    if not profile:
        _public_error("Internal profile not found.", 404)
    db.delete(profile)
    db.commit()
    return RedirectResponse(url="/admin/astro-workspace/profiles", status_code=303)


@app.get("/admin/training", response_class=HTMLResponse)
@admin_required
async def admin_training_hub(
    request: Request,
    db: Session = Depends(get_db),
    notice: str = "",
):
    total_items = db.query(db_mod.KnowledgeItem).count()
    review_pending = db.query(db_mod.KnowledgeItem).filter(
        db_mod.KnowledgeItem.status == "review_required"
    ).count()
    open_gaps = db.query(db_mod.KnowledgeGap).filter(
        db_mod.KnowledgeGap.status == "open"
    ).count()

    try:
        coverage = coverage_svc.compute_knowledge_coverage(db)
    except Exception:
        coverage = {"summary": {}, "by_category": {}}

    recent_items = (
        db.query(db_mod.KnowledgeItem)
        .order_by(db_mod.KnowledgeItem.updated_at.desc())
        .limit(8)
        .all()
    )

    priority_gaps = (
        db.query(db_mod.KnowledgeGap)
        .filter(db_mod.KnowledgeGap.status == "open")
        .order_by(db_mod.KnowledgeGap.created_at.desc())
        .limit(10)
        .all()
    )

    return templates.TemplateResponse(
        request=request,
        name="admin/training_hub.html",
        context=_ai_quality_context(
            request,
            active_page="training_hub",
            notice=notice,
            stats={
                "total_items": total_items,
                "review_pending": review_pending,
                "open_gaps": open_gaps,
                "coverage_pct": round(
                    coverage.get("summary", {}).get("overall_pct", 0), 1
                ),
            },
            coverage=coverage,
            recent_items=recent_items,
            priority_gaps=priority_gaps,
        ),
    )


@app.get("/admin/training/write", response_class=HTMLResponse)
@admin_required
async def admin_training_write(
    request: Request,
    db: Session = Depends(get_db),
    entity: str = "",
    notice: str = "",
):
    prefill_entity = str(entity or "").strip()
    detected_type = "reference"
    lowered_entity = prefill_entity.lower()
    if "yoga" in lowered_entity:
        detected_type = "yoga"
    elif "dasha" in lowered_entity:
        detected_type = "dasha"
    elif any(token in lowered_entity for token in ("nakshatra", "ashwini", "bharani", "rohini", "hasta", "chitra", "swati", "anuradha", "revati", "dhanishta")):
        detected_type = "nakshatra"
    return templates.TemplateResponse(
        request=request,
        name="admin/training_write.html",
        context=_ai_quality_context(
            request,
            active_page="training_write",
            prefill_entity=prefill_entity,
            prefill_item_type=detected_type,
            notice=notice,
            item_type_options=[
                "nakshatra", "yoga", "dasha", "concept",
                "house_interpretation", "planet_combination",
                "qa", "reference",
            ],
        ),
    )


@app.get("/admin/training/qa", response_class=HTMLResponse)
@admin_required
async def admin_training_qa(
    request: Request,
    db: Session = Depends(get_db),
    notice: str = "",
):
    recent_qa = (
        db.query(db_mod.KnowledgeItem)
        .filter(db_mod.KnowledgeItem.item_type == "qa")
        .order_by(db_mod.KnowledgeItem.updated_at.desc())
        .limit(5)
        .all()
    )
    return templates.TemplateResponse(
        request=request,
        name="admin/training_qa.html",
        context=_ai_quality_context(
            request,
            active_page="training_qa",
            notice=notice,
            recent_qa=recent_qa,
        ),
    )


@app.get("/admin/training/dashboard", response_class=HTMLResponse)
@admin_required
async def admin_training_dashboard(request: Request, db: Session = Depends(get_db)):
    evaluations = db.query(db_mod.EvaluationResult).order_by(db_mod.EvaluationResult.created_at.desc()).all()
    tasks = db.query(db_mod.TrainingTask).order_by(db_mod.TrainingTask.created_at.desc()).all()
    gaps = db.query(db_mod.KnowledgeGap).order_by(db_mod.KnowledgeGap.created_at.desc()).all()
    knowledge_items = db.query(db_mod.KnowledgeItem).order_by(db_mod.KnowledgeItem.updated_at.desc()).all()
    by_report_type = defaultdict(lambda: {"count": 0, "accuracy_total": 0.0, "depth_total": 0.0, "safety_total": 0.0})
    issue_frequency = defaultdict(int)
    for row in evaluations:
        key = str(row.report_type or "unknown")
        by_report_type[key]["count"] += 1
        by_report_type[key]["accuracy_total"] += float(row.accuracy_score or 0)
        by_report_type[key]["depth_total"] += float(row.depth_score or 0)
        by_report_type[key]["safety_total"] += float(row.safety_score or 0)
        for issue in _json_loads_safe(row.detected_issues_json, []):
            issue_frequency[str(issue)] += 1
    report_metrics = []
    for report_type, values in sorted(by_report_type.items()):
        count = max(values["count"], 1)
        report_metrics.append(
            {
                "report_type": report_type,
                "count": values["count"],
                "avg_accuracy": round(values["accuracy_total"] / count, 3),
                "avg_depth": round(values["depth_total"] / count, 3),
                "avg_safety": round(values["safety_total"] / count, 3),
            }
        )
    return templates.TemplateResponse(
        request=request,
        name="admin/training_dashboard.html",
        context=_ai_quality_context(
            request,
            active_page="training_dashboard",
            summary={
                "knowledge_items": len(knowledge_items),
                "evaluations": len(evaluations),
                "open_gaps": len([gap for gap in gaps if gap.status == "open"]),
                "open_tasks": len([task for task in tasks if task.status == "open"]),
            },
            report_metrics=report_metrics,
            issue_frequency=sorted(issue_frequency.items(), key=lambda item: item[1], reverse=True),
            recent_evaluations=evaluations[:12],
            recent_tasks=tasks[:12],
        ),
    )


@app.get("/admin/knowledge/coverage", response_class=HTMLResponse)
@admin_required
async def admin_knowledge_coverage(request: Request, db: Session = Depends(get_db), notice: str = ""):
    coverage = coverage_svc.compute_knowledge_coverage(db)
    return templates.TemplateResponse(
        request=request,
        name="admin/knowledge_coverage.html",
        context=_auth_template_context(
            request,
            dashboard_user=request.state.admin_user,
            active_page="astro_workspace",
            coverage=coverage,
            notice=notice,
        ),
    )


@app.post("/admin/knowledge/import-json")
@admin_required
async def admin_knowledge_import_json(
    request: Request,
    db: Session = Depends(get_db),
    csrf_token: str = Form(default=""),
    payload_json: str = Form(default=""),
):
    await validate_csrf_token(request, csrf_token)
    admin_user = get_request_user(request, db)
    try:
        parsed = json.loads(payload_json or "[]")
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid_json", "message": "payload_json must be valid JSON."}, status_code=400)
    payloads = parsed.get("items") if isinstance(parsed, dict) and isinstance(parsed.get("items"), list) else parsed
    try:
        imported = knowledge_import_service.import_deep_knowledge_items(db, payloads, admin_user=admin_user)
        db.commit()
        return JSONResponse(
            {
                "ok": True,
                "imported_count": len(imported),
                "knowledge_item_ids": [item.id for item in imported],
            }
        )
    except ValueError as exc:
        db.rollback()
        return JSONResponse({"ok": False, "error": "validation_error", "message": str(exc)}, status_code=400)


@app.get("/admin/knowledge/review", response_class=HTMLResponse)
@admin_required
async def admin_knowledge_review_list(request: Request, db: Session = Depends(get_db), notice: str = ""):
    items = _review_required_items(db)
    return templates.TemplateResponse(
        request=request,
        name="admin/knowledge_review.html",
        context=_ai_quality_context(
            request,
            active_page="knowledge_library",
            items=_knowledge_review_rows(items),
            notice=notice,
        ),
    )


@app.post("/admin/knowledge/review/bulk-approve")
@admin_required
async def admin_knowledge_review_bulk_approve(
    request: Request,
    db: Session = Depends(get_db),
    csrf_token: str = Form(default=""),
):
    await validate_csrf_token(request, csrf_token)
    form = await request.form()
    raw_ids = form.getlist("knowledge_ids")
    knowledge_ids = [int(value) for value in raw_ids if str(value or "").strip().isdigit()]
    approved_count = 0
    if knowledge_ids:
        items = db.query(db_mod.KnowledgeItem).filter(db_mod.KnowledgeItem.id.in_(knowledge_ids)).all()
        for item in items:
            _apply_knowledge_review_status(db, item, "published")
            approved_count += 1
    db.commit()
    return RedirectResponse(url=f"/admin/knowledge/review?notice={approved_count}+items+approved", status_code=303)


@app.post("/admin/knowledge/review/bulk-reject")
@admin_required
async def admin_knowledge_review_bulk_reject(
    request: Request,
    db: Session = Depends(get_db),
    csrf_token: str = Form(default=""),
):
    await validate_csrf_token(request, csrf_token)
    form = await request.form()
    raw_ids = form.getlist("knowledge_ids")
    knowledge_ids = [int(value) for value in raw_ids if str(value or "").strip().isdigit()]
    rejected_count = 0
    if knowledge_ids:
        items = db.query(db_mod.KnowledgeItem).filter(db_mod.KnowledgeItem.id.in_(knowledge_ids)).all()
        for item in items:
            _apply_knowledge_review_status(db, item, "rejected")
            rejected_count += 1
    db.commit()
    return RedirectResponse(url=f"/admin/knowledge/review?notice={rejected_count}+items+rejected", status_code=303)


@app.post("/admin/knowledge/review/auto-approve")
@admin_required
async def admin_knowledge_review_auto_approve(
    request: Request,
    db: Session = Depends(get_db),
    csrf_token: str = Form(default=""),
):
    await validate_csrf_token(request, csrf_token)
    approved_count = 0
    for item in _review_required_items(db):
        metadata = _knowledge_item_metadata(item)
        confidence_level = str(metadata.get("confidence_level") or "").strip().lower()
        sensitivity_level = str(metadata.get("sensitivity_level") or "").strip().lower()
        if confidence_level != "high":
            continue
        if sensitivity_level == "sensitive":
            continue
        if _is_auto_approve_blocked(item, metadata):
            continue
        _apply_knowledge_review_status(db, item, "published")
        approved_count += 1
    db.commit()
    return RedirectResponse(url=f"/admin/knowledge/review?notice={approved_count}+items+auto-approved", status_code=303)


@app.get("/admin/knowledge/review/{knowledge_id}", response_class=HTMLResponse)
@admin_required
async def admin_knowledge_review_detail(request: Request, knowledge_id: int, db: Session = Depends(get_db), notice: str = ""):
    item = db.query(db_mod.KnowledgeItem).filter(db_mod.KnowledgeItem.id == knowledge_id).first()
    if not item:
        _public_error("Knowledge item not found.", 404)
    metadata = _knowledge_item_metadata(item)
    return templates.TemplateResponse(
        request=request,
        name="admin/knowledge_review_detail.html",
        context=_ai_quality_context(
            request,
            active_page="knowledge_library",
            item=item,
            metadata=metadata,
            form_data={},
            notice=notice,
            error="",
        ),
    )


@app.post("/admin/knowledge/review/{knowledge_id}", response_class=HTMLResponse)
@admin_required
async def admin_knowledge_review_save(
    request: Request,
    knowledge_id: int,
    db: Session = Depends(get_db),
    csrf_token: str = Form(default=""),
):
    await validate_csrf_token(request, csrf_token)
    item = db.query(db_mod.KnowledgeItem).filter(db_mod.KnowledgeItem.id == knowledge_id).first()
    if not item:
        _public_error("Knowledge item not found.", 404)
    form = await request.form()
    metadata = _knowledge_item_metadata(item)
    for field in [
        "category",
        "entity_type",
        "primary_entity",
        "secondary_entity",
        "source_type",
        "source_title",
        "source_reference",
        "classical_view",
        "modern_synthesis",
        "interpretation_logic",
        "strong_condition",
        "weak_condition",
        "risk_pattern",
        "opportunity_pattern",
        "dasha_activation",
        "transit_activation",
        "safe_language_notes",
        "what_not_to_say",
        "premium_synthesis_sentence",
        "confidence_level",
        "sensitivity_level",
    ]:
        metadata[field] = str(form.get(field) or "").strip()
    metadata["tags"] = _csv_to_list(form.get("tags"))
    coverage_entities = [value.lower() for value in _csv_to_list(form.get("coverage_entities"))]
    metadata["coverage_entities"] = coverage_entities
    metadata["review_required"] = True
    title = str(form.get("title") or "").strip() or item.title
    item = knowledge_service.update_knowledge_item(
        db,
        item,
        title=title,
        body_text=_knowledge_review_body(metadata) or item.body_text,
        summary_text=metadata.get("premium_synthesis_sentence") or item.summary_text,
        entities=coverage_entities,
        metadata=metadata,
        status=str(item.status or "review_required").strip() or "review_required",
    )
    db.commit()
    return RedirectResponse(url=f"/admin/knowledge/review/{knowledge_id}?notice=saved", status_code=303)


@app.post("/admin/knowledge/review/{knowledge_id}/publish")
@admin_required
async def admin_knowledge_review_publish(
    request: Request,
    knowledge_id: int,
    db: Session = Depends(get_db),
    csrf_token: str = Form(default=""),
):
    await validate_csrf_token(request, csrf_token)
    item = db.query(db_mod.KnowledgeItem).filter(db_mod.KnowledgeItem.id == knowledge_id).first()
    if not item:
        _public_error("Knowledge item not found.", 404)
    _apply_knowledge_review_status(db, item, "published")
    db.commit()
    return RedirectResponse(url="/admin/knowledge/review?notice=published", status_code=303)


@app.post("/admin/knowledge/review/{knowledge_id}/reject")
@admin_required
async def admin_knowledge_review_reject(
    request: Request,
    knowledge_id: int,
    db: Session = Depends(get_db),
    csrf_token: str = Form(default=""),
):
    await validate_csrf_token(request, csrf_token)
    item = db.query(db_mod.KnowledgeItem).filter(db_mod.KnowledgeItem.id == knowledge_id).first()
    if not item:
        _public_error("Knowledge item not found.", 404)
    _apply_knowledge_review_status(db, item, "rejected")
    db.commit()
    return RedirectResponse(url="/admin/knowledge/review?notice=rejected", status_code=303)


@app.get("/admin/knowledge", response_class=HTMLResponse)
@admin_required
async def admin_knowledge_library(request: Request, db: Session = Depends(get_db), q: str = "", entity: str = ""):
    query = db.query(db_mod.KnowledgeItem)
    if str(q or "").strip():
        like = f"%{str(q).strip()}%"
        query = query.filter(or_(db_mod.KnowledgeItem.title.ilike(like), db_mod.KnowledgeItem.body_text.ilike(like)))
    items = query.order_by(db_mod.KnowledgeItem.updated_at.desc()).all()
    if str(entity or "").strip():
        items = [
            item
            for item in items
            if str(entity).strip().lower() in (_json_loads_safe(item.entities_json, []) or [])
        ]
    documents = db.query(db_mod.SourceDocument).order_by(db_mod.SourceDocument.updated_at.desc()).limit(50).all()
    return templates.TemplateResponse(
        request=request,
        name="admin/knowledge_library.html",
        context=_ai_quality_context(
            request,
            active_page="knowledge_library",
            items=items,
            documents=documents,
            filters={"q": q, "entity": entity},
        ),
    )


@app.get("/admin/documents", response_class=HTMLResponse)
@admin_required
async def admin_documents(request: Request, db: Session = Depends(get_db), notice: str = ""):
    documents = db.query(db_mod.SourceDocument).order_by(db_mod.SourceDocument.updated_at.desc()).all()
    rows = []
    for document in documents:
        metadata = _json_loads_safe(document.metadata_json, {}) or {}
        chunk_count = sum(len(item.chunks or []) for item in (document.knowledge_items or []))
        status = str(metadata.get("processing_status") or ("processed" if chunk_count else "uploaded"))
        diagnostics = metadata.get("parser_diagnostics") or {}
        rows.append(
            {
                "document": document,
                "processing_status": status,
                "chunk_count": chunk_count,
                "file_name": Path(str(document.file_path or "")).name if str(document.file_path or "").strip() else None,
                "parser_used": diagnostics.get("parser_used") or "-",
                "page_count": diagnostics.get("page_count") or 0,
                "block_count": diagnostics.get("block_count") or 0,
                "preview": diagnostics.get("preview") or "",
                "parse_error": diagnostics.get("error"),
            }
        )
    return templates.TemplateResponse(
        request=request,
        name="admin/documents.html",
        context=_ai_quality_context(
            request,
            active_page="documents",
            documents=rows,
            notice=notice,
        ),
    )


@app.post("/admin/documents/upload")
@admin_required
async def admin_documents_upload(
    request: Request,
    db: Session = Depends(get_db),
    csrf_token: str = Form(default=""),
    title: str = Form(default=""),
    document_type: str = Form(default="book"),
    file: UploadFile | None = File(default=None),
):
    await validate_csrf_token(request, csrf_token)
    admin_user = get_request_user(request, db)
    if file is None or not str(getattr(file, "filename", "") or "").strip():
        return RedirectResponse(url="/admin/documents?notice=missing_file", status_code=303)
    filename = str(getattr(file, "filename", "") or "")
    if not filename.lower().endswith(".pdf"):
        return RedirectResponse(url="/admin/documents?notice=invalid_pdf", status_code=303)
    upload_dir = get_upload_dir()
    upload_dir.mkdir(parents=True, exist_ok=True)
    stored_name = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{secrets.token_hex(4)}.pdf"
    destination = upload_dir / stored_name
    contents = await file.read()
    destination.write_bytes(contents)
    source_document = db_mod.SourceDocument(
        title=str(title or "").strip() or Path(filename).stem or "Uploaded document",
        file_path=str(destination),
        document_type=str(document_type or "book").strip() or "book",
        source_label=filename,
        content_text=None,
        created_by_user_id=getattr(admin_user, "id", None),
        uploaded_at=datetime.utcnow(),
    )
    db.add(source_document)
    db.flush()

    diagnostics = document_parser.parse_pdf_with_diagnostics(destination)
    text_blocks = list(diagnostics.get("blocks") or [])
    page_blocks = list(diagnostics.get("page_blocks") or [])
    chunk_input = page_blocks or text_blocks
    meaningful_chunks = document_chunker.chunk_text_blocks(chunk_input)
    source_document.content_text = "\n\n".join(
        (block.get("text") if isinstance(block, dict) else str(block)) for block in (page_blocks or text_blocks)
    ) if (page_blocks or text_blocks) else None
    source_document.metadata_json = json.dumps(
        {
            "processing_status": "processed" if meaningful_chunks else "parse_empty",
            "block_count": len(text_blocks),
            "chunk_count": len(meaningful_chunks),
            "parser_diagnostics": diagnostics,
        },
        ensure_ascii=False,
    )
    for index, chunk in enumerate(meaningful_chunks, start=1):
        chunk_title = str(chunk.get("title") or f"{source_document.title} {index}").strip()
        coverage_entities = chunk.get("coverage_entities") or []
        item = knowledge_service.create_knowledge_item(
            db,
            title=chunk_title,
            body_text=str(chunk.get("text") or "").strip(),
            language="tr",
            item_type=str(chunk.get("category") or "reference"),
            summary_text=str(chunk.get("topic") or "").strip() or chunk_title,
            entities=coverage_entities,
            source_document=source_document,
                metadata={
                    "document_id": source_document.id,
                    "document_type": source_document.document_type,
                    "topic": chunk.get("topic"),
                    "primary_entity": chunk.get("entity"),
                    "category": chunk.get("category"),
                    "source_file_path": str(destination),
                    "source_page_start": chunk.get("source_page_start"),
                    "source_page_end": chunk.get("source_page_end"),
                    "is_toc": bool(chunk.get("is_toc")),
                    "is_index": bool(chunk.get("is_index")),
                    "noise_score": chunk.get("noise_score"),
                },
                created_by_user_id=getattr(admin_user, "id", None),
            )
        if not (item.chunks or []):
            chunk_text = str(chunk.get("text") or "").strip()
            if chunk_text:
                vector = embedding_service.generate_embedding(chunk_text)
                db.add(
                    db_mod.KnowledgeChunk(
                        knowledge_item=item,
                        chunk_index=0,
                        chunk_text=chunk_text,
                        embedding_json=embedding_service.serialize_embedding(vector),
                        entities_json=json.dumps(coverage_entities, ensure_ascii=False),
                        coverage_entities_json=json.dumps(coverage_entities, ensure_ascii=False),
                        token_count=len(chunk_text.split()),
                    )
                )
    db.commit()
    if not meaningful_chunks:
        return RedirectResponse(url="/admin/documents?notice=parse_empty", status_code=303)
    return RedirectResponse(url=f"/admin/documents?notice=uploaded:{len(meaningful_chunks)}", status_code=303)


@app.post("/admin/documents/{document_id}/extract-nakshatra")
@admin_required
async def admin_document_extract_nakshatra(
    request: Request,
    document_id: int,
    db: Session = Depends(get_db),
    csrf_token: str = Form(default=""),
):
    await validate_csrf_token(request, csrf_token)
    admin_user = get_request_user(request, db)
    document = db.query(db_mod.SourceDocument).filter(db_mod.SourceDocument.id == document_id).first()
    if not document:
        _public_error("Document not found.", 404)
    diagnostics = document_parser.parse_pdf_with_diagnostics(document.file_path)
    text_blocks = list(diagnostics.get("blocks") or [])
    page_blocks = list(diagnostics.get("page_blocks") or [])
    if not text_blocks:
        metadata = _json_loads_safe(document.metadata_json, {}) or {}
        metadata["processing_status"] = "parse_empty"
        metadata["parser_diagnostics"] = diagnostics
        document.metadata_json = json.dumps(metadata, ensure_ascii=False)
        db.commit()
        return RedirectResponse(url="/admin/documents?notice=parse_empty", status_code=303)
    extracted = nakshatra_extraction_service.extract_nakshatra_knowledge_from_document(
        page_blocks or text_blocks,
        document.title,
    )
    if not extracted.get("section_count", 0):
        metadata = _json_loads_safe(document.metadata_json, {}) or {}
        metadata["processing_status"] = str(metadata.get("processing_status") or "processed")
        metadata["parser_diagnostics"] = diagnostics
        metadata["nakshatra_extraction"] = {
            "section_count": 0,
            "chunk_count": 0,
        }
        document.metadata_json = json.dumps(metadata, ensure_ascii=False)
        db.commit()
        return RedirectResponse(url="/admin/documents?notice=nakshatra_sections_empty", status_code=303)
    created_count = 0
    for section in extracted.get("sections") or []:
        for chunk in section.get("suggested_chunks") or []:
            coverage_entities = [entity for entity in (chunk.get("coverage_entities") or []) if str(entity or "").strip()]
            knowledge_service.create_knowledge_item(
                db,
                title=chunk.get("title") or "Nakshatra review chunk",
                body_text=chunk.get("classical_view") or "",
                language="tr",
                item_type="nakshatra",
                summary_text=chunk.get("premium_synthesis_sentence") or "",
                entities=coverage_entities,
                source_document=document,
                metadata={**chunk, "review_required": True, "status": "review_required"},
                created_by_user_id=getattr(admin_user, "id", None),
                status="review_required",
            )
            created_count += 1
    metadata = _json_loads_safe(document.metadata_json, {}) or {}
    metadata["parser_diagnostics"] = diagnostics
    metadata["nakshatra_extraction"] = {
        "section_count": extracted.get("section_count", 0),
        "chunk_count": created_count,
    }
    db.execute(
        sa_text(
            "UPDATE source_documents SET metadata_json = :metadata WHERE id = :document_id"
        ),
        {
            "metadata": json.dumps(metadata, ensure_ascii=False),
            "document_id": document.id,
        },
    )
    db.commit()
    return RedirectResponse(
        url=f"/admin/documents?notice=nakshatra_extracted:{extracted.get('section_count', 0)}:{created_count}",
        status_code=303,
    )


@app.post("/admin/knowledge")
@admin_required
async def admin_knowledge_library_create(request: Request, db: Session = Depends(get_db), csrf_token: str = Form(default="")):
    await validate_csrf_token(request, csrf_token)
    form = await request.form()
    admin_user = get_request_user(request, db)
    title = str(form.get("title") or "").strip()
    body_text = str(form.get("body_text") or "").strip()
    if not title or not body_text:
        raise HTTPException(status_code=400, detail="Knowledge title and body are required.")
    source_title = str(form.get("source_title") or "").strip()
    source_document = None
    if source_title:
        source_document = db_mod.SourceDocument(
            title=source_title,
            document_type=str(form.get("document_type") or "note").strip() or "note",
            source_label=str(form.get("source_label") or "").strip() or None,
            source_uri=str(form.get("source_uri") or "").strip() or None,
            language="en" if str(form.get("language") or "tr").lower() == "en" else "tr",
            content_text=body_text,
            created_by_user_id=getattr(admin_user, "id", None),
        )
        db.add(source_document)
        db.flush()
    knowledge_service.create_knowledge_item(
        db,
        title=title,
        body_text=body_text,
        language=form.get("language") or "tr",
        item_type=form.get("item_type") or "reference",
        summary_text=form.get("summary_text") or "",
        entities=[part.strip() for part in str(form.get("entities") or "").split(",") if part.strip()],
        source_document=source_document,
        metadata={"source_hint": str(form.get("source_label") or "").strip() or None},
        created_by_user_id=getattr(admin_user, "id", None),
    )
    db.commit()
    return RedirectResponse(url="/admin/knowledge", status_code=303)


@app.get("/admin/gaps", response_class=HTMLResponse)
@admin_required
async def admin_knowledge_gaps(request: Request, db: Session = Depends(get_db), status: str = "open"):
    query = db.query(db_mod.KnowledgeGap)
    if str(status or "").strip():
        query = query.filter(db_mod.KnowledgeGap.status == str(status).strip())
    gaps = query.order_by(db_mod.KnowledgeGap.created_at.desc()).all()
    return templates.TemplateResponse(
        request=request,
        name="admin/knowledge_gaps.html",
        context=_ai_quality_context(
            request,
            active_page="knowledge_gaps",
            gaps=gaps,
            status=status,
        ),
    )


@app.get("/admin/evaluations", response_class=HTMLResponse)
@admin_required
async def admin_evaluation_logs(request: Request, db: Session = Depends(get_db), report_type: str = ""):
    query = db.query(db_mod.EvaluationResult)
    if str(report_type or "").strip():
        query = query.filter(db_mod.EvaluationResult.report_type == str(report_type).strip())
    evaluations = query.order_by(db_mod.EvaluationResult.created_at.desc()).all()
    return templates.TemplateResponse(
        request=request,
        name="admin/evaluation_logs.html",
        context=_ai_quality_context(
            request,
            active_page="evaluation_logs",
            evaluations=evaluations,
            report_type=report_type,
        ),
    )


@app.get("/admin/tasks", response_class=HTMLResponse)
@admin_required
async def admin_training_tasks(request: Request, db: Session = Depends(get_db), status: str = ""):
    if not str(status or "").strip():
        training_service.generate_training_tasks_from_gaps(db, created_by_user_id=getattr(request.state.admin_user, "id", None))
        db.commit()
    query = db.query(db_mod.TrainingTask)
    if str(status or "").strip():
        query = query.filter(db_mod.TrainingTask.status == str(status).strip())
    tasks = query.order_by(db_mod.TrainingTask.created_at.desc()).all()
    return templates.TemplateResponse(
        request=request,
        name="admin/training_tasks.html",
        context=_ai_quality_context(
            request,
            active_page="training_tasks",
            tasks=tasks,
            status=status,
        ),
    )


@app.get("/admin/test-playground", response_class=HTMLResponse)
@admin_required
async def admin_test_playground(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse(
        request=request,
        name="admin/test_playground.html",
        context=_ai_quality_context(
            request,
            active_page="test_playground",
            result=None,
            form_data={},
        ),
    )


@app.post("/admin/test-playground", response_class=HTMLResponse)
@admin_required
async def admin_test_playground_submit(request: Request, db: Session = Depends(get_db), csrf_token: str = Form(default="")):
    await validate_csrf_token(request, csrf_token)
    form = await request.form()
    admin_user = get_request_user(request, db)
    chart_data = _json_loads_safe(form.get("chart_data_json"), {}) if str(form.get("chart_data_json") or "").strip() else {}
    output_text = str(form.get("output_text") or "").strip()
    if _workspace_form_flag(form, "generate_output") and chart_data:
        try:
            output_text = ai_logic.generate_interpretation(chart_data)
        except Exception as exc:
            output_text = output_text or f"[generation unavailable: {exc}]"
    evaluation = evaluation_service.evaluate_interpretation(output_text, chart_data)
    evaluation_row = db_mod.EvaluationResult(
        report_type=str(chart_data.get("report_type") or "").strip() or None,
        language="en" if str(chart_data.get("language") or "tr").lower() == "en" else "tr",
        chart_data_json=json.dumps(chart_data, ensure_ascii=False, default=str),
        output_text=output_text or "",
        accuracy_score=float(evaluation.get("accuracy_score") or 0.0),
        depth_score=float(evaluation.get("depth_score") or 0.0),
        safety_score=float(evaluation.get("safety_score") or 0.0),
        detected_issues_json=json.dumps(evaluation.get("detected_issues") or [], ensure_ascii=False),
        metadata_json=json.dumps(evaluation.get("metadata") or {}, ensure_ascii=False),
        created_by_user_id=getattr(admin_user, "id", None),
    )
    db.add(evaluation_row)
    db.flush()
    gaps = gap_detector.detect_knowledge_gaps(
        db,
        chart_data=chart_data,
        output=output_text,
        evaluation_result=evaluation,
        report_type=chart_data.get("report_type"),
        language=chart_data.get("language") or "tr",
    )
    db.commit()
    return templates.TemplateResponse(
        request=request,
        name="admin/test_playground.html",
        context=_ai_quality_context(
            request,
            active_page="test_playground",
            form_data={"chart_data_json": json.dumps(chart_data, ensure_ascii=False, indent=2), "output_text": output_text},
            result={"evaluation": evaluation, "evaluation_row": evaluation_row, "gaps": gaps},
        ),
    )


@app.get("/admin/segments", response_class=HTMLResponse)
@admin_required
async def admin_segments(
    request: Request,
    db: Session = Depends(get_db),
    segment: str = "",
    group: str = "",
    priority: str = "",
    channel: str = "",
    message_type: str = "",
    view: str = "crm",
):
    try:
        selected_segment_names, selected_group, selected_priority, error_message = _resolve_segment_filters(segment, group, priority)
        if error_message:
            logger.warning("Admin segments invalid filter segment=%s group=%s priority=%s", segment, group, priority)
            _public_error(error_message, 400)

        segment_context = generate_campaign_ready_segments(db)
        if selected_priority:
            selected_segment_names = [
                name for name in selected_segment_names
                if segment_context["segment_meta"].get(name, {}).get("priority") == selected_priority
            ]
        normalized_channel = str(channel or "").strip().lower()
        normalized_message_type = str(message_type or "").strip().lower()
        if normalized_channel:
            selected_segment_names = [
                name for name in selected_segment_names
                if str(segment_context["segment_meta"].get(name, {}).get("recommended_channel", "")).strip().lower() == normalized_channel
            ]
        if normalized_message_type:
            selected_segment_names = [
                name for name in selected_segment_names
                if str(segment_context["segment_meta"].get(name, {}).get("recommended_message_type", "")).strip().lower() == normalized_message_type
            ]
        selected_view, export_columns = build_export_columns_for_view(view)
        if not export_columns:
            logger.warning("Admin segments invalid export view=%s", view)
            _public_error(f"Unknown export view: {view}", 400)
        filtered_segments = {name: segment_context["segments"].get(name, []) for name in selected_segment_names}
        filtered_meta = {name: segment_context["segment_meta"].get(name, {}) for name in selected_segment_names}
        filtered_briefs = {name: segment_context["campaign_briefs"].get(name, {}) for name in selected_segment_names}
        logger.info(
            "Admin campaign console viewed group=%s segment=%s priority=%s channel=%s message_type=%s view=%s",
            selected_group or "-", segment or "-", selected_priority or "-", normalized_channel or "-", normalized_message_type or "-", selected_view,
        )
        return templates.TemplateResponse(
            request=request,
            name="admin/segments.html",
            context=_auth_template_context(
                request,
                dashboard_user=request.state.admin_user,
                summary={name: segment_context["summary"].get(name, 0) for name in selected_segment_names},
                segments=filtered_segments,
                groups=SEGMENT_GROUPS,
                segment_meta=filtered_meta,
                campaign_briefs=filtered_briefs,
                export_view=selected_view,
                filters={"segment": segment, "group": group, "priority": priority, "channel": channel, "message_type": message_type, "view": selected_view},
            ),
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception("Admin segments failed")
        _public_error("Admin segments sayfasi yuklenemedi.", 500)


@app.get("/admin/segments/export")
@admin_required
async def admin_segments_export(
    request: Request,
    db: Session = Depends(get_db),
    segment: str = "",
    group: str = "",
    priority: str = "",
    channel: str = "",
    message_type: str = "",
    view: str = "crm",
):
    try:
        selected_segment_names, selected_group, selected_priority, error_message = _resolve_segment_filters(segment, group, priority)
        if error_message:
            logger.warning("Admin segments export invalid filter segment=%s group=%s priority=%s", segment, group, priority)
            _public_error(error_message, 400)

        segment_context = generate_campaign_ready_segments(db)
        if selected_priority:
            selected_segment_names = [
                name for name in selected_segment_names
                if segment_context["segment_meta"].get(name, {}).get("priority") == selected_priority
            ]
        normalized_channel = str(channel or "").strip().lower()
        normalized_message_type = str(message_type or "").strip().lower()
        if normalized_channel:
            selected_segment_names = [
                name for name in selected_segment_names
                if str(segment_context["segment_meta"].get(name, {}).get("recommended_channel", "")).strip().lower() == normalized_channel
            ]
        if normalized_message_type:
            selected_segment_names = [
                name for name in selected_segment_names
                if str(segment_context["segment_meta"].get(name, {}).get("recommended_message_type", "")).strip().lower() == normalized_message_type
            ]
        selected_view, export_columns = build_export_columns_for_view(view)
        if not export_columns:
            logger.warning("Admin segments export invalid view=%s", view)
            _public_error(f"Unknown export view: {view}", 400)
        rows = []
        for segment_name in selected_segment_names:
            base_rows = _segment_export_rows(segment_context["segments"], [segment_name])
            campaign_brief = segment_context["campaign_briefs"].get(segment_name, {})
            rows.extend(build_campaign_export_row(row, campaign_brief) for row in base_rows)

        buffer = StringIO()
        writer = csv.DictWriter(
            buffer,
            fieldnames=export_columns,
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column) for column in export_columns})

        today_stamp = datetime.utcnow().strftime("%Y%m%d")
        if len(selected_segment_names) == 1:
            base_name = _sanitize_download_name(selected_segment_names[0].lower(), fallback="segments")
        elif selected_group:
            base_name = _sanitize_download_name(f"{selected_group}_segments", fallback="segments")
        elif selected_priority:
            base_name = _sanitize_download_name(f"{selected_priority}_segments", fallback="segments")
        else:
            base_name = "lifecycle_segments"
        filename = f"{base_name}_{selected_view}_{today_stamp}.csv"
        logger.info(
            "Admin campaign export generated filename=%s rows=%s priority=%s channel=%s message_type=%s view=%s",
            filename, len(rows), selected_priority or "-", normalized_channel or "-", normalized_message_type or "-", selected_view,
        )
        return Response(
            content=buffer.getvalue(),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception("Admin segments export failed")
        _public_error("Segment export olusturulamadi.", 500)


@app.post("/webhooks/payments")
async def payments_webhook(request: Request, db: Session = Depends(get_db)):
    try:
        provider = payments.get_payment_provider()
        payment_data = provider.verify_webhook(
            {
                "payload_bytes": await request.body(),
                "signature_header": request.headers.get("stripe-signature", ""),
                "headers": dict(request.headers),
            }
        )
        if not payment_data:
            return {"status": "ignored"}
        report = db.query(db_mod.GeneratedReport).filter(
            db_mod.GeneratedReport.id == int(payment_data["report_id"]),
            db_mod.GeneratedReport.user_id == int(payment_data["user_id"]),
        ).first()
        if not report:
            logger.warning(
                "Payments webhook report not found report_id=%s user_id=%s",
                payment_data.get("report_id"),
                payment_data.get("user_id"),
            )
            return {"status": "ignored", "reason": "report_not_found"}
        _finalize_report_purchase(report, payment_data)
        mark_email_capture_converted(db, report=report, email=getattr(getattr(report, "user", None), "email", None))
        db.commit()
        return {"status": "ok"}
    except payments.PaymentError as exc:
        logger.warning("Payments webhook rejected detail=%s", exc)
        _public_error("Payment webhook could not be verified.", 400)
    except Exception:
        logger.exception("Payments webhook processing failed")
        _public_error("Payment webhook processing failed.", 400)


@app.post("/api/v1/email-capture")
async def api_email_capture(request: Request, db: Session = Depends(get_db)):
    current_language = _result_language(request, get_request_user(request, db))
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse(content={"ok": False, "error": "invalid_payload"}, status_code=400)

    email = payload.get("email")
    report_id = payload.get("report_id")
    source = payload.get("source") or "result_page"

    try:
        capture, created = capture_email_lead(db, email=email, report_id=report_id, source=source)
        db.commit()
        return JSONResponse(
            content={
                "ok": True,
                "capture": {
                    "id": capture.id,
                    "email": capture.email,
                    "report_id": capture.report_id,
                    "source": capture.source,
                    "is_converted": bool(capture.is_converted),
                },
                "created": created,
                "message": "Okumanız daha sonra bakmak üzere kaydedildi." if current_language == "tr" else "Your reading has been saved for later.",
            }
        )
    except ValueError as exc:
        return JSONResponse(content={"ok": False, "error": str(exc)}, status_code=400)
    except Exception:
        logger.exception("Email capture failed report_id=%s source=%s", report_id, source)
        return JSONResponse(content={"ok": False, "error": "email_capture_failed"}, status_code=500)


@app.post("/api/v1/feedback")
async def api_feedback(request: Request, db: Session = Depends(get_db)):
    current_user = get_request_user(request, db)
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse(content={"ok": False, "error": "invalid_payload"}, status_code=400)

    try:
        entry = save_feedback_entry(db, payload, user=current_user)
        db.commit()
        return JSONResponse(
            content={
                "ok": True,
                "feedback": {
                    "id": entry.id,
                    "report_id": entry.report_id,
                    "report_type": entry.report_type,
                    "stage": entry.stage,
                    "rating": entry.rating,
                    "comment": entry.comment,
                    "recommend_flag": entry.recommend_flag,
                    "created_at": entry.created_at.isoformat() if entry.created_at else None,
                },
                "message": "Thanks for your feedback",
            }
        )
    except ValueError as exc:
        return JSONResponse(content={"ok": False, "error": str(exc)}, status_code=400)
    except LookupError as exc:
        return JSONResponse(content={"ok": False, "error": str(exc)}, status_code=404)
    except Exception:
        logger.exception("Feedback submission failed")
        return JSONResponse(content={"ok": False, "error": "feedback_save_failed"}, status_code=500)


@app.post("/billing/webhook")
async def billing_webhook(request: Request, db: Session = Depends(get_db)):
    try:
        expected_token = os.getenv("BILLING_WEBHOOK_TOKEN", "").strip()
        if expected_token:
            provided_token = request.headers.get("x-billing-webhook-token", "").strip()
            if provided_token != expected_token:
                logger.warning("Billing webhook rejected due to invalid token")
                _public_error("Webhook yetkilendirilemedi.", 401)
        payload = await request.json()
        result = process_billing_notification_event(db, payload)
        return {"status": "ok", "result": result}
    except Exception:
        logger.exception("Billing webhook processing failed")
        _public_error("Billing bildirimi islenemedi.", 400)


@app.post("/calculate", response_class=HTMLResponse)
async def calculate_from_form(
    request: Request,
    full_name: str = Form(default=""),
    birth_date: str = Form(default=""),
    birth_time: str = Form(default=""),
    birth_city: str = Form(default=""),
    country: str = Form(default=""),
    resolved_birth_place: str = Form(default=""),
    resolved_latitude: str = Form(default=""),
    resolved_longitude: str = Form(default=""),
    resolved_timezone: str = Form(default=""),
    resolved_geocode_provider: str = Form(default=""),
    resolved_geocode_confidence: str = Form(default=""),
    report_type: str = Form(default="premium"),
    parent_full_name: str = Form(default=""),
    parent_birth_date: str = Form(default=""),
    parent_birth_time: str = Form(default=""),
    parent_birth_city: str = Form(default=""),
    parent_country: str = Form(default=""),
    parent_resolved_birth_place: str = Form(default=""),
    parent_resolved_latitude: str = Form(default=""),
    parent_resolved_longitude: str = Form(default=""),
    parent_resolved_timezone: str = Form(default=""),
    parent_resolved_geocode_provider: str = Form(default=""),
    parent_resolved_geocode_confidence: str = Form(default=""),
    child_full_name: str = Form(default=""),
    child_birth_date: str = Form(default=""),
    child_birth_time: str = Form(default=""),
    child_birth_city: str = Form(default=""),
    child_country: str = Form(default=""),
    child_resolved_birth_place: str = Form(default=""),
    child_resolved_latitude: str = Form(default=""),
    child_resolved_longitude: str = Form(default=""),
    child_resolved_timezone: str = Form(default=""),
    child_resolved_geocode_provider: str = Form(default=""),
    child_resolved_geocode_confidence: str = Form(default=""),
    csrf_token: str = Form(default=""),
    db: Session = Depends(get_db),
):
    if not launch_free_calculator_enabled():
        return _coming_soon_template_response(request)
    enforce_rate_limit(request, "calculate", limit=8, window_seconds=600)
    await validate_csrf_token(request, csrf_token)
    try:
        current_user = get_request_user(request, db)
        current_language = _result_language(request, current_user)
        if resolved_birth_place and resolved_latitude and resolved_longitude and resolved_timezone:
            _log_birthplace_event(
                db,
                "submit_with_selected_suggestion",
                provider=resolved_geocode_provider or "suggestion",
                outcome="submitted",
                location_source="suggestion_selection",
                confidence=float(resolved_geocode_confidence) if resolved_geocode_confidence not in (None, "") else None,
            )
        else:
            _log_birthplace_event(
                db,
                "submit_without_selected_suggestion",
                provider=None,
                outcome="submitted",
                location_source="manual_input",
            )
        report_type, access_notice = resolve_report_type_for_user(current_user, report_type)
        report_type, report_type_config = get_report_type_config(report_type)
        if report_type == "parent_child":
            if not all([
                parent_full_name.strip(), parent_birth_date.strip(), parent_birth_time.strip(), parent_birth_city.strip(),
                child_full_name.strip(), child_birth_date.strip(), child_birth_time.strip(), child_birth_city.strip(),
            ]):
                _public_error("Parent ve child dogum bilgilerini birlikte girmeniz gerekiyor.", 400)

            parent_birth_context = _build_birth_context(
                f"{parent_birth_date}T{parent_birth_time}",
                parent_birth_city,
                parent_country,
                resolved_location_payload=_resolved_birth_location_payload_from_form(
                    birth_city=parent_birth_city,
                    country=parent_country,
                    resolved_birth_place=parent_resolved_birth_place,
                    resolved_latitude=parent_resolved_latitude,
                    resolved_longitude=parent_resolved_longitude,
                    resolved_timezone=parent_resolved_timezone,
                    geocode_provider=parent_resolved_geocode_provider,
                    geocode_confidence=parent_resolved_geocode_confidence,
                ),
            )
            child_birth_context = _build_birth_context(
                f"{child_birth_date}T{child_birth_time}",
                child_birth_city,
                child_country,
                resolved_location_payload=_resolved_birth_location_payload_from_form(
                    birth_city=child_birth_city,
                    country=child_country,
                    resolved_birth_place=child_resolved_birth_place,
                    resolved_latitude=child_resolved_latitude,
                    resolved_longitude=child_resolved_longitude,
                    resolved_timezone=child_resolved_timezone,
                    geocode_provider=child_resolved_geocode_provider,
                    geocode_confidence=child_resolved_geocode_confidence,
                ),
            )
            parent_bundle = _calculate_chart_bundle_from_birth_context(parent_birth_context)
            child_bundle = _calculate_chart_bundle_from_birth_context(child_birth_context)
            parent_bundle["name"] = parent_full_name.strip()
            child_bundle["name"] = child_full_name.strip()
            interpretation_context = build_parent_child_interpretation(parent_bundle, child_bundle)
            interpretation_context = _localize_result_layer_text(interpretation_context, current_language)
            ai_interpretation = build_parent_child_ai_summary(interpretation_context)
            child_meta = child_bundle["calculation_config"]
            result_data = {
                "request": request,
                "language": current_language,
                "full_name": child_full_name,
                "birth_date": child_birth_date,
                "birth_time": child_birth_time,
                "birth_city": child_birth_context["normalized_birth_place"],
                "birth_country": child_country,
                "raw_birth_place_input": child_birth_context["raw_birth_place_input"],
                "normalized_birth_place": child_birth_context["normalized_birth_place"],
                "latitude": child_birth_context["latitude"],
                "longitude": child_birth_context["longitude"],
                "timezone": child_birth_context["timezone"],
                "geocode_provider": child_birth_context["geocode_provider"],
                "geocode_confidence": child_birth_context["geocode_confidence"],
                "location_source": child_birth_context.get("location_source"),
                "calculation_config": child_meta,
                "report_type": report_type,
                "report_type_config": report_type_config,
                "access_notice": access_notice,
                "natal_data": _serialize_temporal_values(child_bundle["natal_data"]),
                "dasha_data": _serialize_temporal_values(child_bundle["dasha_data"]),
                "navamsa_data": _serialize_temporal_values(child_bundle["navamsa_data"]),
                "transit_data": _serialize_temporal_values(child_bundle["transit_data"]),
                "eclipse_data": _serialize_temporal_values(child_bundle["eclipse_data"]),
                "fullmoon_data": _serialize_temporal_values(child_bundle["fullmoon_data"]),
                "interpretation_context": _serialize_temporal_values(interpretation_context),
                "ai_interpretation": ai_interpretation,
                "parent_profile": {
                    "full_name": parent_full_name,
                    "birth_date": parent_birth_date,
                    "birth_time": parent_birth_time,
                    "birth_city": parent_birth_context["normalized_birth_place"],
                    "birth_country": parent_country,
                },
                "child_profile_meta": {
                    "full_name": child_full_name,
                    "birth_date": child_birth_date,
                    "birth_time": child_birth_time,
                    "birth_city": child_birth_context["normalized_birth_place"],
                    "birth_country": child_country,
                },
                "parent_natal_data": _serialize_temporal_values(parent_bundle["natal_data"]),
                "parent_dasha_data": _serialize_temporal_values(parent_bundle["dasha_data"]),
            }
            result_data["related_articles"] = []
            payload_json = {
                "language": current_language,
                "full_name": child_full_name,
                "birth_date": child_birth_date,
                "birth_time": child_birth_time,
                "birth_city": child_birth_context["normalized_birth_place"],
                "birth_country": child_country,
                "raw_birth_place_input": child_birth_context["raw_birth_place_input"],
                "normalized_birth_place": child_birth_context["normalized_birth_place"],
                "latitude": child_birth_context["latitude"],
                "longitude": child_birth_context["longitude"],
                "timezone": child_birth_context["timezone"],
                "geocode_provider": child_birth_context["geocode_provider"],
                "geocode_confidence": child_birth_context["geocode_confidence"],
                "location_source": child_birth_context.get("location_source"),
                "report_type": report_type,
                "natal_data": child_bundle["natal_data"],
                "dasha_data": child_bundle["dasha_data"],
                "navamsa_data": child_bundle["navamsa_data"],
                "transit_data": child_bundle["transit_data"],
                "eclipse_data": child_bundle["eclipse_data"],
                "fullmoon_data": child_bundle["fullmoon_data"],
                "interpretation_context": interpretation_context,
                "ai_interpretation": ai_interpretation,
                "parent_profile": {
                    "full_name": parent_full_name,
                    "birth_date": parent_birth_date,
                    "birth_time": parent_birth_time,
                    "birth_city": parent_birth_context["normalized_birth_place"],
                    "birth_country": parent_country,
                },
                "child_profile_meta": {
                    "full_name": child_full_name,
                    "birth_date": child_birth_date,
                    "birth_time": child_birth_time,
                    "birth_city": child_birth_context["normalized_birth_place"],
                    "birth_country": child_country,
                },
                "parent_natal_data": parent_bundle["natal_data"],
                "parent_dasha_data": parent_bundle["dasha_data"],
            }

            report_record = None
            if current_user:
                if can_save_more_reports(current_user, db):
                    profile = _upsert_user_profile(
                        db,
                        current_user,
                        {
                            "full_name": child_full_name,
                            "birth_date": child_birth_date,
                            "birth_time": child_birth_time,
                            "birth_city": child_birth_context["normalized_birth_place"],
                            "birth_country": child_country,
                            "raw_birth_place_input": child_birth_context["raw_birth_place_input"],
                            "normalized_birth_place": child_birth_context["normalized_birth_place"],
                            "lat": child_birth_context["latitude"],
                            "lon": child_birth_context["longitude"],
                            "timezone": child_birth_context["timezone"],
                            "geocode_provider": child_birth_context["geocode_provider"],
                            "geocode_confidence": child_birth_context["geocode_confidence"],
                            "natal_data": child_bundle["natal_data"],
                        },
                    )
                    report_record = _save_generated_report(
                        db,
                        current_user,
                        profile,
                        report_type,
                        payload_json,
                        interpretation_context,
                        child_bundle["calculation_metadata"],
                    )
                    db.commit()
                    db.refresh(report_record)
                    payload_json["generated_report_id"] = report_record.id
                else:
                    access_notice = (access_notice + " " if access_notice else "") + "Kayit limiti doldugu icin bu rapor hesaba eklenmedi."
                    result_data["access_notice"] = access_notice

            result_data["payload_json"] = _serialize_temporal_values(payload_json)
            result_data = _apply_report_access_context(result_data, report_record, current_user=current_user)
            return templates.TemplateResponse(request=request, name="result.html", context=result_data)
        date = f"{birth_date}T{birth_time}"
        resolved_location_payload = _resolved_birth_location_payload_from_form(
            birth_city=birth_city,
            country=country,
            resolved_birth_place=resolved_birth_place,
            resolved_latitude=resolved_latitude,
            resolved_longitude=resolved_longitude,
            resolved_timezone=resolved_timezone,
            geocode_provider=resolved_geocode_provider,
            geocode_confidence=resolved_geocode_confidence,
        )
        birth_context = _build_birth_context(date, birth_city, country, resolved_location_payload=resolved_location_payload)
        calculation_context = _make_calculation_context(birth_context)
        birth_dt = calculation_context.datetime_utc
        lat = birth_context["latitude"]
        lon = birth_context["longitude"]
        normalized_birth_place = birth_context["normalized_birth_place"]
        timezone_name = birth_context["timezone"]
        birthplace_accuracy_notice = None
        if birth_context.get("location_source") != "suggestion_selection":
            birthplace_accuracy_notice = "Dogum yeri suggestion listesinden secilmedigi icin sistem metni yeniden cozumledi. Daha hassas sonuc icin listeden secim yapmanizi oneririz."
        _log_birthplace_event(
            db,
            "resolved_birthplace_success",
            provider=birth_context.get("geocode_provider"),
            outcome="success",
            location_source=birth_context.get("location_source"),
            confidence=birth_context.get("geocode_confidence"),
        )
        if birth_context.get("geocode_confidence") is not None and float(birth_context.get("geocode_confidence")) < 0.65:
            _log_birthplace_event(
                db,
                "ambiguous_or_low_confidence_birthplace",
                provider=birth_context.get("geocode_provider"),
                outcome="low_confidence",
                location_source=birth_context.get("location_source"),
                confidence=birth_context.get("geocode_confidence"),
            )
        natal_data = engines_natal.calculate_natal_data(calculation_context)
        _log_chart_calculation_audit(location_payload=birth_context, birth_context=birth_context, natal_data=natal_data)

        moon_lon = next(p["abs_longitude"] for p in natal_data["planets"] if p["name"] == "Moon")
        dasha_data = engines_dasha.calculate_vims_dasha(calculation_context, moon_lon)
        navamsa_data = engines_navamsa.calculate_navamsa(natal_data)
        current_transits = engines_transits.get_current_transits(calculation_context)
        transit_data = engines_transits.score_current_impact(natal_data, current_transits)
        eclipse_data = engines_eclipses.calculate_upcoming_eclipses(calculation_context, natal_data=natal_data)
        fullmoon_data = []
        if engines_fullmoons:
            fullmoon_data = engines_fullmoons.calculate_upcoming_fullmoons(birth_dt, lat, lon, natal_data)

        phase28_events = _build_phase28_event_stream(transit_data, eclipse_data, fullmoon_data)
        psychological_themes = extract_psychological_themes(phase28_events)
        life_area_analysis = analyze_life_area_impact(psychological_themes)
        narrative_analysis = compress_ai_narratives(
            phase28_events,
            psychological_themes,
            life_area_analysis,
        )
        timing_intelligence = build_timing_intelligence(phase28_events, narrative_analysis)
        interpretation_context = _build_interpretation_context(
            phase28_events,
            psychological_themes,
            life_area_analysis,
            narrative_analysis,
            timing_intelligence,
        )
        user_feedback_history = load_feedback_history(db, user_id=current_user.id, limit=50) if current_user else []
        recommendation_feedback_summary = (
            compute_recommendation_feedback_summary(_load_recommendation_feedback_history(db, user_id=current_user.id, limit=100))
            if current_user
            else {}
        )
        interpretation_context["signal_layer"] = _build_interpretation_accuracy_context(
            natal_data,
            dasha_data,
            personalization={
                "user_feedback": user_feedback_history,
                "recommendation_feedback_summary": recommendation_feedback_summary,
            },
            transit_data=transit_data,
        )
        interpretation_context["recommendation_layer"] = interpretation_context["signal_layer"].get("recommendation_layer", {})
        interpretation_context = _localize_result_layer_text(interpretation_context, current_language)

        result_data = {
            "request": request,
            "language": current_language,
            "full_name": full_name,
            "birth_date": birth_date,
            "birth_time": birth_time,
            "birth_city": normalized_birth_place,
            "birth_country": country,
            "raw_birth_place_input": birth_context["raw_birth_place_input"],
            "normalized_birth_place": normalized_birth_place,
            "latitude": lat,
            "longitude": lon,
            "timezone": timezone_name,
            "geocode_provider": birth_context["geocode_provider"],
            "geocode_confidence": birth_context["geocode_confidence"],
            "location_source": birth_context.get("location_source"),
            "birthplace_accuracy_notice": birthplace_accuracy_notice,
            "calculation_config": _build_calculation_config_payload(calculation_context),
            "report_type": report_type,
            "report_type_config": report_type_config,
            "access_notice": access_notice,
            "natal_data": _serialize_temporal_values(natal_data),
            "dasha_data": _serialize_temporal_values(dasha_data),
            "navamsa_data": _serialize_temporal_values(navamsa_data),
            "transit_data": _serialize_temporal_values(transit_data),
            "eclipse_data": _serialize_temporal_values(eclipse_data),
            "fullmoon_data": _serialize_temporal_values(fullmoon_data),
            "interpretation_context": _serialize_temporal_values(interpretation_context),
        }
        result_data["related_articles"] = _match_related_articles_for_result(db, interpretation_context, language=current_language)
        payload_json = {
            "language": current_language,
            "full_name": full_name,
            "birth_date": birth_date,
            "birth_time": birth_time,
            "birth_city": normalized_birth_place,
            "birth_country": country,
            "raw_birth_place_input": birth_context["raw_birth_place_input"],
            "normalized_birth_place": normalized_birth_place,
            "latitude": lat,
            "longitude": lon,
            "timezone": timezone_name,
            "geocode_provider": birth_context["geocode_provider"],
            "geocode_confidence": birth_context["geocode_confidence"],
            "location_source": birth_context.get("location_source"),
            "birthplace_accuracy_notice": birthplace_accuracy_notice,
            "report_type": report_type,
            "natal_data": natal_data,
            "dasha_data": dasha_data,
            "navamsa_data": navamsa_data,
            "transit_data": transit_data,
            "eclipse_data": eclipse_data,
            "fullmoon_data": fullmoon_data,
            "interpretation_context": interpretation_context,
        }

        report_record = None
        if current_user:
            if can_save_more_reports(current_user, db):
                profile = _upsert_user_profile(
                    db,
                    current_user,
                    {
                        "full_name": full_name,
                        "birth_date": birth_date,
                        "birth_time": birth_time,
                        "birth_city": normalized_birth_place,
                        "birth_country": country,
                        "raw_birth_place_input": birth_context["raw_birth_place_input"],
                        "normalized_birth_place": normalized_birth_place,
                        "lat": lat,
                        "lon": lon,
                        "timezone": timezone_name,
                        "geocode_provider": birth_context["geocode_provider"],
                        "geocode_confidence": birth_context["geocode_confidence"],
                        "natal_data": natal_data,
                    },
                )
                calculation_metadata = build_calculation_metadata_snapshot(
                    calculation_context=calculation_context,
                    birth_context=birth_context,
                )
                report_record = _save_generated_report(
                    db,
                    current_user,
                    profile,
                    report_type,
                    payload_json,
                    interpretation_context,
                    calculation_metadata,
                )
                db.commit()
                db.refresh(report_record)
                payload_json["generated_report_id"] = report_record.id
            else:
                access_notice = (access_notice + " " if access_notice else "") + "Kayit limiti doldugu icin bu rapor hesaba eklenmedi."
                result_data["access_notice"] = access_notice

        result_data["payload_json"] = _serialize_temporal_values(payload_json)
        result_data = _apply_report_access_context(result_data, report_record, current_user=current_user)
        return templates.TemplateResponse(request=request, name="result.html", context=result_data)
    except BirthPlaceResolutionError as exc:
        logger.warning("Calculate birth place validation failed: %s", exc)
        try:
            _log_birthplace_event(
                db,
                "resolved_birthplace_failure",
                provider=resolved_geocode_provider or "nominatim",
                outcome=exc.code,
                location_source="manual_input",
            )
            if exc.code in {"ambiguous_place", "timezone_unresolved"}:
                _log_birthplace_event(
                    db,
                    "ambiguous_or_low_confidence_birthplace",
                    provider=resolved_geocode_provider or "nominatim",
                    outcome=exc.code,
                    location_source="manual_input",
                )
        except Exception:
            logger.exception("Failed to log birthplace resolution failure")
        _public_error("Dogum yeriniz fazla genel veya belirsiz. Lutfen ilce, sehir veya kasaba yazarak listeden dogru secimi yapin. Ornek: Besiktas, Istanbul, Turkey", 400)
    except ValueError as exc:
        logger.warning("Calculate geocoding validation failed: %s", exc)
        _public_error("Dogum yeri secimi tamamlanamadi. En dogru sonuc icin listeden bir dogum yeri secin.", 400)
    except Exception:
        logger.exception("Calculate flow failed")
        _public_error("Rapor hesaplanirken beklenmeyen bir hata olustu. Lutfen bilgileri kontrol edip tekrar deneyin.", 400)


@app.post("/interpret")
async def interpret_from_payload(request: Request, payload_json: str | None = Form(default=None), db: Session = Depends(get_db)):
    if not launch_ai_interpretation_enabled():
        return _coming_soon_json_response(_result_language(request, get_request_user(request, db)))
    enforce_rate_limit(request, "interpret", limit=8, window_seconds=600)
    await validate_csrf_token(request)
    try:
        if payload_json is not None:
            payload_data = json.loads(payload_json)
        else:
            payload_data = await request.json()
        payload_data = _attach_astro_signal_context(payload_data)
        current_user = get_request_user(request, db)
        report_id = payload_data.get("generated_report_id")
        if report_id:
            if not current_user:
                _public_error("Tam AI yorumu icin once hesabiniza giris yapin ve raporu acin.", 401)
            report = db.query(db_mod.GeneratedReport).filter(
                db_mod.GeneratedReport.id == report_id,
                db_mod.GeneratedReport.user_id == current_user.id,
            ).first()
            if not report:
                _public_error("Rapor bulunamadi.", 404)
            if not can_view_full_report(report):
                _public_error("Tam AI yorumu premium unlock sonrasinda acilir.", 403)
        yorum = ai_logic.generate_interpretation(payload_data)
        if report_id:
            if report:
                report.ai_interpretation_text = yorum
                report.result_payload_json = json.dumps(payload_data)
                db.commit()
        return {"yorum": yorum, "interpretation": yorum}
    except ai_logic.AIConfigurationError:
        logger.exception("AI configuration error")
        _public_error("AI yorum servisi simdilik hazir degil. Lutfen GEMINI_API_KEY ayarini kontrol edin.", 503)
    except ai_logic.AIServiceError:
        logger.exception("AI service error")
        _public_error("AI yorum servisi su anda yanit vermiyor. Lutfen birazdan tekrar deneyin.", 503)
    except Exception:
        logger.exception("Interpret endpoint failed")
        _public_error("Yorum uretilirken beklenmeyen bir hata olustu. Lutfen tekrar deneyin.", 400)


@app.post("/report/pdf-preview", response_class=HTMLResponse)
async def report_pdf_preview(request: Request, payload_json: str | None = Form(default=None), language: str | None = Form(default=None), csrf_token: str = Form(default=""), db: Session = Depends(get_db)):
    enforce_rate_limit(request, "report_pdf_preview", limit=12, window_seconds=600)
    await validate_csrf_token(request, csrf_token)
    try:
        if payload_json is not None:
            payload_data = json.loads(payload_json)
        else:
            payload_data = await request.json()
        if language:
            payload_data["language"] = language
        current_user = get_request_user(request, db)
        effective_report_type, access_notice = resolve_report_type_for_user(current_user, payload_data.get("report_type"))
        payload_data["report_type"] = effective_report_type
        if access_notice:
            payload_data["access_notice"] = access_notice
        report = _owned_report_from_payload(db, current_user, payload_data)
        report_context = _render_report_preview_context(request, payload_data, report=report, current_user=current_user)
        return templates.TemplateResponse(request=request, name="report_pdf.html", context=report_context)
    except Exception:
        logger.exception("Report preview failed")
        _public_error("Premium rapor onizlemesi olusturulamadi. Lutfen tekrar deneyin.", 400)


@app.post("/report/pdf")
async def report_pdf_export(request: Request, payload_json: str | None = Form(default=None), language: str | None = Form(default=None), csrf_token: str = Form(default=""), db: Session = Depends(get_db)):
    enforce_rate_limit(request, "report_pdf", limit=6, window_seconds=600)
    await validate_csrf_token(request, csrf_token)
    render_started_at = time.perf_counter()
    try:
        if payload_json is not None:
            payload_data = json.loads(payload_json)
        else:
            payload_data = await request.json()
        if language:
            payload_data["language"] = language
        current_user = get_request_user(request, db)
        effective_report_type, access_notice = resolve_report_type_for_user(current_user, payload_data.get("report_type"))
        payload_data["report_type"] = effective_report_type
        if access_notice:
            payload_data["access_notice"] = access_notice
        report = _owned_report_from_payload(db, current_user, payload_data)
        if not report:
            _public_error("Tam PDF icin once kayitli raporu acmaniz gerekiyor.", 403)
        report_context = _render_report_preview_context(request, payload_data, report=report, current_user=current_user)
        if not can_export_pdf(current_user, report_context.get("report_type")) or not can_download_pdf(report):
            logger.info("PDF export blocked for report_type=%s", report_context.get("report_type"))
            _public_error("Bu rapor turu icin PDF indirme aktif degil. Onizleme kullanabilirsiniz.", 403)
        client_name = str(report_context.get("client_name") or "user")
        safe_client_name = _sanitize_download_name(client_name, fallback="user")
        logger.info("PDF export started for client=%s language=%s", safe_client_name, payload_data.get("language", "tr"))
        pdf_bytes = _generate_pdf_bytes_from_report(report_context)
        mark_report_as_delivered(report)
        db.commit()
        filename = f"{safe_client_name}_{datetime.now().strftime('%Y%m%d')}_vedic_report.pdf"
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        elapsed_ms = round((time.perf_counter() - render_started_at) * 1000, 1)
        logger.info("PDF export succeeded for filename=%s bytes=%s duration_ms=%s", filename, len(pdf_bytes), elapsed_ms)
        return Response(content=pdf_bytes, media_type="application/pdf", headers=headers)
    except ai_logic.AIServiceError:
        elapsed_ms = round((time.perf_counter() - render_started_at) * 1000, 1)
        logger.exception("Report PDF export unavailable after %sms", elapsed_ms)
        _public_error("PDF export servisi bu ortamda henuz hazir degil. Simdilik PDF onizleme kullanabilirsiniz.", 503)
    except HTTPException:
        raise
    except Exception:
        elapsed_ms = round((time.perf_counter() - render_started_at) * 1000, 1)
        logger.exception("Report PDF export failed after %sms", elapsed_ms)
        _public_error("PDF raporu olusturulamadi. Lutfen tekrar deneyin.", 500)


@app.get("/api/v1/full-report")
async def get_comprehensive_report(
    date: str,
    city: str,
    name: str = "Olcan",
    db: Session = Depends(get_db),
):
    try:
        record = db.query(db_mod.UserRecord).filter(
            db_mod.UserRecord.birth_date == date,
            db_mod.UserRecord.city == city,
        ).first()

        if record:
            natal_data = json.loads(record.natal_data_json)
            birth_context = _build_birth_context_from_saved_fields(
                date,
                raw_birth_place_input=getattr(record, "raw_birth_place_input", None),
                normalized_birth_place=getattr(record, "normalized_birth_place", None),
                latitude=record.lat,
                longitude=record.lon,
                timezone=getattr(record, "timezone", None),
                geocode_provider=getattr(record, "geocode_provider", None),
                geocode_confidence=getattr(record, "geocode_confidence", None),
                fallback_place_text=record.city,
            )
            calculation_context = _make_calculation_context(birth_context)
            lat, lon = birth_context["latitude"], birth_context["longitude"]
            birth_dt = calculation_context.datetime_utc
            if not getattr(record, "timezone", None) or not getattr(record, "normalized_birth_place", None):
                record.raw_birth_place_input = birth_context["raw_birth_place_input"]
                record.normalized_birth_place = birth_context["normalized_birth_place"]
                record.lat = lat
                record.lon = lon
                record.timezone = birth_context["timezone"]
                record.geocode_provider = birth_context["geocode_provider"]
                record.geocode_confidence = birth_context["geocode_confidence"]
                db.commit()
        else:
            birth_context = _build_birth_context(date, city)
            calculation_context = _make_calculation_context(birth_context)
            birth_dt = calculation_context.datetime_utc
            lat, lon = birth_context["latitude"], birth_context["longitude"]
            natal_data = engines_natal.calculate_natal_data(calculation_context)
            _log_chart_calculation_audit(location_payload=birth_context, birth_context=birth_context, natal_data=natal_data)

            new_user = db_mod.UserRecord(
                name=name,
                birth_date=date,
                city=city,
                raw_birth_place_input=birth_context["raw_birth_place_input"],
                normalized_birth_place=birth_context["normalized_birth_place"],
                lat=lat,
                lon=lon,
                timezone=birth_context["timezone"],
                geocode_provider=birth_context["geocode_provider"],
                geocode_confidence=birth_context["geocode_confidence"],
                natal_data_json=json.dumps(natal_data),
            )
            db.add(new_user)
            db.commit()

        if record:
            calculation_context = _make_calculation_context(birth_context)
            _log_chart_calculation_audit(location_payload=birth_context, birth_context=birth_context, natal_data=natal_data)

        moon_lon = next(p["abs_longitude"] for p in natal_data["planets"] if p["name"] == "Moon")
        dasha_data = engines_dasha.calculate_vims_dasha(calculation_context, moon_lon)
        navamsa_data = engines_navamsa.calculate_navamsa(natal_data)

        current_transits = engines_transits.get_current_transits(calculation_context)
        transit_impact = engines_transits.score_current_impact(natal_data, current_transits)

        eclipse_data = engines_eclipses.calculate_upcoming_eclipses(calculation_context, natal_data=natal_data)

        fullmoon_data = []
        if engines_fullmoons:
            fullmoon_data = engines_fullmoons.calculate_upcoming_fullmoons(birth_dt, lat, lon, natal_data)

        phase28_events = _build_phase28_event_stream(transit_impact, eclipse_data, fullmoon_data)
        psychological_themes = extract_psychological_themes(phase28_events)
        life_area_analysis = analyze_life_area_impact(psychological_themes)
        narrative_analysis = compress_ai_narratives(
            phase28_events,
            psychological_themes,
            life_area_analysis,
        )
        timing_intelligence = build_timing_intelligence(phase28_events, narrative_analysis)
        interpretation_context = _build_interpretation_context(
            phase28_events,
            psychological_themes,
            life_area_analysis,
            narrative_analysis,
            timing_intelligence,
        )

        ai_payload = _attach_astro_signal_context(
            {
                "report_type": "birth_chart_karma",
                "natal_data": natal_data,
                "dasha_data": dasha_data,
                "navamsa_data": navamsa_data,
                "transit_data": transit_impact,
                "eclipse_data": eclipse_data,
                "fullmoon_data": fullmoon_data,
                "timing_data": timing_intelligence,
                "psychological_themes": psychological_themes,
                "life_area_analysis": life_area_analysis,
                "narrative_analysis": narrative_analysis,
                "interpretation_context": interpretation_context,
            },
            report_type="birth_chart_karma",
        )

        try:
            ai_insight = ai_logic.generate_interpretation(ai_payload)
        except ai_logic.AIConfigurationError:
            logger.exception("Full report AI configuration error")
            _public_error("AI yorum servisi simdilik hazir degil. Lutfen GEMINI_API_KEY ayarini kontrol edin.", 503)
        except ai_logic.AIServiceError:
            logger.exception("Full report AI service error")
            _public_error("AI yorum servisi su anda yanit vermiyor. Lutfen birazdan tekrar deneyin.", 503)

        return _serialize_temporal_values({
            "status": "success",
            "metadata": {
                "user": name,
                "calculated_at": datetime.now(pytz.UTC).isoformat(),
                "engine_v": "5.3-Timing",
                "birth_location": {
                    "raw_birth_place_input": birth_context["raw_birth_place_input"],
                    "normalized_birth_place": birth_context["normalized_birth_place"],
                    "latitude": birth_context["latitude"],
                    "longitude": birth_context["longitude"],
                    "timezone": birth_context["timezone"],
                    "geocode_provider": birth_context["geocode_provider"],
                    "geocode_confidence": birth_context["geocode_confidence"],
                },
                "calculation_config": _build_calculation_config_payload(calculation_context),
            },
            "ai_insight": ai_insight,
            "data_layers": {
                "natal": natal_data,
                "navamsa": navamsa_data,
                "dasha": dasha_data,
                "transits": transit_impact,
                "eclipses": eclipse_data,
                "fullmoons": fullmoon_data,
                "phase28_events": phase28_events,
                "psychological_themes": psychological_themes,
                "life_area_analysis": life_area_analysis,
                "narrative_analysis": narrative_analysis,
                "timing_intelligence": timing_intelligence,
                "interpretation_context": interpretation_context,
            },
        })

    except HTTPException:
        db.rollback()
        raise
    except BirthPlaceResolutionError as exc:
        db.rollback()
        logger.warning("Full report birth place validation failed: %s", exc)
        _public_error("Dogum yeri net olarak cozumlenemedi. Lutfen ilce, sehir ve ulke bilgisini daha acik girin.", 400)
    except Exception:
        db.rollback()
        logger.exception("Full report generation failed")
        _public_error("Rapor hazirlanirken beklenmeyen bir hata olustu.", 400)


# Production run:
# gunicorn app:app -k uvicorn.workers.UvicornWorker
#
# Local development:
# uvicorn app:app --host 0.0.0.0 --port 8000 --reload
if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, reload=True)
