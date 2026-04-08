import logging
import os
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, TemplateNotFound, select_autoescape

logger = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parent
EMAIL_TEMPLATES_DIR = BASE_DIR / "templates" / "emails"

email_env = Environment(
    loader=FileSystemLoader(str(EMAIL_TEMPLATES_DIR)),
    autoescape=select_autoescape(["html", "xml"]),
)


@dataclass
class EmailSendResult:
    ok: bool
    status: str
    subject: str
    provider_message_id: str | None = None
    error_message: str | None = None


def _env_flag(name, default=False):
    value = str(os.getenv(name, str(default))).strip().lower()
    return value in {"1", "true", "yes", "on"}


def get_email_config():
    return {
        "backend": os.getenv("MAIL_BACKEND", "smtp").strip().lower(),
        "host": os.getenv("MAIL_HOST", "").strip(),
        "port": int(os.getenv("MAIL_PORT", "587") or "587"),
        "username": os.getenv("MAIL_USERNAME", "").strip(),
        "password": os.getenv("MAIL_PASSWORD", "").strip(),
        "use_tls": _env_flag("MAIL_USE_TLS", True),
        "use_ssl": _env_flag("MAIL_USE_SSL", False),
        "from_address": os.getenv("MAIL_FROM_ADDRESS", "").strip(),
        "from_name": os.getenv("MAIL_FROM_NAME", "Jyotish").strip(),
        "app_base_url": os.getenv("APP_BASE_URL", "http://127.0.0.1:8000").strip(),
        "support_email": os.getenv("SUPPORT_EMAIL", "").strip(),
        "billing_email": os.getenv("BILLING_EMAIL", "").strip(),
    }


def is_email_configured():
    config = get_email_config()
    required = [config["host"], config["from_address"]]
    if config["username"]:
        required.append(config["password"])
    return all(bool(item) for item in required)


def render_email_template(template_name, **context):
    try:
        html_template = email_env.get_template(f"{template_name}.html")
        text_template = email_env.get_template(f"{template_name}.txt")
    except TemplateNotFound as exc:
        raise RuntimeError(f"Email template missing: {template_name}") from exc
    return html_template.render(**context), text_template.render(**context)


def send_transactional_email(to_email, subject, html_body, text_body=None):
    if not is_email_configured():
        logger.warning("Transactional email skipped because mail configuration is missing")
        return EmailSendResult(ok=False, status="skipped", subject=subject, error_message="mail_not_configured")

    config = get_email_config()
    message = EmailMessage()
    from_header = f"{config['from_name']} <{config['from_address']}>" if config["from_name"] else config["from_address"]
    message["Subject"] = subject
    message["From"] = from_header
    message["To"] = to_email
    message.set_content(text_body or "Please view this message in an HTML-capable email client.")
    message.add_alternative(html_body, subtype="html")

    try:
        if config["use_ssl"]:
            server = smtplib.SMTP_SSL(config["host"], config["port"], timeout=20)
        else:
            server = smtplib.SMTP(config["host"], config["port"], timeout=20)
        with server:
            if config["use_tls"] and not config["use_ssl"]:
                server.starttls()
            if config["username"]:
                server.login(config["username"], config["password"])
            server.send_message(message)
        logger.info("Transactional email sent to=%s subject=%s", to_email, subject)
        return EmailSendResult(ok=True, status="sent", subject=subject)
    except Exception as exc:
        logger.exception("Transactional email failed to=%s subject=%s", to_email, subject)
        return EmailSendResult(ok=False, status="failed", subject=subject, error_message=str(exc))


def send_template_email(to_email, template_name, subject, **context):
    html_body, text_body = render_email_template(template_name, **context)
    return send_transactional_email(to_email, subject, html_body, text_body=text_body)
