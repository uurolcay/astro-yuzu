import logging
import os
import smtplib
import ssl
from dataclasses import dataclass
from email import encoders
from email.message import EmailMessage
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from jinja2 import ChoiceLoader, Environment, FileSystemLoader, TemplateNotFound, select_autoescape

logger = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parent
EMAIL_TEMPLATES_DIR = BASE_DIR / "templates" / "emails"

email_env = Environment(
    loader=ChoiceLoader([
        FileSystemLoader(str(BASE_DIR / "templates")),
        FileSystemLoader(str(EMAIL_TEMPLATES_DIR)),
    ]),
    autoescape=select_autoescape(["html", "xml"]),
)

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587") or "587")
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
FROM_NAME = os.getenv("FROM_NAME", "Focus Astrology")
FROM_EMAIL = os.getenv("FROM_EMAIL", SMTP_USER)


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


def render_email(template_name: str, **kwargs) -> str:
    return email_env.get_template(template_name).render(**kwargs)


def send_email(
    to_email: str,
    subject: str,
    html_body: str,
    plain_body: str = "",
    attachment_path: str | None = None,
    attachment_filename: str | None = None,
) -> bool:
    if not SMTP_USER or not SMTP_PASS:
        print(f"[EMAIL STUB] To: {to_email} | Subject: {subject}")
        return True
    try:
        msg = MIMEMultipart("mixed")
        msg["Subject"] = subject
        msg["From"] = f"{FROM_NAME} <{FROM_EMAIL}>"
        msg["To"] = to_email

        alt = MIMEMultipart("alternative")
        alt.attach(MIMEText(plain_body or "", "plain", "utf-8"))
        alt.attach(MIMEText(html_body, "html", "utf-8"))
        msg.attach(alt)

        if attachment_path and os.path.exists(attachment_path):
            with open(attachment_path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            fname = attachment_filename or os.path.basename(attachment_path)
            part.add_header("Content-Disposition", f'attachment; filename="{fname}"')
            msg.attach(part)

        ctx = ssl.create_default_context()
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls(context=ctx)
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(FROM_EMAIL, to_email, msg.as_string())
        return True
    except Exception as exc:
        print(f"[EMAIL ERROR] {exc}")
        return False


def _attach_files(message, attachments=None):
    for item in attachments or []:
        path = Path(item.get("path") if isinstance(item, dict) else item)
        filename = (item.get("filename") if isinstance(item, dict) else None) or path.name
        if not path.exists() or not path.is_file():
            raise FileNotFoundError(f"Email attachment not found: {path}")
        data = path.read_bytes()
        if not data:
            raise ValueError(f"Email attachment is empty: {path}")
        message.add_attachment(
            data,
            maintype=(item.get("maintype") if isinstance(item, dict) else None) or "application",
            subtype=(item.get("subtype") if isinstance(item, dict) else None) or "pdf",
            filename=filename,
        )


def send_transactional_email(to_email, subject, html_body, text_body=None, attachments=None):
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
        _attach_files(message, attachments=attachments)
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


def send_template_email(to_email, template_name, subject, attachments=None, **context):
    html_body, text_body = render_email_template(template_name, **context)
    return send_transactional_email(to_email, subject, html_body, text_body=text_body, attachments=attachments)
