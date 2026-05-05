from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

import app
import database as db_mod
from services import knowledge_service


@pytest.fixture()
def review_env():
    db_mod.init_db()
    db = db_mod.SessionLocal()
    db.query(db_mod.KnowledgeChunk).delete()
    db.query(db_mod.KnowledgeItem).delete()
    db.query(db_mod.SourceDocument).delete()
    db.query(db_mod.ServiceOrder).delete()
    db.query(db_mod.AppUser).delete()
    admin = db_mod.AppUser(
        email="knowledge-sort-admin@example.com",
        password_hash="hash",
        name="Knowledge Sort Admin",
        is_admin=True,
        is_active=True,
        plan_code="elite",
    )
    db.add(admin)
    db.commit()
    client = TestClient(app.app)

    def request_admin_pair(request, session):
        user = session.query(db_mod.AppUser).filter(db_mod.AppUser.email == "knowledge-sort-admin@example.com").first()
        return user, None

    source_document = db_mod.SourceDocument(title="Nakshatra Kitabı", document_type="book")
    db.add(source_document)
    db.flush()
    knowledge_service.create_knowledge_item(
        db,
        title="Dhanishta Review Chunk",
        body_text="Dhanishta can show discipline and rhythmic timing.",
        language="tr",
        item_type="nakshatra",
        summary_text="A calm review chunk.",
        entities=["dhanishta", "saturn"],
        source_document=source_document,
        metadata={
            "review_required": True,
            "status": "review_required",
            "category": "nakshatra",
            "primary_entity": "dhanishta",
            "source_title": "Nakshatra Kitabı",
            "confidence_level": "medium",
            "sensitivity_level": "high",
            "coverage_entities": ["dhanishta", "saturn"],
        },
        created_by_user_id=admin.id,
        status="review_required",
    )
    db.commit()

    try:
        yield db, client, request_admin_pair
    finally:
        db.rollback()
        db.query(db_mod.KnowledgeChunk).delete()
        db.query(db_mod.KnowledgeItem).delete()
        db.query(db_mod.SourceDocument).delete()
        db.query(db_mod.ServiceOrder).delete()
        db.query(db_mod.AppUser).delete()
        db.commit()
        db.close()


def test_review_sort_by_sensitivity_returns_200(review_env):
    _db, client, request_admin_pair = review_env
    with patch.object(app, "_require_admin_user", side_effect=request_admin_pair):
        response = client.get("/admin/knowledge/review?sort=sensitivity&direction=asc")
    assert response.status_code == 200


def test_review_sort_by_created_at_returns_200(review_env):
    _db, client, request_admin_pair = review_env
    with patch.object(app, "_require_admin_user", side_effect=request_admin_pair):
        response = client.get("/admin/knowledge/review?sort=created_at&order=desc")
    assert response.status_code == 200


def test_review_sort_invalid_field_falls_back_without_crash(review_env):
    _db, client, request_admin_pair = review_env
    with patch.object(app, "_require_admin_user", side_effect=request_admin_pair):
        response = client.get("/admin/knowledge/review?sort=invalid_field")
    assert response.status_code == 200


def test_review_empty_source_document_id_returns_200(review_env):
    _db, client, request_admin_pair = review_env
    with patch.object(app, "_require_admin_user", side_effect=request_admin_pair):
        response = client.get("/admin/knowledge/review?source_document_id=")
    assert response.status_code == 200


def test_review_invalid_source_document_id_returns_200(review_env):
    _db, client, request_admin_pair = review_env
    with patch.object(app, "_require_admin_user", side_effect=request_admin_pair):
        response = client.get("/admin/knowledge/review?source_document_id=abc")
    assert response.status_code == 200


def test_review_numeric_source_document_id_filters(review_env):
    db, client, request_admin_pair = review_env
    admin = db.query(db_mod.AppUser).filter(db_mod.AppUser.email == "knowledge-sort-admin@example.com").first()
    other_source = db_mod.SourceDocument(title="Other Source", document_type="book")
    db.add(other_source)
    db.flush()
    knowledge_service.create_knowledge_item(
        db,
        title="Other Source Review Chunk",
        body_text="Other source body text.",
        language="tr",
        item_type="nakshatra",
        summary_text="Other summary.",
        entities=["ashwini"],
        source_document=other_source,
        metadata={
            "review_required": True,
            "status": "review_required",
            "category": "nakshatra",
            "primary_entity": "ashwini",
            "source_title": "Other Source",
            "confidence_level": "medium",
            "sensitivity_level": "low",
        },
        created_by_user_id=admin.id,
        status="review_required",
    )
    db.commit()
    target = db.query(db_mod.KnowledgeItem).filter(db_mod.KnowledgeItem.title == "Dhanishta Review Chunk").first()
    with patch.object(app, "_require_admin_user", side_effect=request_admin_pair):
        response = client.get(f"/admin/knowledge/review?source_document_id={target.source_document_id}")
    assert response.status_code == 200
    assert "Dhanishta Review Chunk" in response.text
    assert "Other Source Review Chunk" not in response.text


def test_review_empty_filter_params_return_200(review_env):
    _db, client, request_admin_pair = review_env
    with patch.object(app, "_require_admin_user", side_effect=request_admin_pair):
        response = client.get("/admin/knowledge/review?status=&sensitivity=&category=&q=&sort=&direction=")
    assert response.status_code == 200


def test_sensitivity_sort_key_high_is_greater_than_medium():
    assert app._sensitivity_sort_key("high") > app._sensitivity_sort_key("medium")


def test_sensitivity_sort_key_medium_is_greater_than_low():
    assert app._sensitivity_sort_key("medium") > app._sensitivity_sort_key("low")


def test_sensitivity_sort_key_unknown_is_negative_one():
    assert app._sensitivity_sort_key("-") == -1


def test_confidence_sort_key_ordering():
    assert app._confidence_sort_key("high") > app._confidence_sort_key("low")


def test_clean_extracted_title_removes_unknown_suffix_with_context():
    assert app._clean_extracted_title("Dhanishta - Unknown", "dhanishta", "Nakshatra Kitabı") == "Dhanishta — Nakshatra Kitabı"


def test_clean_extracted_title_adds_subtopic_and_page_context():
    assert app._clean_extracted_title(
        "Dhanishta - Unknown",
        "dhanishta",
        "Nakshatra Kitabı",
        42,
        metadata={"category": "nakshatra"},
    ) == "Dhanishta — Nakshatra Knowledge — p.42"


def test_clean_extracted_title_uses_source_and_page_for_unknown():
    assert app._clean_extracted_title("Unknown", "", "Kitap", 23) == "Kitap — p.23"


def test_clean_extracted_title_keeps_meaningful_title():
    assert app._clean_extracted_title("Bharani — Kimlik") == "Bharani — Kimlik"


def test_clean_extracted_title_empty_returns_section():
    assert app._clean_extracted_title("") == "Bölüm"


def test_review_routes_do_not_create_service_orders(review_env):
    db, client, request_admin_pair = review_env
    item_id = db.query(db_mod.KnowledgeItem.id).first()[0]
    before = db.query(db_mod.ServiceOrder).count()
    with patch.object(app, "_require_admin_user", side_effect=request_admin_pair):
        client.get("/admin/knowledge/review")
        client.get("/admin/knowledge/review?sort=sensitivity_level&order=asc")
        client.get("/admin/knowledge/review?sort=invalid_field")
        client.get(f"/admin/knowledge/review/{item_id}")
    db.expire_all()
    assert db.query(db_mod.ServiceOrder).count() == before
