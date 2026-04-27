import pytest


@pytest.fixture(autouse=True)
def _default_launch_flags_off(monkeypatch):
    monkeypatch.setenv("LAUNCH_MODE", "false")
    monkeypatch.setenv("ENABLE_PAYMENTS", "true")
    monkeypatch.setenv("ENABLE_FREE_CALCULATOR", "true")
    monkeypatch.setenv("ENABLE_AI_INTERPRETATION", "true")
    monkeypatch.setenv("ENABLE_CONSULTATION_BOOKING", "true")
