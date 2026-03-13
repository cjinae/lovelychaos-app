from collections.abc import Generator
import os
from pathlib import Path
import tempfile

from alembic import command
from alembic.config import Config
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.db import get_db
from app.main import app, seed_data
from app.services.calendar import MockCalendarProvider
from app.services.llm import MockDecisionEngine
from app.services.notifications import MockNotificationProvider

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def session_factory() -> Generator[sessionmaker, None, None]:
    fd, path = tempfile.mkstemp(prefix="lovelychaos-test-", suffix=".db")
    os.close(fd)
    engine = create_engine(
        f"sqlite+pysqlite:///{path}",
        future=True,
        connect_args={"check_same_thread": False},
    )
    TestingSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    alembic_cfg = Config(str(REPO_ROOT / "alembic.ini"))
    alembic_cfg.set_main_option("script_location", str(REPO_ROOT / "alembic"))
    alembic_cfg.set_main_option("sqlalchemy.url", f"sqlite:///{path}")
    command.upgrade(alembic_cfg, "head")
    with TestingSessionLocal() as db:
        seed_data(db)
        db.commit()
    try:
        yield TestingSessionLocal
    finally:
        engine.dispose()
        os.remove(path)


@pytest.fixture
def db_session(session_factory: sessionmaker) -> Generator[Session, None, None]:
    with session_factory() as db:
        yield db


@pytest.fixture
def client(session_factory: sessionmaker) -> Generator[TestClient, None, None]:
    def override_get_db():
        with session_factory() as db:
            yield db

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


@pytest.fixture(autouse=True)
def force_mock_calendar_provider(monkeypatch):
    import app.main as main_module

    monkeypatch.setattr(main_module, "calendar_provider", MockCalendarProvider())


@pytest.fixture(autouse=True)
def force_mock_notification_provider(monkeypatch):
    import app.main as main_module

    monkeypatch.setattr(main_module, "notification_provider", MockNotificationProvider())


@pytest.fixture(autouse=True)
def force_mock_llm_engine(monkeypatch):
    import app.main as main_module

    monkeypatch.setattr(main_module, "engine_llm", MockDecisionEngine())
