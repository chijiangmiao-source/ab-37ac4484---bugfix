"""ASGI entrypoint used by uvicorn (kept side-effect-free out of app.main)."""

from .main import create_app

app = create_app()
