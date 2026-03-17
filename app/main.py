"""
main.py — FastAPI app, startup, mounts
"""
from pathlib import Path
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from app.config import settings, ROOT_DIR
from app.models import init_db

app = FastAPI(
    title="Commerce Intelligence",
    version="1.0.0",
    docs_url="/docs" if settings.DEBUG else None,
)


@app.on_event("startup")
def startup():
    init_db()


# ── Static: dashboard ──────────────────────────────────────────
dashboard_dir = ROOT_DIR / "dashboard"
if dashboard_dir.exists():
    app.mount("/dashboard", StaticFiles(directory=str(dashboard_dir), html=True), name="dashboard")


# ── Routes ─────────────────────────────────────────────────────
from app.routes.api import router as api_router      # noqa: E402
from app.routes.admin import router as admin_router   # noqa: E402

app.include_router(api_router)
app.include_router(admin_router)


@app.get("/")
def root():
    from app.database import query
    stores = query("SELECT id, name, api_token FROM stores")
    store_links = {}
    for s in stores:
        store_links[s["id"]] = {
            "name": s["name"],
            "token": s["api_token"],
            "admin": f"/admin/{s['id']}?token={s['api_token']}",
            "dashboard": f"/dashboard/?store={s['id']}&token={s['api_token']}",
            "api": f"/api/{s['id']}/data?token={s['api_token']}",
        }
    return {
        "app": "Commerce Intelligence",
        "version": "1.0.0",
        "stores": store_links,
    }
