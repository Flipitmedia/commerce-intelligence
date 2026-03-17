"""
main.py — FastAPI app, startup, mounts
"""
import threading
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


def _run_auto_sync():
    """Sync automatico de todas las tiendas (periodo activo)."""
    from app.database import query
    stores = query("SELECT * FROM stores")
    for store in stores:
        store = dict(store)
        # Shopify
        if store.get("shopify_domain") and store.get("shopify_client_id"):
            try:
                from app.sync_shopify import sync_shopify_periodo
                result = sync_shopify_periodo(store)
                print(f"  Auto-sync Shopify [{store['id']}]: {result.get('orders', 0)} orders")
            except Exception as e:
                print(f"  Auto-sync Shopify [{store['id']}] ERROR: {e}")
        # Meta
        if store.get("meta_access_token") and store.get("meta_ad_account_id"):
            try:
                from app.sync_meta import sync_meta_periodo
                result = sync_meta_periodo(store)
                print(f"  Auto-sync Meta [{store['id']}]: {result.get('rows', 0)} rows")
            except Exception as e:
                print(f"  Auto-sync Meta [{store['id']}] ERROR: {e}")


def _schedule_auto_sync():
    """Programa sync automatico a las 9:00 y 21:00 hora Chile."""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    import time

    tz = ZoneInfo("America/Santiago")
    target_hours = {9, 21}

    while True:
        now = datetime.now(tz)
        # Buscar la proxima hora target
        next_run = None
        for h in sorted(target_hours):
            candidate = now.replace(hour=h, minute=0, second=0, microsecond=0)
            if candidate > now:
                next_run = candidate
                break
        if next_run is None:
            # Proximo dia, primera hora target
            from datetime import timedelta
            tomorrow = now + timedelta(days=1)
            next_run = tomorrow.replace(hour=min(target_hours), minute=0, second=0, microsecond=0)

        wait_seconds = (next_run - now).total_seconds()
        print(f"Auto-sync programado para {next_run.strftime('%Y-%m-%d %H:%M')} Chile ({int(wait_seconds)}s)")
        time.sleep(wait_seconds)

        print(f"Auto-sync iniciando ({datetime.now(tz).strftime('%H:%M')} Chile)...")
        try:
            _run_auto_sync()
            print("Auto-sync completado.")
        except Exception as e:
            print(f"Auto-sync error global: {e}")


@app.on_event("startup")
def startup():
    init_db()
    # Iniciar scheduler de auto-sync en background thread
    t = threading.Thread(target=_schedule_auto_sync, daemon=True)
    t.start()


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
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/admin/", status_code=302)
