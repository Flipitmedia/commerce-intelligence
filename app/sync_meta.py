"""
sync_meta.py — Cliente Meta Marketing API

Portado desde SyncMeta.gs
"""
import time
import httpx
from app.database import get_db
from app.sync_shopify import generate_period_range

META_API_VERSION = "v21.0"


# ── Helpers ──────────────────────────────────────────────────────

def meta_fetch(url: str) -> dict:
    """GET generico a la Meta Graph API."""
    with httpx.Client(timeout=30) as client:
        resp = client.get(url)

    if resp.status_code != 200:
        error_msg = f"Meta API error {resp.status_code}"
        try:
            err_json = resp.json()
            if "error" in err_json:
                error_msg += f": {err_json['error'].get('message', '')}"
                code = err_json["error"].get("code")
                if code == 190:
                    error_msg += "\n-> TOKEN INVALIDO o EXPIRADO."
                elif code == 200:
                    error_msg += "\n-> ACCESO BLOQUEADO. Token sin permisos suficientes."
                elif code == 100:
                    error_msg += "\n-> PARAMETRO INVALIDO. Verifica Ad Account ID."
        except Exception:
            error_msg += f": {resp.text[:300]}"
        raise RuntimeError(error_msg)

    return resp.json()


def meta_fetch_insights(access_token: str, ad_account_id: str, params: dict) -> list[dict]:
    """
    GET insights de Meta Marketing API con paginacion automatica.
    """
    all_data = []
    base_url = f"https://graph.facebook.com/{META_API_VERSION}/{ad_account_id}/insights"
    params["access_token"] = access_token

    # Construir URL
    from urllib.parse import urlencode
    url = f"{base_url}?{urlencode(params)}"

    while url:
        data = meta_fetch(url)
        items = data.get("data", [])
        all_data.extend(items)

        # Paginacion cursor-based
        url = None
        paging = data.get("paging", {})
        if paging.get("next"):
            url = paging["next"]

        if url:
            time.sleep(0.5)

    return all_data


def extract_action(actions: list | None, action_type: str) -> float:
    """Extrae un valor del array 'actions' de Meta."""
    if not actions:
        return 0.0
    for a in actions:
        if a.get("action_type") == action_type:
            return float(a.get("value", 0))
    return 0.0


def extract_action_value(action_values: list | None, action_type: str) -> float:
    """Extrae un valor del array 'action_values' de Meta."""
    if not action_values:
        return 0.0
    for a in action_values:
        if a.get("action_type") == action_type:
            return float(a.get("value", 0))
    return 0.0


# ── Sync functions ──────────────────────────────────────────────

def extract_insight_data(row: dict) -> dict:
    """Extrae datos de una fila de insights de Meta."""
    import json as _json

    actions = row.get("actions") or []
    action_values = row.get("action_values") or []

    # Meta usa diferentes action_types segun pixel config
    purchases = (
        extract_action(actions, "offsite_conversion.fb_pixel_purchase")
        or extract_action(actions, "purchase")
    )
    purchase_value = (
        extract_action_value(action_values, "offsite_conversion.fb_pixel_purchase")
        or extract_action_value(action_values, "purchase")
    )
    add_to_cart = (
        extract_action(actions, "offsite_conversion.fb_pixel_add_to_cart")
        or extract_action(actions, "add_to_cart")
    )
    initiate_checkout = (
        extract_action(actions, "offsite_conversion.fb_pixel_initiate_checkout")
        or extract_action(actions, "initiate_checkout")
    )

    date_str = row.get("date_start", "")
    periodo = date_str[:7] if date_str else ""

    return {
        "date": date_str,
        "periodo": periodo,
        "campaign_id": row.get("campaign_id", ""),
        "campaign_name": row.get("campaign_name", ""),
        "objective": row.get("objective", ""),
        "spend": float(row.get("spend", 0)),
        "impressions": int(row.get("impressions", 0)),
        "clicks": int(row.get("clicks", 0)),
        "ctr": float(row.get("ctr", 0)),
        "cpm": float(row.get("cpm", 0)),
        "cpc": float(row.get("cpc", 0)),
        "purchases": purchases,
        "purchase_value": purchase_value,
        "add_to_cart": add_to_cart,
        "initiate_checkout": initiate_checkout,
    }


def sync_meta_periodo(store: dict, periodo: str | None = None) -> dict:
    """
    Sincroniza datos de Meta Ads para un periodo.
    Retorna {rows: int}.
    """
    import json as _json

    periodo = periodo or store["periodo_activo"]
    access_token = store["meta_access_token"]
    ad_account_id = store["meta_ad_account_id"]

    if not access_token or not ad_account_id:
        raise ValueError(
            f"Store '{store['id']}': faltan credenciales Meta. "
            "Configura meta_access_token y meta_ad_account_id."
        )

    # Asegurar prefijo act_
    if not ad_account_id.startswith("act_"):
        ad_account_id = f"act_{ad_account_id}"

    # Meta usa fechas de reporte fijas (no timestamps con timezone),
    # NO extender ±1 dia como Shopify. Usar rango exacto del mes.
    import calendar as _cal
    parts = periodo.split("-")
    year, month = int(parts[0]), int(parts[1])
    last_day = _cal.monthrange(year, month)[1]
    since_date = f"{year}-{month:02d}-01"
    until_date = f"{year}-{month:02d}-{last_day:02d}"

    print(f"  Sync Meta [{store['id']}]: periodo={periodo}")

    insights = meta_fetch_insights(access_token, ad_account_id, {
        "fields": "campaign_id,campaign_name,objective,spend,impressions,clicks,ctr,cpm,cpc,actions,action_values",
        "time_range": _json.dumps({"since": since_date, "until": until_date}),
        "level": "campaign",
        "time_increment": "1",
        "limit": "500",
    })

    print(f"  Meta: {len(insights)} filas de insights")

    rows = []
    for row in insights:
        data = extract_insight_data(row)
        data["store_id"] = store["id"]
        rows.append(data)

    # Escribir en DB (reemplazo atomico por periodo)
    with get_db() as conn:
        conn.execute(
            "DELETE FROM meta_insights WHERE store_id = ? AND periodo = ?",
            (store["id"], periodo),
        )
        for row in rows:
            cols = ", ".join(row.keys())
            placeholders = ", ".join(["?"] * len(row))
            conn.execute(
                f"INSERT OR REPLACE INTO meta_insights ({cols}) VALUES ({placeholders})",
                tuple(row.values()),
            )

    print(f"  DB: {len(rows)} meta insights")
    return {"rows": len(rows)}


def sync_meta_full(store: dict) -> dict:
    """Sincroniza ultimos 12 meses de Meta Ads."""
    from datetime import date, timedelta

    today = date.today()
    start = date(today.year, today.month, 1)
    for _ in range(11):
        start = (start - timedelta(days=1)).replace(day=1)
    start_period = f"{start.year}-{start.month:02d}"
    end_period = store["periodo_activo"]

    periodos = generate_period_range(start_period, end_period)
    total_rows = 0

    for periodo in periodos:
        result = sync_meta_periodo(store, periodo)
        total_rows += result["rows"]
        time.sleep(1)

    print(f"  Meta full sync: {total_rows} filas ({len(periodos)} meses)")
    return {"rows": total_rows, "months": len(periodos)}
