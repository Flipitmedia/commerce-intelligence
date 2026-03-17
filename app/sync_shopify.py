"""
sync_shopify.py — Cliente Shopify API (OAuth + fetch orders)

Portado desde SyncShopify.gs + ShopifyAuth.gs
Soporta:
  - Token directo (shpat_)
  - client_credentials grant
"""
import time
import calendar
import httpx
from app.database import get_db

SHOPIFY_API_VERSION = "2026-01"
SHOPIFY_SCOPES = "read_orders,read_products,read_all_orders"


# ── Auth ──────────────────────────────────────────────────────────

def get_shopify_token(store: dict) -> str:
    """
    Obtiene un access token de Shopify.
    - Si client_id empieza con shpat_, es un token directo.
    - Si no, usa client_credentials grant.
    """
    client_id = store["shopify_client_id"]
    client_secret = store["shopify_client_secret"]
    domain = store["shopify_domain"]

    if not domain or not client_id:
        raise ValueError(
            f"Store '{store['id']}': faltan credenciales Shopify. "
            "Configura shopify_domain y shopify_client_id."
        )

    # Metodo A: token directo
    if client_id.startswith("shpat_"):
        return client_id

    # Metodo B: client_credentials grant
    if not client_secret:
        raise ValueError(
            f"Store '{store['id']}': falta shopify_client_secret para client_credentials."
        )

    url = f"https://{domain}/admin/oauth/access_token"
    with httpx.Client(timeout=30) as client:
        resp = client.post(
            url,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
        )

    if resp.status_code != 200:
        raise RuntimeError(
            f"Error obteniendo access token de Shopify (HTTP {resp.status_code}): "
            f"{resp.text[:300]}"
        )

    data = resp.json()
    if "access_token" not in data:
        raise RuntimeError(f"Respuesta sin access_token: {resp.text[:300]}")

    return data["access_token"]


# ── Helpers ──────────────────────────────────────────────────────

def period_to_date_range(periodo: str) -> tuple[str, str]:
    """
    YYYY-MM -> (since, until) en formato ISO para Shopify API.
    Extiende 1 dia antes y despues para cubrir timezones no-UTC
    (ej: Santiago UTC-3/4). extract_order_data() luego filtra por periodo real.
    """
    from datetime import date, timedelta
    parts = periodo.split("-")
    year = int(parts[0])
    month = int(parts[1])
    last_day = calendar.monthrange(year, month)[1]
    start = date(year, month, 1) - timedelta(days=1)
    end = date(year, month, last_day) + timedelta(days=1)
    since = f"{start.isoformat()}T00:00:00-00:00"
    until = f"{end.isoformat()}T23:59:59-00:00"
    return since, until


def generate_period_range(start: str, end: str) -> list[str]:
    """Genera lista de periodos YYYY-MM desde start hasta end (inclusive)."""
    periods = []
    s = start.split("-")
    year, month = int(s[0]), int(s[1])
    e = end.split("-")
    end_year, end_month = int(e[0]), int(e[1])

    while year < end_year or (year == end_year and month <= end_month):
        periods.append(f"{year}-{month:02d}")
        month += 1
        if month > 12:
            month = 1
            year += 1
    return periods


def shopify_fetch_all(domain: str, token: str, endpoint: str, params: dict) -> list[dict]:
    """
    GET a Shopify Admin API con paginacion automatica via Link header.
    Retorna todos los objetos del recurso.
    """
    all_items = []
    base_url = f"https://{domain}/admin/api/{SHOPIFY_API_VERSION}/{endpoint}"

    with httpx.Client(timeout=60) as client:
        url = base_url
        first = True

        while url:
            if first:
                resp = client.get(url, params=params, headers={"X-Shopify-Access-Token": token})
                first = False
            else:
                resp = client.get(url, headers={"X-Shopify-Access-Token": token})

            if resp.status_code != 200:
                raise RuntimeError(
                    f"Shopify API error {resp.status_code}: {resp.text[:500]}"
                )

            data = resp.json()
            resource_key = list(data.keys())[0]
            items = data.get(resource_key, [])
            all_items.extend(items)

            # Paginacion via Link header
            url = None
            link_header = resp.headers.get("link", "")
            if link_header:
                import re
                match = re.search(r'<([^>]+)>;\s*rel="next"', link_header)
                if match:
                    url = match.group(1)

            # Rate limiting
            if url:
                time.sleep(0.5)

    return all_items


# ── Sync functions ──────────────────────────────────────────────

def extract_order_data(order: dict, store_id: str, store_timezone: str = "America/Santiago") -> tuple[dict, list[dict], list[dict]]:
    """
    Extrae datos de un order de Shopify.
    Retorna (order_row, [line_rows], [refund_rows]).
    v2: Incluye current_* prices, line_item_id, y refunds detallados.
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo

    # Calcular shipping total y detectar si tiene envio
    shipping_lines = order.get("shipping_lines") or []
    shipping_total = 0.0
    for sl in shipping_lines:
        shipping_total += float(sl.get("price", 0))
    has_shipping = 1 if shipping_lines else 0

    # Parsear fecha con timezone de la tienda
    raw_dt = order.get("created_at") or ""
    if raw_dt:
        dt = datetime.fromisoformat(raw_dt)
        local_dt = dt.astimezone(ZoneInfo(store_timezone))
        created_at = local_dt.strftime("%Y-%m-%d")
        periodo = local_dt.strftime("%Y-%m")
    else:
        created_at = ""
        periodo = ""

    order_data = {
        "id": str(order["id"]),
        "store_id": store_id,
        "created_at": created_at,
        "periodo": periodo,
        "financial_status": order.get("financial_status", ""),
        "fulfillment_status": order.get("fulfillment_status") or "",
        "total_price": float(order.get("total_price", 0)),
        "subtotal_price": float(order.get("subtotal_price", 0)),
        "total_discounts": float(order.get("total_discounts", 0)),
        "total_tax": float(order.get("total_tax", 0)),
        "total_shipping": shipping_total,
        "has_shipping": has_shipping,
        "current_subtotal_price": float(order.get("current_subtotal_price", 0)),
        "current_total_price": float(order.get("current_total_price", 0)),
        "currency": order.get("currency", ""),
        "source_name": order.get("source_name", ""),
        "tags": order.get("tags", ""),
    }

    # Line items con line_item_id de Shopify
    line_items = []
    for item in (order.get("line_items") or []):
        line_items.append({
            "order_id": str(order["id"]),
            "store_id": store_id,
            "periodo": periodo,
            "financial_status": order.get("financial_status", ""),
            "line_item_id": str(item.get("id", "")),
            "item_name": item.get("name", ""),
            "item_sku": item.get("sku", ""),
            "quantity": int(item.get("quantity", 0)),
            "price": float(item.get("price", 0)),
        })

    # Refunds detallados
    refund_rows = []
    for refund in (order.get("refunds") or []):
        refund_id = str(refund.get("id", ""))
        refund_dt = refund.get("created_at") or raw_dt
        if refund_dt:
            rdt = datetime.fromisoformat(refund_dt)
            local_rdt = rdt.astimezone(ZoneInfo(store_timezone))
            refund_created = local_rdt.strftime("%Y-%m-%d")
            refund_periodo = local_rdt.strftime("%Y-%m")
        else:
            refund_created = created_at
            refund_periodo = periodo

        for rli in (refund.get("refund_line_items") or []):
            li = rli.get("line_item") or {}
            refund_rows.append({
                "refund_id": refund_id,
                "order_id": str(order["id"]),
                "store_id": store_id,
                "created_at": refund_created,
                "periodo": refund_periodo,
                "line_item_id": str(rli.get("line_item_id", "")),
                "item_sku": li.get("sku", ""),
                "item_name": li.get("name", ""),
                "quantity": int(rli.get("quantity", 0)),
                "subtotal": float(rli.get("subtotal", 0)),
            })

    return order_data, line_items, refund_rows


def sync_shopify_periodo(store: dict, periodo: str | None = None) -> dict:
    """
    Sincroniza pedidos de Shopify para un periodo.
    v2.3: Fetch por created_at (ordenes del periodo) + updated_at (ordenes
    de periodos anteriores que fueron modificadas, ej: refunds tardios).
    Retorna {orders: int, lines: int, refunds: int}.
    """
    periodo = periodo or store["periodo_activo"]
    token = get_shopify_token(store)
    since, until = period_to_date_range(periodo)
    store_tz = store.get("timezone", "America/Santiago")

    print(f"  Sync Shopify [{store['id']}]: periodo={periodo}")

    # 1. Ordenes creadas en el periodo (comportamiento original)
    orders_created = shopify_fetch_all(store["shopify_domain"], token, "orders.json", {
        "status": "any",
        "created_at_min": since,
        "created_at_max": until,
        "limit": 250,
    })

    # 2. Ordenes actualizadas en el periodo (captura refunds tardios)
    #    Pueden incluir ordenes de meses anteriores que fueron reembolsadas ahora
    orders_updated = shopify_fetch_all(store["shopify_domain"], token, "orders.json", {
        "status": "any",
        "updated_at_min": since,
        "updated_at_max": until,
        "limit": 250,
    })

    # Merge: deduplicate by order id
    seen_ids = set()
    orders = []
    for o in orders_created + orders_updated:
        oid = o["id"]
        if oid not in seen_ids:
            seen_ids.add(oid)
            orders.append(o)

    print(f"  Shopify: {len(orders)} pedidos obtenidos ({len(orders_created)} created, {len(orders_updated)} updated)")

    # Separar ordenes del periodo target vs ordenes de otros periodos (updated)
    order_rows = []       # ordenes del periodo target (reemplazo atomico)
    line_rows = []
    refund_rows = []
    updated_orders = []   # ordenes de otros periodos (upsert individual)
    updated_lines = []
    updated_refunds = []

    for order in orders:
        od, lines, refunds = extract_order_data(order, store["id"], store_tz)
        if od["periodo"] == periodo:
            order_rows.append(od)
            line_rows.extend(lines)
            refund_rows.extend(refunds)
        elif od["periodo"]:
            # Orden de otro periodo actualizada (ej: refund tardio)
            updated_orders.append(od)
            updated_lines.extend(lines)
            updated_refunds.extend(refunds)

    # Escribir en DB
    with get_db() as conn:
        # 1. Reemplazo atomico del periodo target (comportamiento original)
        conn.execute(
            "DELETE FROM orders WHERE store_id = ? AND periodo = ?",
            (store["id"], periodo),
        )
        conn.execute(
            "DELETE FROM order_lines WHERE store_id = ? AND periodo = ?",
            (store["id"], periodo),
        )
        order_ids = [r["id"] for r in order_rows]
        if order_ids:
            placeholders_ids = ",".join(["?"] * len(order_ids))
            conn.execute(
                f"DELETE FROM order_refunds WHERE store_id = ? AND order_id IN ({placeholders_ids})",
                (store["id"], *order_ids),
            )

        for row in order_rows:
            cols = ", ".join(row.keys())
            placeholders = ", ".join(["?"] * len(row))
            conn.execute(
                f"INSERT INTO orders ({cols}) VALUES ({placeholders})",
                tuple(row.values()),
            )

        for row in line_rows:
            cols = ", ".join(row.keys())
            placeholders = ", ".join(["?"] * len(row))
            conn.execute(
                f"INSERT INTO order_lines ({cols}) VALUES ({placeholders})",
                tuple(row.values()),
            )

        for row in refund_rows:
            cols = ", ".join(row.keys())
            placeholders = ", ".join(["?"] * len(row))
            conn.execute(
                f"INSERT OR REPLACE INTO order_refunds ({cols}) VALUES ({placeholders})",
                tuple(row.values()),
            )

        # 2. Upsert ordenes de otros periodos (actualizadas con refunds tardios)
        for row in updated_orders:
            conn.execute(
                "DELETE FROM orders WHERE store_id = ? AND id = ?",
                (store["id"], row["id"]),
            )
            conn.execute(
                "DELETE FROM order_lines WHERE store_id = ? AND order_id = ?",
                (store["id"], row["id"]),
            )
            conn.execute(
                "DELETE FROM order_refunds WHERE store_id = ? AND order_id = ?",
                (store["id"], row["id"]),
            )
            cols = ", ".join(row.keys())
            placeholders = ", ".join(["?"] * len(row))
            conn.execute(
                f"INSERT INTO orders ({cols}) VALUES ({placeholders})",
                tuple(row.values()),
            )

        for row in updated_lines:
            cols = ", ".join(row.keys())
            placeholders = ", ".join(["?"] * len(row))
            conn.execute(
                f"INSERT INTO order_lines ({cols}) VALUES ({placeholders})",
                tuple(row.values()),
            )

        for row in updated_refunds:
            cols = ", ".join(row.keys())
            placeholders = ", ".join(["?"] * len(row))
            conn.execute(
                f"INSERT OR REPLACE INTO order_refunds ({cols}) VALUES ({placeholders})",
                tuple(row.values()),
            )

    total = len(order_rows) + len(updated_orders)
    print(f"  DB: {len(order_rows)} orders periodo, {len(updated_orders)} orders updated, {len(line_rows)+len(updated_lines)} lines, {len(refund_rows)+len(updated_refunds)} refunds")
    return {
        "orders": total,
        "lines": len(line_rows) + len(updated_lines),
        "refunds": len(refund_rows) + len(updated_refunds),
        "updated_from_other_periods": len(updated_orders),
        "debug": {
            "domain": store["shopify_domain"],
            "periodo": periodo,
            "date_range": f"{since} → {until}",
            "fetched_created": len(orders_created),
            "fetched_updated": len(orders_updated),
            "token_type": "direct" if token.startswith("shpat_") else "oauth",
        },
    }


def sync_shopify_full(store: dict) -> dict:
    """
    Sincroniza ultimos 12 meses de Shopify.
    v2: Incluye refunds detallados.
    Retorna {orders: int, lines: int, refunds: int, months: int}.
    """
    from datetime import date, timedelta

    today = date.today()
    start = date(today.year, today.month, 1)
    for _ in range(11):
        start = (start - timedelta(days=1)).replace(day=1)
    start_period = f"{start.year}-{start.month:02d}"
    end_period = store["periodo_activo"]

    periodos = generate_period_range(start_period, end_period)
    total_orders = 0
    total_lines = 0
    total_refunds = 0

    token = get_shopify_token(store)
    store_tz = store.get("timezone", "America/Santiago")

    for periodo in periodos:
        since, until = period_to_date_range(periodo)
        print(f"  Fetch Shopify: {periodo}")

        orders = shopify_fetch_all(store["shopify_domain"], token, "orders.json", {
            "status": "any",
            "created_at_min": since,
            "created_at_max": until,
            "limit": 250,
        })

        order_rows = []
        line_rows = []
        refund_rows = []
        for order in orders:
            od, lines, refunds = extract_order_data(order, store["id"], store_tz)
            if od["periodo"] != periodo:
                continue  # orden de un dia buffer
            order_rows.append(od)
            line_rows.extend(lines)
            refund_rows.extend(refunds)

        with get_db() as conn:
            conn.execute(
                "DELETE FROM orders WHERE store_id = ? AND periodo = ?",
                (store["id"], periodo),
            )
            conn.execute(
                "DELETE FROM order_lines WHERE store_id = ? AND periodo = ?",
                (store["id"], periodo),
            )
            order_ids = [r["id"] for r in order_rows]
            if order_ids:
                ph = ",".join(["?"] * len(order_ids))
                conn.execute(
                    f"DELETE FROM order_refunds WHERE store_id = ? AND order_id IN ({ph})",
                    (store["id"], *order_ids),
                )
            for row in order_rows:
                cols = ", ".join(row.keys())
                placeholders = ", ".join(["?"] * len(row))
                conn.execute(
                    f"INSERT INTO orders ({cols}) VALUES ({placeholders})",
                    tuple(row.values()),
                )
            for row in line_rows:
                cols = ", ".join(row.keys())
                placeholders = ", ".join(["?"] * len(row))
                conn.execute(
                    f"INSERT INTO order_lines ({cols}) VALUES ({placeholders})",
                    tuple(row.values()),
                )
            for row in refund_rows:
                cols = ", ".join(row.keys())
                placeholders = ", ".join(["?"] * len(row))
                conn.execute(
                    f"INSERT OR REPLACE INTO order_refunds ({cols}) VALUES ({placeholders})",
                    tuple(row.values()),
                )

        total_orders += len(order_rows)
        total_lines += len(line_rows)
        total_refunds += len(refund_rows)
        time.sleep(1)

    print(f"  Full sync: {total_orders} orders, {total_lines} lines, {total_refunds} refunds ({len(periodos)} meses)")
    return {"orders": total_orders, "lines": total_lines, "refunds": total_refunds, "months": len(periodos)}
