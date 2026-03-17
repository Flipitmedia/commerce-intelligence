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
    YYYY-MM -> (since, until) en formato ISO.
    Ej: '2026-03' -> ('2026-03-01T00:00:00-00:00', '2026-03-31T23:59:59-00:00')
    """
    parts = periodo.split("-")
    year = int(parts[0])
    month = int(parts[1])
    last_day = calendar.monthrange(year, month)[1]
    since = f"{year}-{month:02d}-01T00:00:00-00:00"
    until = f"{year}-{month:02d}-{last_day:02d}T23:59:59-00:00"
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

def extract_order_data(order: dict, store_id: str) -> tuple[list, list[list]]:
    """
    Extrae datos de un order de Shopify.
    Retorna (order_row, [line_rows]).
    order_row: datos para tabla orders
    line_rows: datos para tabla order_lines
    """
    # Calcular shipping total
    shipping_total = 0.0
    for sl in (order.get("shipping_lines") or []):
        shipping_total += float(sl.get("price", 0))

    created_at = (order.get("created_at") or "")[:10]  # YYYY-MM-DD
    periodo = created_at[:7]  # YYYY-MM

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
        "currency": order.get("currency", ""),
        "source_name": order.get("source_name", ""),
        "tags": order.get("tags", ""),
    }

    line_items = []
    for item in (order.get("line_items") or []):
        line_items.append({
            "order_id": str(order["id"]),
            "store_id": store_id,
            "periodo": periodo,
            "financial_status": order.get("financial_status", ""),
            "item_name": item.get("name", ""),
            "item_sku": item.get("sku", ""),
            "quantity": int(item.get("quantity", 0)),
            "price": float(item.get("price", 0)),
        })

    return order_data, line_items


def sync_shopify_periodo(store: dict, periodo: str | None = None) -> dict:
    """
    Sincroniza pedidos de Shopify para un periodo.
    Si periodo es None, usa store['periodo_activo'].
    Retorna {orders: int, lines: int}.
    """
    periodo = periodo or store["periodo_activo"]
    token = get_shopify_token(store)
    since, until = period_to_date_range(periodo)

    print(f"  Sync Shopify [{store['id']}]: periodo={periodo}")

    orders = shopify_fetch_all(store["shopify_domain"], token, "orders.json", {
        "status": "any",
        "created_at_min": since,
        "created_at_max": until,
        "limit": 250,
    })

    print(f"  Shopify: {len(orders)} pedidos obtenidos")

    # Preparar datos
    order_rows = []
    line_rows = []
    for order in orders:
        od, lines = extract_order_data(order, store["id"])
        od["store_id"] = store["id"]
        order_rows.append(od)
        for line in lines:
            line["store_id"] = store["id"]
            line_rows.append(line)

    # Escribir en DB (reemplazo atomico por periodo)
    with get_db() as conn:
        # Delete existing periodo data
        conn.execute(
            "DELETE FROM orders WHERE store_id = ? AND periodo = ?",
            (store["id"], periodo),
        )
        conn.execute(
            "DELETE FROM order_lines WHERE store_id = ? AND periodo = ?",
            (store["id"], periodo),
        )

        # Insert new data
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

    print(f"  DB: {len(order_rows)} orders, {len(line_rows)} lines")
    return {"orders": len(order_rows), "lines": len(line_rows)}


def sync_shopify_full(store: dict) -> dict:
    """
    Sincroniza ultimos 12 meses de Shopify.
    Retorna {orders: int, lines: int, months: int}.
    """
    from datetime import date, timedelta

    today = date.today()
    start = date(today.year, today.month, 1)
    # Retroceder 11 meses
    for _ in range(11):
        start = (start - timedelta(days=1)).replace(day=1)
    start_period = f"{start.year}-{start.month:02d}"
    end_period = store["periodo_activo"]

    periodos = generate_period_range(start_period, end_period)
    total_orders = 0
    total_lines = 0

    token = get_shopify_token(store)

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
        for order in orders:
            od, lines = extract_order_data(order, store["id"])
            od["store_id"] = store["id"]
            order_rows.append(od)
            for line in lines:
                line["store_id"] = store["id"]
                line_rows.append(line)

        # Escribir periodo en DB
        with get_db() as conn:
            conn.execute(
                "DELETE FROM orders WHERE store_id = ? AND periodo = ?",
                (store["id"], periodo),
            )
            conn.execute(
                "DELETE FROM order_lines WHERE store_id = ? AND periodo = ?",
                (store["id"], periodo),
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

        total_orders += len(order_rows)
        total_lines += len(line_rows)
        time.sleep(1)  # Pausa entre meses

    print(f"  Full sync: {total_orders} orders, {total_lines} lines ({len(periodos)} meses)")
    return {"orders": total_orders, "lines": total_lines, "months": len(periodos)}
