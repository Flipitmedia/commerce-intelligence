"""
models.py — Tablas SQL (CREATE TABLE statements)
Mapean directamente a las hojas de Google Sheets.
"""
from app.database import executescript, query_one, execute

SCHEMA = """
-- ── stores (reemplaza CONFIG) ──────────────────────────────────
CREATE TABLE IF NOT EXISTS stores (
    id                  TEXT PRIMARY KEY,
    name                TEXT NOT NULL DEFAULT 'Mi Tienda',
    currency            TEXT NOT NULL DEFAULT 'CLP',
    periodo_activo      TEXT NOT NULL DEFAULT '2026-03',
    tax_rate            REAL NOT NULL DEFAULT 0.19,
    cogs_method         TEXT NOT NULL DEFAULT 'porcentaje_fijo',
    cogs_pct            REAL NOT NULL DEFAULT 0.35,
    comision_pasarela   REAL NOT NULL DEFAULT 0.0349,
    comision_shopify    REAL NOT NULL DEFAULT 0.01,
    fulfillment_cost    REAL NOT NULL DEFAULT 1000,
    costo_envio_gratis  REAL NOT NULL DEFAULT 4500,
    target_mer          REAL NOT NULL DEFAULT 3.0,
    target_margen       REAL NOT NULL DEFAULT 0.40,
    target_utilidad     REAL NOT NULL DEFAULT 0.20,
    target_roas_meta    REAL NOT NULL DEFAULT 4.0,
    shopify_domain      TEXT NOT NULL DEFAULT '',
    shopify_client_id   TEXT NOT NULL DEFAULT '',
    shopify_client_secret TEXT NOT NULL DEFAULT '',
    meta_access_token   TEXT NOT NULL DEFAULT '',
    meta_ad_account_id  TEXT NOT NULL DEFAULT '',
    primary_color       TEXT NOT NULL DEFAULT '#5abfb5',
    accent_color        TEXT NOT NULL DEFAULT '#cf7866',
    logo_url            TEXT NOT NULL DEFAULT '',
    api_token           TEXT NOT NULL
);

-- ── orders (reemplaza RAW_SHOPIFY) ─────────────────────────────
CREATE TABLE IF NOT EXISTS orders (
    id                  TEXT NOT NULL,
    store_id            TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    periodo             TEXT NOT NULL,
    financial_status    TEXT NOT NULL DEFAULT '',
    fulfillment_status  TEXT NOT NULL DEFAULT '',
    total_price         REAL NOT NULL DEFAULT 0,
    subtotal_price      REAL NOT NULL DEFAULT 0,
    total_discounts     REAL NOT NULL DEFAULT 0,
    total_tax           REAL NOT NULL DEFAULT 0,
    total_shipping      REAL NOT NULL DEFAULT 0,
    currency            TEXT NOT NULL DEFAULT '',
    source_name         TEXT NOT NULL DEFAULT '',
    tags                TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (id, store_id),
    FOREIGN KEY (store_id) REFERENCES stores(id)
);
CREATE INDEX IF NOT EXISTS idx_orders_periodo ON orders(store_id, periodo, financial_status);

-- ── order_lines (reemplaza RAW_SHOPIFY_LINES) ──────────────────
CREATE TABLE IF NOT EXISTS order_lines (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id            TEXT NOT NULL,
    store_id            TEXT NOT NULL,
    periodo             TEXT NOT NULL,
    financial_status    TEXT NOT NULL DEFAULT '',
    item_name           TEXT NOT NULL DEFAULT '',
    item_sku            TEXT NOT NULL DEFAULT '',
    quantity            INTEGER NOT NULL DEFAULT 0,
    price               REAL NOT NULL DEFAULT 0,
    FOREIGN KEY (store_id) REFERENCES stores(id)
);
CREATE INDEX IF NOT EXISTS idx_order_lines_periodo ON order_lines(store_id, periodo, financial_status);

-- ── meta_insights (reemplaza RAW_META) ─────────────────────────
CREATE TABLE IF NOT EXISTS meta_insights (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    store_id            TEXT NOT NULL,
    date                TEXT NOT NULL,
    periodo             TEXT NOT NULL,
    campaign_id         TEXT NOT NULL DEFAULT '',
    campaign_name       TEXT NOT NULL DEFAULT '',
    objective           TEXT NOT NULL DEFAULT '',
    spend               REAL NOT NULL DEFAULT 0,
    impressions         INTEGER NOT NULL DEFAULT 0,
    clicks              INTEGER NOT NULL DEFAULT 0,
    ctr                 REAL NOT NULL DEFAULT 0,
    cpm                 REAL NOT NULL DEFAULT 0,
    cpc                 REAL NOT NULL DEFAULT 0,
    purchases           REAL NOT NULL DEFAULT 0,
    purchase_value      REAL NOT NULL DEFAULT 0,
    add_to_cart         REAL NOT NULL DEFAULT 0,
    initiate_checkout   REAL NOT NULL DEFAULT 0,
    UNIQUE(store_id, date, campaign_id),
    FOREIGN KEY (store_id) REFERENCES stores(id)
);

-- ── product_costs (reemplaza COSTOS_PRODUCTOS) ─────────────────
CREATE TABLE IF NOT EXISTS product_costs (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    store_id            TEXT NOT NULL,
    sku                 TEXT NOT NULL DEFAULT '',
    product             TEXT NOT NULL DEFAULT '',
    unit_cost           REAL NOT NULL DEFAULT 0,
    notes               TEXT NOT NULL DEFAULT '',
    FOREIGN KEY (store_id) REFERENCES stores(id)
);

-- ── fixed_costs (reemplaza GASTOS_FIJOS) ───────────────────────
CREATE TABLE IF NOT EXISTS fixed_costs (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    store_id            TEXT NOT NULL,
    periodo             TEXT NOT NULL,
    category            TEXT NOT NULL DEFAULT '',
    description         TEXT NOT NULL DEFAULT '',
    amount              REAL NOT NULL DEFAULT 0,
    recurring           INTEGER NOT NULL DEFAULT 0,
    notes               TEXT NOT NULL DEFAULT '',
    FOREIGN KEY (store_id) REFERENCES stores(id)
);

-- ── variable_costs (reemplaza GASTOS_VARIABLES) ────────────────
CREATE TABLE IF NOT EXISTS variable_costs (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    store_id            TEXT NOT NULL,
    date                TEXT NOT NULL,
    periodo             TEXT NOT NULL,
    category            TEXT NOT NULL DEFAULT '',
    description         TEXT NOT NULL DEFAULT '',
    amount              REAL NOT NULL DEFAULT 0,
    notes               TEXT NOT NULL DEFAULT '',
    FOREIGN KEY (store_id) REFERENCES stores(id)
);

-- ── purchase_invoices (reemplaza FACTURAS_COMPRA) ──────────────
CREATE TABLE IF NOT EXISTS purchase_invoices (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    store_id            TEXT NOT NULL,
    date                TEXT NOT NULL,
    periodo             TEXT NOT NULL,
    supplier            TEXT NOT NULL DEFAULT '',
    description         TEXT NOT NULL DEFAULT '',
    net_amount          REAL NOT NULL DEFAULT 0,
    iva                 REAL NOT NULL DEFAULT 0,
    total_amount        REAL NOT NULL DEFAULT 0,
    invoice_number      TEXT NOT NULL DEFAULT '',
    notes               TEXT NOT NULL DEFAULT '',
    FOREIGN KEY (store_id) REFERENCES stores(id)
);
"""


def create_tables():
    """Crea todas las tablas si no existen."""
    executescript(SCHEMA)


def seed_stores():
    """Inserta las tiendas iniciales si no existen."""
    import secrets

    stores = [
        {
            "id": "micovitae",
            "name": "Micovitae",
            "currency": "CLP",
            "periodo_activo": "2026-03",
            "tax_rate": 0.19,
            "cogs_method": "porcentaje_fijo",
            "cogs_pct": 0.35,
            "comision_pasarela": 0.0349,
            "comision_shopify": 0.01,
            "fulfillment_cost": 1000,
            "costo_envio_gratis": 4500,
            "target_mer": 3.0,
            "target_margen": 0.40,
            "target_utilidad": 0.20,
            "target_roas_meta": 4.0,
            "shopify_domain": "128706-2.myshopify.com",
            "shopify_client_id": "",
            "shopify_client_secret": "",
            "meta_access_token": "",
            "meta_ad_account_id": "",
            "primary_color": "#5abfb5",
            "accent_color": "#cf7866",
            "logo_url": "",
        },
        {
            "id": "lali",
            "name": "Lali",
            "currency": "CLP",
            "periodo_activo": "2026-03",
            "tax_rate": 0.19,
            "cogs_method": "porcentaje_fijo",
            "cogs_pct": 0.35,
            "comision_pasarela": 0.0349,
            "comision_shopify": 0.01,
            "fulfillment_cost": 1000,
            "costo_envio_gratis": 4500,
            "target_mer": 3.0,
            "target_margen": 0.40,
            "target_utilidad": 0.20,
            "target_roas_meta": 4.0,
            "shopify_domain": "",
            "shopify_client_id": "",
            "shopify_client_secret": "",
            "meta_access_token": "",
            "meta_ad_account_id": "",
            "primary_color": "#5abfb5",
            "accent_color": "#cf7866",
            "logo_url": "",
        },
    ]

    for s in stores:
        existing = query_one("SELECT id FROM stores WHERE id = ?", (s["id"],))
        if not existing:
            s["api_token"] = secrets.token_urlsafe(32)
            cols = ", ".join(s.keys())
            placeholders = ", ".join(["?"] * len(s))
            execute(
                f"INSERT INTO stores ({cols}) VALUES ({placeholders})",
                tuple(s.values()),
            )
            print(f"  Store '{s['id']}' creada (token: {s['api_token']})")


def init_db():
    """Inicializa la base de datos: crea tablas y seed."""
    print("Inicializando base de datos...")
    create_tables()
    seed_stores()
    print("Base de datos lista.")
