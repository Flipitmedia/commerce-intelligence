"""
admin.py — Admin UI: config, costos, gastos, facturas, sync
Server-rendered con Jinja2. Forms POST para CRUD.
"""
from pathlib import Path
from fastapi import APIRouter, HTTPException, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app.database import query_one, query, execute

router = APIRouter(prefix="/admin", tags=["admin"])
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent.parent / "templates"))

# Categorias (mismo que Constants.gs)
CATEGORIAS_FIJOS = ["Plataforma", "Software", "Salarios", "Alquiler", "Contabilidad", "Seguros", "Otros"]
CATEGORIAS_VARIABLES = ["Packaging", "Envios", "Comisiones", "Marketing_otro", "Freelancers", "Otros"]


def get_store(store_id: str, token: str) -> dict:
    store = query_one("SELECT * FROM stores WHERE id = ?", (store_id,))
    if not store:
        raise HTTPException(404, f"Store '{store_id}' no encontrada")
    if store["api_token"] != token:
        raise HTTPException(401, "Token invalido")
    return store


# ── Config ──────────────────────────────────────────────────────

@router.get("/{store_id}", response_class=HTMLResponse)
def admin_config(request: Request, store_id: str, token: str = ""):
    store = get_store(store_id, token)
    return templates.TemplateResponse("config.html", {
        "request": request, "store": store, "token": token, "active_page": "config",
    })


@router.post("/{store_id}/config", response_class=HTMLResponse)
def admin_config_save(
    request: Request, store_id: str, token: str = "",
    name: str = Form(""), currency: str = Form("CLP"), periodo_activo: str = Form(""),
    tax_rate: float = Form(0.19), comision_pasarela: float = Form(0.0349),
    comision_shopify: float = Form(0.01),
    cogs_method: str = Form("porcentaje_fijo"), cogs_pct: float = Form(0.35),
    fulfillment_cost: float = Form(1000), costo_envio_gratis: float = Form(4500),
    costo_envio_estimado: float = Form(0),
    timezone: str = Form("America/Santiago"),
    target_mer: float = Form(3.0), target_margen: float = Form(0.40),
    target_utilidad: float = Form(0.20), target_roas_meta: float = Form(4.0),
    shopify_domain: str = Form(""), shopify_client_id: str = Form(""),
    shopify_client_secret: str = Form(""),
    meta_access_token: str = Form(""), meta_ad_account_id: str = Form(""),
    primary_color: str = Form(""), accent_color: str = Form(""), logo_url: str = Form(""),
):
    store = get_store(store_id, token)
    execute("""
        UPDATE stores SET
            name=?, currency=?, periodo_activo=?, tax_rate=?,
            comision_pasarela=?, comision_shopify=?,
            cogs_method=?, cogs_pct=?, fulfillment_cost=?, costo_envio_gratis=?,
            costo_envio_estimado=?, timezone=?,
            target_mer=?, target_margen=?, target_utilidad=?, target_roas_meta=?,
            shopify_domain=?, shopify_client_id=?, shopify_client_secret=?,
            meta_access_token=?, meta_ad_account_id=?,
            primary_color=?, accent_color=?, logo_url=?
        WHERE id=?
    """, (
        name, currency, periodo_activo, tax_rate,
        comision_pasarela, comision_shopify,
        cogs_method, cogs_pct, fulfillment_cost, costo_envio_gratis,
        costo_envio_estimado, timezone,
        target_mer, target_margen, target_utilidad, target_roas_meta,
        shopify_domain, shopify_client_id, shopify_client_secret,
        meta_access_token, meta_ad_account_id,
        primary_color, accent_color, logo_url,
        store_id,
    ))
    return RedirectResponse(f"/admin/{store_id}?token={token}", status_code=303)


# ── Product Costs ───────────────────────────────────────────────

@router.get("/{store_id}/costs", response_class=HTMLResponse)
def admin_costs(request: Request, store_id: str, token: str = ""):
    store = get_store(store_id, token)
    items = query("SELECT * FROM product_costs WHERE store_id = ? ORDER BY sku, product", (store_id,))
    return templates.TemplateResponse("costs.html", {
        "request": request, "store": store, "token": token,
        "active_page": "costs", "items": items,
    })


@router.post("/{store_id}/costs", response_class=HTMLResponse)
def admin_costs_add(
    store_id: str, token: str = "",
    sku: str = Form(""), product: str = Form(""),
    unit_cost: float = Form(0), notes: str = Form(""),
):
    get_store(store_id, token)
    execute(
        "INSERT INTO product_costs (store_id, sku, product, unit_cost, notes) VALUES (?,?,?,?,?)",
        (store_id, sku, product, unit_cost, notes),
    )
    return RedirectResponse(f"/admin/{store_id}/costs?token={token}", status_code=303)


@router.post("/{store_id}/costs/{item_id}/delete")
def admin_costs_delete(store_id: str, item_id: int, token: str = ""):
    get_store(store_id, token)
    execute("DELETE FROM product_costs WHERE id = ? AND store_id = ?", (item_id, store_id))
    return RedirectResponse(f"/admin/{store_id}/costs?token={token}", status_code=303)


# ── Fixed Costs ─────────────────────────────────────────────────

@router.get("/{store_id}/fixed-costs", response_class=HTMLResponse)
def admin_fixed_costs(request: Request, store_id: str, token: str = ""):
    store = get_store(store_id, token)
    items = query(
        "SELECT * FROM fixed_costs WHERE store_id = ? ORDER BY periodo DESC, category",
        (store_id,),
    )
    return templates.TemplateResponse("fixed_costs.html", {
        "request": request, "store": store, "token": token,
        "active_page": "fixed_costs", "items": items, "categories": CATEGORIAS_FIJOS,
    })


@router.post("/{store_id}/fixed-costs", response_class=HTMLResponse)
def admin_fixed_costs_add(
    store_id: str, token: str = "",
    periodo: str = Form(""), category: str = Form(""),
    description: str = Form(""), amount: float = Form(0),
    recurring: str = Form("0"), tax_included: str = Form("1"),
    notes: str = Form(""),
):
    get_store(store_id, token)
    rec = 1 if recurring == "1" else 0
    tax_inc = 1 if tax_included == "1" else 0
    execute(
        "INSERT INTO fixed_costs (store_id, periodo, category, description, amount, recurring, tax_included, notes) VALUES (?,?,?,?,?,?,?,?)",
        (store_id, periodo, category, description, amount, rec, tax_inc, notes),
    )
    return RedirectResponse(f"/admin/{store_id}/fixed-costs?token={token}", status_code=303)


@router.post("/{store_id}/fixed-costs/{item_id}/edit")
def admin_fixed_costs_edit(
    store_id: str, item_id: int, token: str = "",
    periodo: str = Form(""), category: str = Form(""),
    description: str = Form(""), amount: float = Form(0),
    recurring: str = Form("0"), tax_included: str = Form("1"),
    notes: str = Form(""),
):
    get_store(store_id, token)
    rec = 1 if recurring == "1" else 0
    tax_inc = 1 if tax_included == "1" else 0
    execute(
        "UPDATE fixed_costs SET periodo=?, category=?, description=?, amount=?, recurring=?, tax_included=?, notes=? WHERE id=? AND store_id=?",
        (periodo, category, description, amount, rec, tax_inc, notes, item_id, store_id),
    )
    return RedirectResponse(f"/admin/{store_id}/fixed-costs?token={token}", status_code=303)


@router.post("/{store_id}/fixed-costs/{item_id}/delete")
def admin_fixed_costs_delete(store_id: str, item_id: int, token: str = ""):
    get_store(store_id, token)
    execute("DELETE FROM fixed_costs WHERE id = ? AND store_id = ?", (item_id, store_id))
    return RedirectResponse(f"/admin/{store_id}/fixed-costs?token={token}", status_code=303)


# ── Variable Costs ──────────────────────────────────────────────

@router.get("/{store_id}/variable-costs", response_class=HTMLResponse)
def admin_variable_costs(request: Request, store_id: str, token: str = ""):
    store = get_store(store_id, token)
    items = query(
        "SELECT * FROM variable_costs WHERE store_id = ? ORDER BY date DESC",
        (store_id,),
    )
    return templates.TemplateResponse("variable_costs.html", {
        "request": request, "store": store, "token": token,
        "active_page": "variable_costs", "items": items, "categories": CATEGORIAS_VARIABLES,
    })


@router.post("/{store_id}/variable-costs", response_class=HTMLResponse)
def admin_variable_costs_add(
    store_id: str, token: str = "",
    date: str = Form(""), periodo: str = Form(""),
    category: str = Form(""), description: str = Form(""),
    amount: float = Form(0), tax_included: str = Form("1"),
    notes: str = Form(""),
):
    get_store(store_id, token)
    tax_inc = 1 if tax_included == "1" else 0
    execute(
        "INSERT INTO variable_costs (store_id, date, periodo, category, description, amount, tax_included, notes) VALUES (?,?,?,?,?,?,?,?)",
        (store_id, date, periodo, category, description, amount, tax_inc, notes),
    )
    return RedirectResponse(f"/admin/{store_id}/variable-costs?token={token}", status_code=303)


@router.post("/{store_id}/variable-costs/{item_id}/edit")
def admin_variable_costs_edit(
    store_id: str, item_id: int, token: str = "",
    date: str = Form(""), periodo: str = Form(""),
    category: str = Form(""), description: str = Form(""),
    amount: float = Form(0), tax_included: str = Form("1"),
    notes: str = Form(""),
):
    get_store(store_id, token)
    tax_inc = 1 if tax_included == "1" else 0
    execute(
        "UPDATE variable_costs SET date=?, periodo=?, category=?, description=?, amount=?, tax_included=?, notes=? WHERE id=? AND store_id=?",
        (date, periodo, category, description, amount, tax_inc, notes, item_id, store_id),
    )
    return RedirectResponse(f"/admin/{store_id}/variable-costs?token={token}", status_code=303)


@router.post("/{store_id}/variable-costs/{item_id}/delete")
def admin_variable_costs_delete(store_id: str, item_id: int, token: str = ""):
    get_store(store_id, token)
    execute("DELETE FROM variable_costs WHERE id = ? AND store_id = ?", (item_id, store_id))
    return RedirectResponse(f"/admin/{store_id}/variable-costs?token={token}", status_code=303)


# ── Purchase Invoices ───────────────────────────────────────────

@router.get("/{store_id}/invoices", response_class=HTMLResponse)
def admin_invoices(request: Request, store_id: str, token: str = ""):
    store = get_store(store_id, token)
    items = query(
        "SELECT * FROM purchase_invoices WHERE store_id = ? ORDER BY date DESC",
        (store_id,),
    )
    return templates.TemplateResponse("invoices.html", {
        "request": request, "store": store, "token": token,
        "active_page": "invoices", "items": items,
    })


@router.post("/{store_id}/invoices", response_class=HTMLResponse)
def admin_invoices_add(
    store_id: str, token: str = "",
    date: str = Form(""), periodo: str = Form(""),
    supplier: str = Form(""), description: str = Form(""),
    net_amount: float = Form(0), iva: float = Form(0),
    total_amount: float = Form(0), invoice_number: str = Form(""),
    impacts_pnl: str = Form("0"), pnl_category: str = Form(""),
    notes: str = Form(""),
):
    get_store(store_id, token)
    imp_pnl = 1 if impacts_pnl == "1" else 0
    # v2.1: si impacts_pnl=1 pero no hay categoria valida, forzar no-impact
    if imp_pnl == 1 and pnl_category not in ("cogs", "fixed", "variable"):
        imp_pnl = 0
    execute(
        """INSERT INTO purchase_invoices
        (store_id, date, periodo, supplier, description, net_amount, iva, total_amount, invoice_number, impacts_pnl, pnl_category, notes)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (store_id, date, periodo, supplier, description, net_amount, iva, total_amount, invoice_number, imp_pnl, pnl_category, notes),
    )
    return RedirectResponse(f"/admin/{store_id}/invoices?token={token}", status_code=303)


@router.post("/{store_id}/invoices/{item_id}/delete")
def admin_invoices_delete(store_id: str, item_id: int, token: str = ""):
    get_store(store_id, token)
    execute("DELETE FROM purchase_invoices WHERE id = ? AND store_id = ?", (item_id, store_id))
    return RedirectResponse(f"/admin/{store_id}/invoices?token={token}", status_code=303)


# ── Sync ────────────────────────────────────────────────────────

def get_db_stats(store_id: str) -> dict:
    orders = query_one("SELECT COUNT(*) as c FROM orders WHERE store_id=?", (store_id,))
    lines = query_one("SELECT COUNT(*) as c FROM order_lines WHERE store_id=?", (store_id,))
    refunds = query_one("SELECT COUNT(*) as c FROM order_refunds WHERE store_id=?", (store_id,))
    meta = query_one("SELECT COUNT(*) as c FROM meta_insights WHERE store_id=?", (store_id,))
    periodos = query_one(
        "SELECT COUNT(DISTINCT periodo) as c FROM orders WHERE store_id=?", (store_id,)
    )
    return {
        "orders": orders["c"] if orders else 0,
        "order_lines": lines["c"] if lines else 0,
        "order_refunds": refunds["c"] if refunds else 0,
        "meta_insights": meta["c"] if meta else 0,
        "periodos": periodos["c"] if periodos else 0,
    }


@router.get("/{store_id}/sync", response_class=HTMLResponse)
def admin_sync(request: Request, store_id: str, token: str = ""):
    store = get_store(store_id, token)
    return templates.TemplateResponse("sync.html", {
        "request": request, "store": store, "token": token,
        "active_page": "sync", "stats": get_db_stats(store_id),
        "result": None, "error": None,
    })


@router.post("/{store_id}/sync/shopify", response_class=HTMLResponse)
def admin_sync_shopify(request: Request, store_id: str, token: str = ""):
    store = get_store(store_id, token)
    try:
        from app.sync_shopify import sync_shopify_periodo
        result = sync_shopify_periodo(store)
    except Exception as e:
        return templates.TemplateResponse("sync.html", {
            "request": request, "store": store, "token": token,
            "active_page": "sync", "stats": get_db_stats(store_id),
            "result": None, "error": str(e),
        })
    return templates.TemplateResponse("sync.html", {
        "request": request, "store": store, "token": token,
        "active_page": "sync", "stats": get_db_stats(store_id),
        "result": {"shopify": result}, "error": None,
    })


@router.post("/{store_id}/sync/shopify-full", response_class=HTMLResponse)
def admin_sync_shopify_full(request: Request, store_id: str, token: str = ""):
    store = get_store(store_id, token)
    try:
        from app.sync_shopify import sync_shopify_full
        result = sync_shopify_full(store)
    except Exception as e:
        return templates.TemplateResponse("sync.html", {
            "request": request, "store": store, "token": token,
            "active_page": "sync", "stats": get_db_stats(store_id),
            "result": None, "error": str(e),
        })
    return templates.TemplateResponse("sync.html", {
        "request": request, "store": store, "token": token,
        "active_page": "sync", "stats": get_db_stats(store_id),
        "result": {"shopify_full": result}, "error": None,
    })


@router.post("/{store_id}/sync/meta", response_class=HTMLResponse)
def admin_sync_meta(request: Request, store_id: str, token: str = ""):
    store = get_store(store_id, token)
    try:
        from app.sync_meta import sync_meta_periodo
        result = sync_meta_periodo(store)
    except Exception as e:
        return templates.TemplateResponse("sync.html", {
            "request": request, "store": store, "token": token,
            "active_page": "sync", "stats": get_db_stats(store_id),
            "result": None, "error": str(e),
        })
    return templates.TemplateResponse("sync.html", {
        "request": request, "store": store, "token": token,
        "active_page": "sync", "stats": get_db_stats(store_id),
        "result": {"meta": result}, "error": None,
    })


@router.post("/{store_id}/sync/meta-full", response_class=HTMLResponse)
def admin_sync_meta_full(request: Request, store_id: str, token: str = ""):
    store = get_store(store_id, token)
    try:
        from app.sync_meta import sync_meta_full
        result = sync_meta_full(store)
    except Exception as e:
        return templates.TemplateResponse("sync.html", {
            "request": request, "store": store, "token": token,
            "active_page": "sync", "stats": get_db_stats(store_id),
            "result": None, "error": str(e),
        })
    return templates.TemplateResponse("sync.html", {
        "request": request, "store": store, "token": token,
        "active_page": "sync", "stats": get_db_stats(store_id),
        "result": {"meta_full": result}, "error": None,
    })


@router.post("/{store_id}/sync/all", response_class=HTMLResponse)
def admin_sync_all(request: Request, store_id: str, token: str = ""):
    """Sync Shopify + Meta periodo activo en un solo click."""
    store = get_store(store_id, token)
    result = {}
    error = None
    try:
        if store["shopify_domain"] and store["shopify_client_id"]:
            from app.sync_shopify import sync_shopify_periodo
            result["shopify"] = sync_shopify_periodo(store)
        else:
            result["shopify"] = {"skipped": True, "reason": "No Shopify credentials"}

        if store["meta_access_token"] and store["meta_ad_account_id"]:
            from app.sync_meta import sync_meta_periodo
            result["meta"] = sync_meta_periodo(store)
        else:
            result["meta"] = {"skipped": True, "reason": "No Meta credentials"}
    except Exception as e:
        error = str(e)

    return templates.TemplateResponse("sync.html", {
        "request": request, "store": store, "token": token,
        "active_page": "sync", "stats": get_db_stats(store_id),
        "result": result if not error else None, "error": error,
    })
