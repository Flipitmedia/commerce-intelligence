"""
api.py — JSON API: /api/{store}/data, /api/{store}/sync
Mismo contrato que doGet de WebApp.gs
"""
from datetime import datetime
from fastapi import APIRouter, HTTPException, BackgroundTasks
from app.database import query_one
from app.pnl import compute_pnl, compute_historico

router = APIRouter(prefix="/api", tags=["api"])

CI_VERSION = "2.4.0"


def get_store_or_404(store_id: str, token: str) -> dict:
    """Valida store_id y api_token."""
    store = query_one("SELECT * FROM stores WHERE id = ?", (store_id,))
    if not store:
        raise HTTPException(404, f"Store '{store_id}' no encontrada")
    if store["api_token"] != token:
        raise HTTPException(401, "Token invalido")
    return store


def build_kpis(pnl: dict, store: dict) -> list[dict]:
    """
    Genera lista de KPIs con targets y estado.
    Replica lo que DASHBOARD mostraba.
    """
    def status(value, target, higher_is_better=True):
        if value is None or target == 0:
            return "N/A" if value is None else "OK"
        ratio = value / target if target != 0 else 0
        if higher_is_better:
            if ratio >= 1.0:
                return "OK"
            elif ratio >= 0.8:
                return "ALERTA"
            else:
                return "CRITICO"
        else:
            # Lower is better (ej: CPA)
            if ratio <= 1.0:
                return "OK"
            elif ratio <= 1.2:
                return "ALERTA"
            else:
                return "CRITICO"

    # Order MUST match dashboard HERO_KPI_INDICES [1,7,2] and SECONDARY [0,3,4,5,6,8,9]
    # idx 0: Ventas Brutas, 1: Ingresos Netos, 2: Margen %, 3: Gasto Ads,
    # 4: MER, 5: ROAS Meta, 6: CPA, 7: Utilidad Op, 8: Utilidad Op %, 9: Break-even
    return [
        {
            "name": "Ventas Brutas (IVA incl.)",
            "value": pnl["ventas_brutas"],
            "target": None,
            "status": "OK",
        },
        {
            "name": "Ingresos Netos (sin IVA)",
            "value": pnl["ingresos_netos"],
            "target": None,
            "status": "OK",
        },
        {
            "name": "Margen Bruto %",
            "value": pnl["margen_bruto_pct"],
            "target": store["target_margen"],
            "status": status(pnl["margen_bruto_pct"], store["target_margen"]),
        },
        {
            "name": "Gasto Ads (Meta)",
            "value": pnl["gasto_ads"],
            "target": None,
            "status": "OK",
        },
        {
            "name": "MER (ROAS General)",
            "value": pnl["mer"],
            "target": store["target_mer"],
            "status": status(pnl["mer"], store["target_mer"]),
        },
        {
            "name": "ROAS Meta",
            "value": pnl["roas_meta"],
            "target": store["target_roas_meta"],
            "status": status(pnl["roas_meta"], store["target_roas_meta"]),
        },
        {
            "name": "CPA",
            "value": pnl["cpa"],
            "target": None,
            "status": "OK",
        },
        {
            "name": "Utilidad Operacional",
            "value": pnl["utilidad_op"],
            "target": None,
            "status": "OK" if pnl["utilidad_op"] > 0 else "CRITICO",
        },
        {
            "name": "Utilidad Operacional %",
            "value": pnl["utilidad_op_pct"],
            "target": store["target_utilidad"],
            "status": status(pnl["utilidad_op_pct"], store["target_utilidad"]),
        },
        {
            "name": "Break-even ROAS",
            "value": pnl["breakeven_roas"],
            "target": None,
            "status": "N/A" if pnl["breakeven_roas"] is None else "OK",
        },
    ]


def build_pnl_cascade(pnl: dict) -> list[dict]:
    """
    Genera P&L en formato cascada para el dashboard.
    v2.3: Incluye fila de ajuste de reconciliacion si la cascada visual
    (ventas_brutas - descuentos + envios - devoluciones) difiere de
    facturacion autoritativa (SUM current_total_price) por >= $1.
    """
    ingresos = pnl["ingresos_netos"]

    def pct(val):
        if ingresos is None or ingresos <= 0:
            return None
        return round(val / ingresos, 4)

    # v2.3: Cascade visual reconciliation check.
    # ventas_brutas - descuentos + envios = total_cobrado (original)
    # facturacion = total_cobrado - devoluciones = total_retenido (authoritative)
    # If rounding causes mismatch, add adjustment row.
    cascade_sum = pnl["ventas_brutas"] - pnl["descuentos"] + pnl["ingreso_envios"] - pnl["devoluciones"]
    ajuste = pnl["facturacion"] - cascade_sum  # should be 0; non-zero = rounding/tax diff

    # Concept names MUST match dashboard PNL_STEPS keys exactly
    rows = [
        {"concept": "(+) Ventas Brutas (IVA incl.)", "amount": pnl["ventas_brutas"], "pctOfSales": pct(pnl["ventas_brutas"])},
        {"concept": "(-) Descuentos", "amount": -pnl["descuentos"], "pctOfSales": pct(-pnl["descuentos"])},
        {"concept": "(+) Recaudado en Envios", "amount": pnl["ingreso_envios"], "pctOfSales": pct(pnl["ingreso_envios"])},
        {"concept": "(-) Devoluciones", "amount": -pnl["devoluciones"], "pctOfSales": pct(-pnl["devoluciones"])},
    ]
    if abs(ajuste) >= 1:
        rows.append({"concept": "(±) Ajuste de Reconciliacion", "amount": ajuste, "pctOfSales": pct(ajuste)})
    rows += [
        {"concept": "(=) Facturacion (con IVA)", "amount": pnl["facturacion"], "pctOfSales": pct(pnl["facturacion"])},
        {"concept": "(-) IVA Debito Fiscal", "amount": -pnl["iva_debito"], "pctOfSales": pct(-pnl["iva_debito"])},
        {"concept": "(=) Ingresos Netos (sin IVA)", "amount": pnl["ingresos_netos"], "pctOfSales": 1.0 if ingresos and ingresos > 0 else None},
        {"concept": "(-) Costo de Productos", "amount": -pnl["costo_productos"], "pctOfSales": pct(-pnl["costo_productos"])},
        {"concept": "(-) Picking y Packing", "amount": -pnl["picking_packing"], "pctOfSales": pct(-pnl["picking_packing"])},
        {"concept": "(-) Costo Total Envios", "amount": -pnl["costo_envios"], "pctOfSales": pct(-pnl["costo_envios"])},
        {"concept": "(=) Margen Bruto", "amount": pnl["margen_bruto"], "pctOfSales": pnl["margen_bruto_pct"]},
        {"concept": "(-) Gasto Ads (Meta)", "amount": -pnl["gasto_ads"], "pctOfSales": pct(-pnl["gasto_ads"])},
        {"concept": "(-) Comision Pasarela", "amount": -pnl["comision_pasarela"], "pctOfSales": pct(-pnl["comision_pasarela"])},
        {"concept": "(-) Comision Shopify", "amount": -pnl["comision_shopify"], "pctOfSales": pct(-pnl["comision_shopify"])},
        {"concept": "(-) Gastos Fijos", "amount": -pnl["gastos_fijos"], "pctOfSales": pct(-pnl["gastos_fijos"])},
        {"concept": "(-) Gastos Variables", "amount": -pnl["gastos_variables"], "pctOfSales": pct(-pnl["gastos_variables"])},
        {"concept": "(=) Total Gastos Op.", "amount": -pnl["total_gastos_op"], "pctOfSales": pct(-pnl["total_gastos_op"])},
        {"concept": "(=) UTILIDAD OPERACIONAL", "amount": pnl["utilidad_op"], "pctOfSales": pnl["utilidad_op_pct"]},
    ]
    return rows


# ── Endpoints ───────────────────────────────────────────────────

@router.get("/{store_id}/debug")
def debug_data(store_id: str, token: str = ""):
    """Debug: muestra conteo de ordenes por financial_status y periodo."""
    from app.database import query
    store = get_store_or_404(store_id, token)
    status_counts = query(
        "SELECT periodo, financial_status, COUNT(*) as cnt FROM orders WHERE store_id = ? GROUP BY periodo, financial_status ORDER BY periodo, financial_status",
        (store_id,),
    )
    meta_counts = query(
        "SELECT periodo, COUNT(*) as cnt, SUM(spend) as total_spend FROM meta_insights WHERE store_id = ? GROUP BY periodo",
        (store_id,),
    )
    sample_order = query(
        "SELECT id, financial_status, total_price, current_total_price, subtotal_price, current_subtotal_price, total_discounts, total_shipping, has_shipping, fulfillment_status FROM orders WHERE store_id = ? LIMIT 3",
        (store_id,),
    )
    return {
        "orders_by_status": [dict(r) for r in status_counts],
        "meta_by_periodo": [dict(r) for r in meta_counts],
        "sample_orders": [dict(r) for r in sample_order],
    }


@router.get("/{store_id}/data")
def get_data(store_id: str, token: str = ""):
    """JSON completo — mismo contrato que doGet de WebApp.gs."""
    store = get_store_or_404(store_id, token)
    pnl = compute_pnl(store)

    return {
        "version": CI_VERSION,
        "generatedAt": datetime.utcnow().isoformat() + "Z",
        "config": {
            "storeName": store["name"],
            "currency": store["currency"],
            "periodoActivo": store["periodo_activo"],
            "targetMER": store["target_mer"],
            "targetMargen": store["target_margen"],
            "targetUtilidad": store["target_utilidad"],
            "taxRate": store["tax_rate"],
            "cogsMethod": store["cogs_method"],
            "cogsPct": store["cogs_pct"],
            "comisionPasarela": store["comision_pasarela"],
            "comisionShopify": store["comision_shopify"],
            "fulfillmentCost": store["fulfillment_cost"],
            "costoEnvioGratis": store["costo_envio_gratis"],
            "targetROASMeta": store["target_roas_meta"],
            "primaryColor": store["primary_color"],
            "accentColor": store["accent_color"],
            "logoUrl": store["logo_url"],
        },
        "kpis": build_kpis(pnl, store),
        "pnl": build_pnl_cascade(pnl),
        "historico": compute_historico(store),
    }


@router.get("/{store_id}/stats")
def get_stats(store_id: str, token: str = ""):
    """DB stats para la UI de sync."""
    from app.database import query
    store = get_store_or_404(store_id, token)
    orders = query_one("SELECT COUNT(*) as c FROM orders WHERE store_id=?", (store_id,))
    lines = query_one("SELECT COUNT(*) as c FROM order_lines WHERE store_id=?", (store_id,))
    refunds = query_one("SELECT COUNT(*) as c FROM order_refunds WHERE store_id=?", (store_id,))
    meta = query_one("SELECT COUNT(*) as c FROM meta_insights WHERE store_id=?", (store_id,))
    periodos = query_one("SELECT COUNT(DISTINCT periodo) as c FROM orders WHERE store_id=?", (store_id,))
    return {
        "orders": orders["c"] if orders else 0,
        "order_lines": lines["c"] if lines else 0,
        "order_refunds": refunds["c"] if refunds else 0,
        "meta_insights": meta["c"] if meta else 0,
        "periodos": periodos["c"] if periodos else 0,
    }


@router.post("/{store_id}/sync")
def sync_periodo(store_id: str, token: str = ""):
    """Sync periodo activo (Shopify + Meta)."""
    store = get_store_or_404(store_id, token)

    results = {}

    # Shopify
    if store["shopify_domain"] and store["shopify_client_id"]:
        try:
            from app.sync_shopify import sync_shopify_periodo
            results["shopify"] = sync_shopify_periodo(store)
        except Exception as e:
            results["shopify"] = {"error": True, "message": str(e)}
    else:
        results["shopify"] = {"skipped": True, "reason": "No Shopify credentials"}

    # Meta
    if store["meta_access_token"] and store["meta_ad_account_id"]:
        try:
            from app.sync_meta import sync_meta_periodo
            results["meta"] = sync_meta_periodo(store)
        except Exception as e:
            results["meta"] = {"error": True, "message": str(e)}
    else:
        results["meta"] = {"skipped": True, "reason": "No Meta credentials"}

    return {"status": "ok", "periodo": store["periodo_activo"], "results": results}


@router.post("/{store_id}/sync/shopify")
def sync_shopify_only(store_id: str, token: str = ""):
    """Sync solo Shopify periodo activo."""
    store = get_store_or_404(store_id, token)
    if store["shopify_domain"] and store["shopify_client_id"]:
        try:
            from app.sync_shopify import sync_shopify_periodo
            return {"status": "ok", "periodo": store["periodo_activo"], "shopify": sync_shopify_periodo(store)}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    return {"status": "skipped", "reason": "No Shopify credentials"}


@router.post("/{store_id}/sync/meta")
def sync_meta_only(store_id: str, token: str = ""):
    """Sync solo Meta periodo activo."""
    store = get_store_or_404(store_id, token)
    if store["meta_access_token"] and store["meta_ad_account_id"]:
        try:
            from app.sync_meta import sync_meta_periodo
            return {"status": "ok", "periodo": store["periodo_activo"], "meta": sync_meta_periodo(store)}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    return {"status": "skipped", "reason": "No Meta credentials"}


@router.post("/{store_id}/sync/full")
def sync_full(store_id: str, token: str = "", source: str = ""):
    """Sync ultimos 12 meses (Shopify + Meta, o solo uno si source='shopify'|'meta')."""
    store = get_store_or_404(store_id, token)

    results = {}

    if source != "meta":
        if store["shopify_domain"] and store["shopify_client_id"]:
            try:
                from app.sync_shopify import sync_shopify_full
                results["shopify"] = sync_shopify_full(store)
            except Exception as e:
                results["shopify"] = {"error": True, "message": str(e)}
        else:
            results["shopify"] = {"skipped": True, "reason": "No Shopify credentials"}

    if source != "shopify":
        if store["meta_access_token"] and store["meta_ad_account_id"]:
            try:
                from app.sync_meta import sync_meta_full
                results["meta"] = sync_meta_full(store)
            except Exception as e:
                results["meta"] = {"error": True, "message": str(e)}
        else:
            results["meta"] = {"skipped": True, "reason": "No Meta credentials"}

    return {"status": "ok", "results": results}
