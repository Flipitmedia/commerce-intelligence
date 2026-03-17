"""
api.py — JSON API: /api/{store}/data, /api/{store}/sync
Mismo contrato que doGet de WebApp.gs
"""
from datetime import datetime
from fastapi import APIRouter, HTTPException, BackgroundTasks
from app.database import query_one
from app.pnl import compute_pnl, compute_historico

router = APIRouter(prefix="/api", tags=["api"])

CI_VERSION = "1.0.0"


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
        if target == 0:
            return "OK"
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
            "status": "OK",
        },
    ]


def build_pnl_cascade(pnl: dict) -> list[dict]:
    """
    Genera P&L en formato cascada para el dashboard.
    Replica las 18 filas de DASHBOARD A18:C35.
    """
    ingresos = pnl["ingresos_netos"] if pnl["ingresos_netos"] != 0 else 1

    def pct(val):
        return round(val / ingresos, 4) if ingresos != 0 else 0

    # Concept names MUST match dashboard PNL_STEPS keys exactly
    return [
        {"concept": "(+) Ventas Brutas (IVA incl.)", "amount": pnl["ventas_brutas"], "pctOfSales": pct(pnl["ventas_brutas"])},
        {"concept": "(-) Descuentos", "amount": -pnl["descuentos"], "pctOfSales": pct(-pnl["descuentos"])},
        {"concept": "(+) Recaudado en Envios", "amount": pnl["ingreso_envios"], "pctOfSales": pct(pnl["ingreso_envios"])},
        {"concept": "(=) Facturacion (con IVA)", "amount": pnl["facturacion"], "pctOfSales": pct(pnl["facturacion"])},
        {"concept": "(-) IVA Debito Fiscal", "amount": -pnl["iva_debito"], "pctOfSales": pct(-pnl["iva_debito"])},
        {"concept": "(=) Ingresos Netos (sin IVA)", "amount": pnl["ingresos_netos"], "pctOfSales": 1.0},
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


# ── Endpoints ───────────────────────────────────────────────────

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


@router.post("/{store_id}/sync")
def sync_periodo(store_id: str, token: str = "", background_tasks: BackgroundTasks = None):
    """Sync periodo activo (Shopify + Meta)."""
    store = get_store_or_404(store_id, token)

    results = {}

    # Shopify
    if store["shopify_domain"] and store["shopify_client_id"]:
        from app.sync_shopify import sync_shopify_periodo
        results["shopify"] = sync_shopify_periodo(store)
    else:
        results["shopify"] = {"skipped": True, "reason": "No Shopify credentials"}

    # Meta
    if store["meta_access_token"] and store["meta_ad_account_id"]:
        from app.sync_meta import sync_meta_periodo
        results["meta"] = sync_meta_periodo(store)
    else:
        results["meta"] = {"skipped": True, "reason": "No Meta credentials"}

    return {"status": "ok", "periodo": store["periodo_activo"], "results": results}


@router.post("/{store_id}/sync/full")
def sync_full(store_id: str, token: str = ""):
    """Sync ultimos 12 meses (Shopify + Meta)."""
    store = get_store_or_404(store_id, token)

    results = {}

    if store["shopify_domain"] and store["shopify_client_id"]:
        from app.sync_shopify import sync_shopify_full
        results["shopify"] = sync_shopify_full(store)
    else:
        results["shopify"] = {"skipped": True, "reason": "No Shopify credentials"}

    if store["meta_access_token"] and store["meta_ad_account_id"]:
        from app.sync_meta import sync_meta_full
        results["meta"] = sync_meta_full(store)
    else:
        results["meta"] = {"skipped": True, "reason": "No Meta credentials"}

    return {"status": "ok", "results": results}
