"""
pnl.py — Motor P&L: 32 metricas en Python puro

Portado desde Calc.gs. Cada formula es una linea de Python.
Reemplaza SUMPRODUCT strings por queries SQL + aritmetica simple.

Nomenclatura chilena:
  - Ventas Brutas = precio pre-descuento (IVA incluido)
  - Facturacion (con IVA) = ventas - descuentos + envios
  - Ingresos Netos (sin IVA) = base para todo el P&L
"""
from app.database import query_one, query


def compute_pnl(store: dict, periodo: str | None = None) -> dict:
    """
    Calcula las 32 metricas del P&L para una tienda y periodo.
    Retorna dict con todas las metricas.
    """
    periodo = periodo or store["periodo_activo"]
    sid = store["id"]
    tax = store["tax_rate"]  # 0.19

    # ── Queries SQL simples ──────────────────────────────────────

    # 1. Ordenes pagadas del periodo
    paid = query_one("""
        SELECT
            COALESCE(SUM(subtotal_price + total_discounts), 0) as ventas_brutas,
            COALESCE(SUM(total_discounts), 0) as descuentos,
            COALESCE(SUM(total_shipping), 0) as ingreso_envios,
            COUNT(*) as num_pedidos,
            COALESCE(SUM(CASE WHEN total_shipping > 0 THEN 1 ELSE 0 END), 0) as pedidos_envio_pagado,
            COALESCE(SUM(CASE WHEN total_shipping = 0 THEN 1 ELSE 0 END), 0) as pedidos_envio_gratis
        FROM orders
        WHERE store_id = ? AND periodo = ? AND financial_status = 'paid'
    """, (sid, periodo))

    # 2. Devoluciones (refunded + partially_refunded)
    refunds = query_one("""
        SELECT COALESCE(SUM(subtotal_price + total_discounts), 0) as devoluciones
        FROM orders
        WHERE store_id = ? AND periodo = ?
          AND financial_status IN ('refunded', 'partially_refunded')
    """, (sid, periodo))

    # 3. Meta ads del periodo
    meta = query_one("""
        SELECT
            COALESCE(SUM(spend), 0) as gasto_ads,
            COALESCE(SUM(purchases), 0) as meta_purchases,
            COALESCE(SUM(purchase_value), 0) as meta_revenue
        FROM meta_insights
        WHERE store_id = ? AND periodo = ?
    """, (sid, periodo))

    # 4. Gastos fijos (recurrentes <= periodo + no-recurrentes = periodo)
    gastos_fijos_row = query_one("""
        SELECT COALESCE(SUM(amount), 0) as total
        FROM fixed_costs
        WHERE store_id = ?
          AND ((recurring = 0 AND periodo = ?)
            OR (recurring = 1 AND periodo <= ?))
    """, (sid, periodo, periodo))

    # 5. Gastos variables del periodo
    gastos_var_row = query_one("""
        SELECT COALESCE(SUM(amount), 0) as total
        FROM variable_costs
        WHERE store_id = ? AND periodo = ?
    """, (sid, periodo))

    # 6. IVA credito fiscal (facturas de compra del periodo)
    iva_credito_row = query_one("""
        SELECT COALESCE(SUM(iva), 0) as total
        FROM purchase_invoices
        WHERE store_id = ? AND periodo = ?
    """, (sid, periodo))

    # 7. Costo de productos (si metodo = por_producto)
    cogs_by_product = 0.0
    if store["cogs_method"] == "por_producto":
        # JOIN order_lines con product_costs por SKU o nombre
        cogs_rows = query("""
            SELECT
                ol.quantity,
                ol.price,
                COALESCE(pc.unit_cost, 0) as unit_cost
            FROM order_lines ol
            LEFT JOIN product_costs pc
                ON pc.store_id = ol.store_id
                AND (pc.sku = ol.item_sku OR pc.product = ol.item_name)
            WHERE ol.store_id = ? AND ol.periodo = ? AND ol.financial_status = 'paid'
        """, (sid, periodo))
        for row in cogs_rows:
            cogs_by_product += row["quantity"] * row["unit_cost"]

    # ── Extraer valores de queries ───────────────────────────────

    ventas_brutas = paid["ventas_brutas"]       # subtotal + discounts de paid
    devoluciones = refunds["devoluciones"]       # de refunded/partially_refunded
    descuentos = abs(paid["descuentos"])         # total_discounts de paid
    ingreso_envios = paid["ingreso_envios"]      # total_shipping de paid
    num_pedidos = paid["num_pedidos"]
    pedidos_envio_pagado = paid["pedidos_envio_pagado"]
    pedidos_envio_gratis = paid["pedidos_envio_gratis"]

    gasto_ads = meta["gasto_ads"]
    meta_purchases = meta["meta_purchases"]
    meta_revenue = meta["meta_revenue"]

    gastos_fijos = gastos_fijos_row["total"]
    gastos_variables = gastos_var_row["total"]
    iva_credito = iva_credito_row["total"]

    # ══════════════════════════════════════════════════════════════
    # SECCION 1: INGRESOS
    # ══════════════════════════════════════════════════════════════

    # Facturacion = Ventas Brutas - Descuentos + Envios Cobrados
    # (B3 solo suma "paid", refunded ya estan excluidos)
    facturacion = ventas_brutas - descuentos + ingreso_envios

    # IVA Debito Fiscal = facturacion * tax / (1 + tax)
    iva_debito = facturacion * tax / (1 + tax) if (1 + tax) != 0 else 0

    # Ingresos Netos (sin IVA) = Facturacion - IVA
    ingresos_netos = facturacion - iva_debito

    # ══════════════════════════════════════════════════════════════
    # SECCION 2: COSTO DE VENTAS
    # ══════════════════════════════════════════════════════════════

    # Costo de Productos
    if store["cogs_method"] == "porcentaje_fijo":
        costo_productos = ingresos_netos * store["cogs_pct"]
    else:
        costo_productos = cogs_by_product

    # Picking y Packing = pedidos * costo / (1 + IVA)
    picking = num_pedidos * store["fulfillment_cost"] / (1 + tax) if (1 + tax) != 0 else 0

    # Costo Total Envios (neto)
    # = (ingreso_envios + pedidos_gratis * costo_unitario) / (1 + IVA)
    costo_envios = (ingreso_envios + pedidos_envio_gratis * store["costo_envio_gratis"]) / (1 + tax) if (1 + tax) != 0 else 0

    # Margen Bruto = Ingresos Netos - Costo Productos - Picking - Envios
    margen_bruto = ingresos_netos - costo_productos - picking - costo_envios

    # Margen Bruto %
    margen_bruto_pct = margen_bruto / ingresos_netos if ingresos_netos != 0 else 0

    # ══════════════════════════════════════════════════════════════
    # SECCION 3: GASTOS OPERACIONALES
    # ══════════════════════════════════════════════════════════════

    # Comisiones sobre facturacion CON IVA
    comision_pasarela = facturacion * store["comision_pasarela"]
    comision_shopify = facturacion * store["comision_shopify"]

    # Total Gastos Operacionales
    total_gastos_op = gasto_ads + comision_pasarela + comision_shopify + gastos_fijos + gastos_variables

    # ══════════════════════════════════════════════════════════════
    # SECCION 4: RESULTADO
    # ══════════════════════════════════════════════════════════════

    utilidad_op = margen_bruto - total_gastos_op
    utilidad_op_pct = utilidad_op / ingresos_netos if ingresos_netos != 0 else 0

    # ══════════════════════════════════════════════════════════════
    # SECCION 5: METRICAS DE CANAL
    # ══════════════════════════════════════════════════════════════

    # MER = Facturacion CON IVA / Gasto Ads
    mer = facturacion / gasto_ads if gasto_ads != 0 else 0

    # CPA = Gasto Ads / Purchases atribuidas
    cpa = gasto_ads / meta_purchases if meta_purchases != 0 else 0

    # Break-even ROAS = Facturacion / (Margen Bruto - Gastos no-ads)
    gastos_no_ads = comision_pasarela + comision_shopify + gastos_fijos + gastos_variables
    breakeven_roas = facturacion / (margen_bruto - gastos_no_ads) if (margen_bruto - gastos_no_ads) > 0 else 0

    # ROAS Meta = Ingresos Atribuidos Meta / Gasto Ads
    roas_meta = meta_revenue / gasto_ads if gasto_ads != 0 else 0

    # ══════════════════════════════════════════════════════════════
    # SECCION 6: ANALISIS ENVIOS E IVA
    # ══════════════════════════════════════════════════════════════

    costo_promedio_envio = ingreso_envios / pedidos_envio_pagado if pedidos_envio_pagado != 0 else 0

    # IVA Neto a Pagar = Debito - Credito
    iva_neto = iva_debito - iva_credito

    # ══════════════════════════════════════════════════════════════
    # Resultado: 32 metricas
    # ══════════════════════════════════════════════════════════════

    return {
        # Seccion 1: Ingresos
        "periodo": periodo,
        "ventas_brutas": round(ventas_brutas, 2),
        "devoluciones": round(devoluciones, 2),
        "descuentos": round(descuentos, 2),
        "facturacion": round(facturacion, 2),
        "iva_debito": round(iva_debito, 2),
        "ingresos_netos": round(ingresos_netos, 2),

        # Seccion 2: Costo de Ventas
        "costo_productos": round(costo_productos, 2),
        "picking_packing": round(picking, 2),
        "costo_envios": round(costo_envios, 2),
        "margen_bruto": round(margen_bruto, 2),
        "margen_bruto_pct": round(margen_bruto_pct, 4),

        # Seccion 3: Gastos Operacionales
        "gasto_ads": round(gasto_ads, 2),
        "comision_pasarela": round(comision_pasarela, 2),
        "comision_shopify": round(comision_shopify, 2),
        "gastos_fijos": round(gastos_fijos, 2),
        "gastos_variables": round(gastos_variables, 2),
        "total_gastos_op": round(total_gastos_op, 2),

        # Seccion 4: Resultado
        "utilidad_op": round(utilidad_op, 2),
        "utilidad_op_pct": round(utilidad_op_pct, 4),

        # Seccion 5: Metricas de Canal
        "mer": round(mer, 2),
        "cpa": round(cpa, 2),
        "breakeven_roas": round(breakeven_roas, 2),
        "ingresos_meta": round(meta_revenue, 2),
        "roas_meta": round(roas_meta, 2),

        # Seccion 6: Analisis Envios e IVA
        "ingreso_envios": round(ingreso_envios, 2),
        "pedidos_envio_pagado": pedidos_envio_pagado,
        "pedidos_envio_gratis": pedidos_envio_gratis,
        "costo_promedio_envio": round(costo_promedio_envio, 2),
        "iva_debito_proy": round(iva_debito, 2),
        "iva_credito": round(iva_credito, 2),
        "iva_neto": round(iva_neto, 2),
    }


def compute_historico(store: dict, periodos: list[str] | None = None) -> list[dict]:
    """
    Calcula P&L para multiples periodos (para graficos historicos).
    Si periodos es None, calcula ultimos 12 meses.
    Retorna lista de dicts con las metricas clave por periodo.
    """
    if periodos is None:
        from app.sync_shopify import generate_period_range
        from datetime import date, timedelta

        today = date.today()
        start = date(today.year, today.month, 1)
        for _ in range(11):
            start = (start - timedelta(days=1)).replace(day=1)
        start_period = f"{start.year}-{start.month:02d}"
        periodos = generate_period_range(start_period, store["periodo_activo"])

    result = []
    for p in periodos:
        pnl = compute_pnl(store, p)
        result.append({
            "periodo": p,
            "ventasTotales": pnl["ventas_brutas"],
            "ingresosNetos": pnl["ingresos_netos"],
            "costoProductos": pnl["costo_productos"],
            "margenBruto": pnl["margen_bruto"],
            "margenBrutoPct": pnl["margen_bruto_pct"],
            "gastoAds": pnl["gasto_ads"],
            "mer": pnl["mer"],
            "roasMeta": pnl["roas_meta"],
            "cpa": pnl["cpa"],
            "totalGastosOp": pnl["total_gastos_op"],
            "utilidadOp": pnl["utilidad_op"],
            "utilidadOpPct": pnl["utilidad_op_pct"],
        })
    return result
