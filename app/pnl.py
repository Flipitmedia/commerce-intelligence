"""
pnl.py v2.4 — Motor P&L con base contable autoritativa

MODELO CONTABLE: Cohorte actualizada
  El P&L se organiza por periodo de CREACION de la orden (cohorte).
  Los refunds se imputan al periodo de la orden original, no al mes del refund.
  Esto responde: "como termino realmente la rentabilidad de las ventas de marzo"
  (incluyendo devoluciones posteriores), NO "cuanto se facturo/devolvio en abril".
  Para cierre mensual por evento economico, se requiere un modelo distinto.

Base contable (v2.2+):
  - facturacion = SUM(current_total_price)  — autoritativa, post-refunds
  - devoluciones = SUM(total_price) - SUM(current_total_price)
  - subtotal_retenido = SUM(current_subtotal_price) — productos post-refunds
  - ingreso_envios_retenido = total_retenido - subtotal_retenido

Cascade visual:
  - ventas_brutas = SUM(subtotal_price + total_discounts) — pre-descuento, IVA incl.
  - descuentos, ingreso_envios, devoluciones — desglose informativo
  - Si cascade != facturacion autoritativa, se agrega fila de ajuste (api.py)

COGS:
  - porcentaje_fijo: % sobre subtotal_retenido / (1+tax) — IVA-inclusive / (1.19)
  - por_producto: suma unit_cost * (qty - refunded_qty) por line_item
  - Ambos metodos revierten COGS en devoluciones
  - Limitacion: no distingue restocked/damaged/discarded — toda devolucion
    revierte costo como si el producto fuera recuperable

Shipping (v2.4):
  - costo_envio_estimado > 0: pedidos_enviados * costo (fulfilled + has_shipping)
  - Fallback: ingreso_envios_despachado (shipping ORIGINAL de fulfilled, no retenido)
    + gratis_fulfilled * costo_gratis. El costo logistico es un hecho fisico:
    ocurre al despachar, no se revierte con el refund del cobro.
  - Pickups (has_shipping=0) excluidos en ambas ramas

Comisiones: floor at 0 (pasarelas no pagan inversa). Aproximacion gerencial,
  no refleja fees fijos por transaccion ni timing real.
KPIs %: None cuando ingresos_netos <= 0

Nomenclatura chilena:
  - Todos los precios Shopify son IVA-incluido (19%)
  - subtotal_price, current_subtotal_price: productos, IVA incluido
  - total_price, current_total_price: productos + shipping, IVA incluido
  - Ingresos Netos = facturacion / 1.19 (base para P&L)
"""
from app.database import query_one, query

# Categorias que se calculan automaticamente en el P&L
AUTO_CALCULATED_CATEGORIES = {"Envios", "Comisiones"}


def compute_pnl(store: dict, periodo: str | None = None) -> dict:
    """
    Calcula las metricas del P&L para una tienda y periodo.
    Retorna dict con todas las metricas.
    """
    periodo = periodo or store["periodo_activo"]
    sid = store["id"]
    tax = store["tax_rate"]  # 0.19

    # ── Queries SQL ────────────────────────────────────────────────

    # 1. Ordenes pagadas + partially_refunded + refunded del periodo
    #    v2.2: Base unificada. total_price/current_total_price son autoritativos.
    #    ventas_brutas/descuentos/ingreso_envios son solo para desglose visual.
    paid = query_one("""
        SELECT
            -- Desglose visual (cascade)
            COALESCE(SUM(subtotal_price + total_discounts), 0) as ventas_brutas,
            COALESCE(SUM(total_discounts), 0) as descuentos,
            COALESCE(SUM(total_shipping), 0) as ingreso_envios,
            COUNT(*) as num_pedidos,
            -- Base autoritativa (calculo real)
            COALESCE(SUM(total_price), 0) as total_cobrado,
            COALESCE(SUM(current_total_price), 0) as total_retenido,
            COALESCE(SUM(current_subtotal_price), 0) as subtotal_retenido,
            -- Shipping/fulfillment
            COALESCE(SUM(CASE WHEN has_shipping = 1 AND total_shipping > 0 THEN 1 ELSE 0 END), 0) as pedidos_envio_pagado,
            COALESCE(SUM(CASE WHEN has_shipping = 1 AND total_shipping = 0 THEN 1 ELSE 0 END), 0) as pedidos_envio_gratis,
            COALESCE(SUM(CASE WHEN has_shipping = 1 AND total_shipping = 0 AND fulfillment_status IN ('fulfilled', 'partial') THEN 1 ELSE 0 END), 0) as pedidos_envio_gratis_fulfilled,
            COALESCE(SUM(CASE WHEN has_shipping = 1 AND fulfillment_status IN ('fulfilled', 'partial') THEN 1 ELSE 0 END), 0) as pedidos_enviados,
            -- v2.4: shipping original de pedidos realmente despachados (para fallback de costo)
            COALESCE(SUM(CASE WHEN has_shipping = 1 AND fulfillment_status IN ('fulfilled', 'partial') THEN total_shipping ELSE 0 END), 0) as ingreso_envios_despachado,
            COALESCE(SUM(CASE WHEN fulfillment_status IN ('fulfilled', 'partial') THEN 1 ELSE 0 END), 0) as pedidos_fulfilled
        FROM orders
        WHERE store_id = ? AND periodo = ?
          AND financial_status IN ('paid', 'partially_refunded', 'refunded')
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

    # 4. Gastos fijos (v2: respeta tax_included)
    gastos_fijos_row = query_one("""
        SELECT COALESCE(SUM(
            CASE WHEN tax_included = 1 THEN amount / (1.0 + ?) ELSE amount END
        ), 0) as total
        FROM fixed_costs
        WHERE store_id = ?
          AND ((recurring = 0 AND periodo = ?)
            OR (recurring = 1 AND periodo <= ?))
    """, (tax, sid, periodo, periodo))

    # 5. Gastos variables (v2: excluye categorias auto-calculadas, respeta tax_included)
    gastos_var_row = query_one("""
        SELECT COALESCE(SUM(
            CASE WHEN tax_included = 1 THEN amount / (1.0 + ?) ELSE amount END
        ), 0) as total
        FROM variable_costs
        WHERE store_id = ? AND periodo = ?
          AND category NOT IN ('Envios', 'Comisiones')
    """, (tax, sid, periodo))

    # 6. IVA credito fiscal (facturas de compra del periodo)
    iva_credito_row = query_one("""
        SELECT COALESCE(SUM(iva), 0) as total
        FROM purchase_invoices
        WHERE store_id = ? AND periodo = ?
    """, (sid, periodo))

    # 6b. Facturas que impactan P&L (v2)
    invoice_costs = query_one("""
        SELECT
            COALESCE(SUM(CASE WHEN pnl_category = 'cogs' THEN net_amount ELSE 0 END), 0) as cogs_invoices,
            COALESCE(SUM(CASE WHEN pnl_category = 'fixed' THEN net_amount ELSE 0 END), 0) as fixed_invoices,
            COALESCE(SUM(CASE WHEN pnl_category = 'variable' THEN net_amount ELSE 0 END), 0) as variable_invoices
        FROM purchase_invoices
        WHERE store_id = ? AND periodo = ? AND impacts_pnl = 1
    """, (sid, periodo))

    # 7. Costo de productos (v2: incluye partially_refunded, resta qty devueltas,
    #    JOIN con prioridad SKU > nombre, scoped por order_id,
    #    ROW_NUMBER evita duplicados de product_costs)
    cogs_by_product = 0.0
    if store["cogs_method"] == "por_producto":
        cogs_rows = query("""
            SELECT
                ol.quantity,
                COALESCE(rq.refunded_qty, 0) as refunded_qty,
                ol.price,
                COALESCE(pc_sku.unit_cost, pc_name.unit_cost, 0) as unit_cost
            FROM order_lines ol
            LEFT JOIN (
                SELECT order_id, line_item_id, store_id, SUM(quantity) as refunded_qty
                FROM order_refunds
                WHERE store_id = ? AND line_item_id != ''
                GROUP BY order_id, line_item_id, store_id
            ) rq ON rq.line_item_id = ol.line_item_id
                AND rq.order_id = ol.order_id
                AND rq.store_id = ol.store_id
                AND ol.line_item_id != ''
            LEFT JOIN (
                SELECT store_id, sku, unit_cost, id,
                    ROW_NUMBER() OVER (PARTITION BY store_id, sku ORDER BY id) as rn
                FROM product_costs
                WHERE sku != ''
            ) pc_sku ON pc_sku.store_id = ol.store_id
                AND pc_sku.sku = ol.item_sku
                AND ol.item_sku != '' AND pc_sku.rn = 1
            LEFT JOIN (
                SELECT store_id, product, unit_cost,
                    ROW_NUMBER() OVER (PARTITION BY store_id, product ORDER BY id) as rn
                FROM product_costs
            ) pc_name ON pc_name.store_id = ol.store_id
                AND pc_name.product = ol.item_name
                AND pc_sku.id IS NULL AND pc_name.rn = 1
            WHERE ol.store_id = ? AND ol.periodo = ?
              AND ol.financial_status IN ('paid', 'partially_refunded', 'refunded')
        """, (sid, sid, periodo))
        for row in cogs_rows:
            net_qty = max(0, row["quantity"] - row["refunded_qty"])
            cogs_by_product += net_qty * row["unit_cost"]

    # ── Extraer valores ────────────────────────────────────────────

    # Desglose visual (para cascade)
    ventas_brutas = paid["ventas_brutas"]
    descuentos = abs(paid["descuentos"])
    ingreso_envios = paid["ingreso_envios"]
    num_pedidos = paid["num_pedidos"]

    # Base autoritativa — evita mezcla de bases
    total_cobrado = paid["total_cobrado"]       # SUM(total_price)
    total_retenido = paid["total_retenido"]     # SUM(current_total_price)
    subtotal_retenido = paid["subtotal_retenido"]  # SUM(current_subtotal_price)

    # Fallback: si current_total_price no fue sincronizado (data pre-v2),
    # los campos current_* quedan en 0 por DEFAULT. Detectar y usar total_price.
    if total_retenido == 0 and total_cobrado > 0:
        total_retenido = total_cobrado
        subtotal_retenido = paid["ventas_brutas"] - paid["descuentos"]  # subtotal_price

    devoluciones = total_cobrado - total_retenido   # refund completo (productos+shipping+tax)

    # Conteos de envio/fulfillment
    pedidos_envio_pagado = paid["pedidos_envio_pagado"]
    pedidos_envio_gratis = paid["pedidos_envio_gratis"]
    pedidos_enviados = paid["pedidos_enviados"]    # has_shipping=1 AND fulfilled
    pedidos_fulfilled = paid["pedidos_fulfilled"]

    gasto_ads = meta["gasto_ads"]
    meta_purchases = meta["meta_purchases"]
    meta_revenue = meta["meta_revenue"]

    gastos_fijos = gastos_fijos_row["total"] + invoice_costs["fixed_invoices"]
    gastos_variables = gastos_var_row["total"] + invoice_costs["variable_invoices"]
    iva_credito = iva_credito_row["total"]

    # ══════════════════════════════════════════════════════════════
    # SECCION 1: INGRESOS
    # ══════════════════════════════════════════════════════════════

    # v2.2: Facturacion = total_retenido (autoritativo, no reconstruido)
    # Equivale a: ventas_brutas - descuentos + envios - devoluciones
    # pero garantiza consistencia de base contable.
    facturacion = total_retenido

    # IVA Debito Fiscal Proyectado
    iva_debito = facturacion * tax / (1 + tax) if (1 + tax) != 0 else 0

    # Ingresos Netos (sin IVA)
    ingresos_netos = facturacion - iva_debito

    # ══════════════════════════════════════════════════════════════
    # SECCION 2: COSTO DE VENTAS
    # ══════════════════════════════════════════════════════════════

    # v2.2: Ingreso envios retenido = total_retenido - subtotal_retenido
    # (shipping efectivamente cobrado despues de refunds)
    ingreso_envios_retenido = total_retenido - subtotal_retenido
    ingreso_envios_neto = ingreso_envios_retenido / (1 + tax) if (1 + tax) != 0 else 0

    # Costo de Productos
    if store["cogs_method"] == "porcentaje_fijo":
        # v2.2: usar subtotal_retenido (productos efectivamente retenidos, sin shipping)
        # Asi COGS se revierte cuando hay devoluciones, y la base excluye shipping real.
        ingresos_productos_netos = subtotal_retenido / (1 + tax) if (1 + tax) != 0 else 0
        costo_productos = ingresos_productos_netos * store["cogs_pct"]
    else:
        costo_productos = cogs_by_product

    # Sumar costos de facturas categorizadas como COGS
    costo_productos += invoice_costs["cogs_invoices"]

    # Picking y Packing = pedidos fulfilled * costo / (1 + IVA)
    # (v2.1: solo ordenes con fulfillment real, no canceladas antes de despacho)
    picking = pedidos_fulfilled * store["fulfillment_cost"] / (1 + tax) if (1 + tax) != 0 else 0

    # Costo Total Envios
    # v2.4: El costo logistico es un hecho fisico — ocurre cuando se despacha,
    # independiente de si el shipping se reembolsa despues.
    costo_envio_estimado = store.get("costo_envio_estimado", 0)
    if costo_envio_estimado > 0:
        # Rama preferida: costo fijo por pedido despachado
        costo_envios = pedidos_enviados * costo_envio_estimado / (1 + tax)
    else:
        # Fallback: usa shipping ORIGINAL de pedidos fulfilled como proxy de costo.
        # ingreso_envios_despachado = SUM(total_shipping) WHERE fulfilled+has_shipping
        # NO usa retenido, porque el costo ocurrio aunque se haya reembolsado el cobro.
        ingreso_envios_despachado = paid["ingreso_envios_despachado"]
        envios_gratis_fulfilled = paid["pedidos_envio_gratis_fulfilled"]
        costo_envios = (ingreso_envios_despachado + envios_gratis_fulfilled * store["costo_envio_gratis"]) / (1 + tax) if (1 + tax) != 0 else 0

    # Margen Bruto
    margen_bruto = ingresos_netos - costo_productos - picking - costo_envios
    margen_bruto_pct = margen_bruto / ingresos_netos if ingresos_netos > 0 else None

    # ══════════════════════════════════════════════════════════════
    # SECCION 3: GASTOS OPERACIONALES
    # ══════════════════════════════════════════════════════════════

    # Comisiones sobre facturacion CON IVA (v2.1: floor at 0, pasarelas no pagan inversa)
    comision_pasarela = max(0, facturacion * store["comision_pasarela"])
    comision_shopify = max(0, facturacion * store["comision_shopify"])

    total_gastos_op = gasto_ads + comision_pasarela + comision_shopify + gastos_fijos + gastos_variables

    # ══════════════════════════════════════════════════════════════
    # SECCION 4: RESULTADO
    # ══════════════════════════════════════════════════════════════

    utilidad_op = margen_bruto - total_gastos_op
    utilidad_op_pct = utilidad_op / ingresos_netos if ingresos_netos > 0 else None

    # ══════════════════════════════════════════════════════════════
    # SECCION 5: METRICAS DE CANAL
    # ══════════════════════════════════════════════════════════════

    mer = facturacion / gasto_ads if gasto_ads != 0 else 0
    cpa = gasto_ads / meta_purchases if meta_purchases != 0 else 0

    # Break-even ROAS (v2: None si denominador <= 0)
    gastos_no_ads = comision_pasarela + comision_shopify + gastos_fijos + gastos_variables
    denominator = margen_bruto - gastos_no_ads
    breakeven_roas = facturacion / denominator if denominator > 0 else None

    roas_meta = meta_revenue / gasto_ads if gasto_ads != 0 else 0

    # ══════════════════════════════════════════════════════════════
    # SECCION 6: ANALISIS ENVIOS E IVA
    # ══════════════════════════════════════════════════════════════

    costo_promedio_envio = costo_envios / pedidos_enviados if pedidos_enviados > 0 else 0

    # IVA Neto Proyectado (v2: renombrado, no es IVA contable real)
    iva_neto = iva_debito - iva_credito

    # ══════════════════════════════════════════════════════════════
    # Resultado
    # ══════════════════════════════════════════════════════════════

    return {
        # Seccion 1: Ingresos
        "periodo": periodo,
        "ventas_brutas": round(ventas_brutas),
        "devoluciones": round(devoluciones),
        "descuentos": round(descuentos),
        "facturacion": round(facturacion),
        "iva_debito": round(iva_debito),
        "ingresos_netos": round(ingresos_netos),

        # Seccion 2: Costo de Ventas
        "costo_productos": round(costo_productos),
        "picking_packing": round(picking),
        "costo_envios": round(costo_envios),
        "margen_bruto": round(margen_bruto),
        "margen_bruto_pct": round(margen_bruto_pct, 4) if margen_bruto_pct is not None else None,

        # Seccion 3: Gastos Operacionales
        "gasto_ads": round(gasto_ads),
        "comision_pasarela": round(comision_pasarela),
        "comision_shopify": round(comision_shopify),
        "gastos_fijos": round(gastos_fijos),
        "gastos_variables": round(gastos_variables),
        "total_gastos_op": round(total_gastos_op),

        # Seccion 4: Resultado
        "utilidad_op": round(utilidad_op),
        "utilidad_op_pct": round(utilidad_op_pct, 4) if utilidad_op_pct is not None else None,

        # Seccion 5: Metricas de Canal
        "mer": round(mer, 2),
        "cpa": round(cpa),
        "breakeven_roas": round(breakeven_roas, 2) if breakeven_roas is not None else None,
        "ingresos_meta": round(meta_revenue),
        "roas_meta": round(roas_meta, 2),

        # Seccion 6: Analisis Envios e IVA
        "total_cobrado": round(total_cobrado),
        "ingreso_envios": round(ingreso_envios),
        "ingreso_envios_retenido": round(ingreso_envios_retenido),
        "pedidos_envio_pagado": pedidos_envio_pagado,
        "pedidos_envio_gratis": pedidos_envio_gratis,
        "costo_promedio_envio": round(costo_promedio_envio),
        "iva_debito_proy": round(iva_debito),
        "iva_credito": round(iva_credito),
        "iva_neto_proy": round(iva_neto),
    }


def compute_historico(store: dict, periodos: list[str] | None = None) -> list[dict]:
    """
    Calcula P&L para multiples periodos (para graficos historicos).
    Si periodos es None, calcula ultimos 12 meses.
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
            "descuentos": pnl["descuentos"],
            "ingresoEnvios": pnl["ingreso_envios"],
            "devoluciones": pnl["devoluciones"],
            "ajusteReconciliacion": pnl.get("ajuste_reconciliacion", 0),
            "facturacion": pnl["facturacion"],
            "iva": pnl["iva_debito"],
            "ingresosNetos": pnl["ingresos_netos"],
            "costoProductos": pnl["costo_productos"],
            "picking": pnl["picking_packing"],
            "costoEnvios": pnl["costo_envios"],
            "margenBruto": pnl["margen_bruto"],
            "margenBrutoPct": pnl["margen_bruto_pct"],
            "gastoAds": pnl["gasto_ads"],
            "comisionPasarela": pnl["comision_pasarela"],
            "comisionShopify": pnl["comision_shopify"],
            "gastosFijos": pnl["gastos_fijos"],
            "gastosVariables": pnl["gastos_variables"],
            "totalGastosOp": pnl["total_gastos_op"],
            "utilidadOp": pnl["utilidad_op"],
            "utilidadOpPct": pnl["utilidad_op_pct"],
            "mer": pnl["mer"],
            "roasMeta": pnl["roas_meta"],
            "cpa": pnl["cpa"],
        })
    return result
