# Commerce Intelligence

## Descripcion
Sistema de BI para tiendas Shopify. Backend FastAPI + SQLite, dashboard HTML (Chart.js + Tailwind).

## Stack
- **Backend**: Python 3.12, FastAPI, SQLite, Jinja2
- **Frontend admin**: Tailwind CSS (CDN), Material Symbols, HTMX
- **Dashboard**: HTML standalone, Chart.js, Tailwind dark theme
- **Deploy**: Railway (Dockerfile), volumen persistente en `/app/data`
- **Repo**: github.com/Flipitmedia/commerce-intelligence

## Estructura
```
app/
  main.py          — Entry point FastAPI
  config.py        — Settings (.env)
  database.py      — SQLite helpers
  models.py        — 8 tablas + seed
  pnl.py           — Motor P&L (32 metricas)
  sync_shopify.py  — Shopify API (OAuth + cursor pagination)
  sync_meta.py     — Meta Marketing API
  routes/
    api.py         — JSON API (/api/{store}/data, sync)
    admin.py       — Admin UI CRUD (config, costos, gastos, facturas, sync)
  templates/       — Jinja2 (base, config, sync, costs, fixed/variable costs, invoices)
dashboard/
  index.html       — Dashboard web (consume JSON API)
```

## URLs de produccion
- Root: https://commerce-intelligence-production-ecd7.up.railway.app/
- Los tokens se generan al crear la DB. Consultar root `/` para obtenerlos.

## Control de versiones (OBLIGATORIO)

Despues de cada cambio funcional exitoso:
```
git add -A && git commit -m "descripcion breve del cambio"
```

Antes de hacer cambios grandes o destructivos (reescrituras, migraciones, refactors), crear rama:
```
git checkout -b nombre-descriptivo
```

NUNCA hacer cambios sin commitear el estado actual primero.

Despues de commitear, hacer push automaticamente:
```
git push origin main
```

## Reglas de desarrollo
- IVA chileno: 19%, incluido en precio. Formula: `iva = facturacion * 0.19 / 1.19`
- Moneda default: CLP (sin decimales)
- Periodo formato: YYYY-MM
- Los endpoints de sync SIEMPRE deben tener try/except y retornar errores como JSON
- El admin usa sidebar lateral con paleta brand (teal)
- No exponer credenciales API en endpoints publicos
