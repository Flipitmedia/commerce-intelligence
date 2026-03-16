"""
admin.py — Admin UI: config, costos, gastos, facturas
Placeholder — se implementa en Fase 3
"""
from fastapi import APIRouter

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/{store_id}")
def admin_home(store_id: str, token: str = ""):
    return {"message": f"Admin UI para {store_id} — en construccion", "store": store_id}
