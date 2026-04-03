"""
SAP B1 Service Layer → Supabase
Sincroniza Items, Stock por Almacén y Números de Serie/Lote.
Pagina de 20 en 20 automáticamente.

Variables de entorno requeridas:
    SAP_BASE_URL    = https://TU_SERVIDOR:50000/b1s/v1
    SAP_COMPANY_DB  = TU_BASE_SAP
    SAP_USER        = manager
    SAP_PASSWORD    = tu_password
    SUPABASE_URL    = https://xxxx.supabase.co
    SUPABASE_KEY    = tu_anon_key
"""

import os
import requests
import logging
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Configuración ──────────────────────────────────────────────────────────────
SAP_BASE_URL   = os.environ["SAP_BASE_URL"].rstrip("/")
SAP_COMPANY_DB = os.environ["SAP_COMPANY_DB"]
SAP_USER       = os.environ["SAP_USER"]
SAP_PASSWORD   = os.environ["SAP_PASSWORD"]

SUPABASE_URL   = os.environ.get("SUPABASE_URL", "")
log.info(f"SUPABASE_URL recibida: '{SUPABASE_URL[:20] if SUPABASE_URL else 'VACIA'}'")
SUPABASE_KEY   = os.environ["SUPABASE_KEY"]

PAGE_SIZE  = 20
VERIFY_SSL = False

# ── Sesión SAP ─────────────────────────────────────────────────────────────────
session = requests.Session()
session.verify = VERIFY_SSL

if not VERIFY_SSL:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def sap_login():
    r = session.post(f"{SAP_BASE_URL}/Login", json={
        "CompanyDB": SAP_COMPANY_DB,
        "UserName":  SAP_USER,
        "Password":  SAP_PASSWORD,
    }, timeout=30)
    r.raise_for_status()
    session.cookies.set("B1SESSION", r.json()["SessionId"])
    log.info("Sesion SAP abierta ✓")


def sap_logout():
    try:
        session.post(f"{SAP_BASE_URL}/Logout", timeout=10)
        log.info("Sesion SAP cerrada ✓")
    except Exception:
        pass


# ── Fetch paginado genérico ────────────────────────────────────────────────────
def fetch_all(endpoint: str, page_size: int = 20) -> list[dict]:
    """Pagina de 20 en 20 hasta traer todos los registros del endpoint."""
    all_records = []
    skip = 0

    while True:
        r = session.get(f"{SAP_BASE_URL}/{endpoint}", params={
            "$top":  page_size,
            "$skip": skip,
        }, timeout=60)
        r.raise_for_status()
        page = r.json().get("value", [])

        if not page:
            break

        all_records.extend(page)
        log.info(f"  [{endpoint}] pagina skip={skip} → {len(page)} registros | total: {len(all_records)}")

        if len(page) < page_size:
            break

        skip += page_size

    log.info(f"Total {endpoint}: {len(all_records)} ✓")
    return all_records


# ── Mapeos ─────────────────────────────────────────────────────────────────────
def map_item(item: dict) -> dict:
    return {
        "item_code":      item.get("ItemCode"),
        "item_name":      item.get("ItemName"),
        "item_type":      item.get("ItemType"),
        "item_group":     item.get("ItemsGroupName") or item.get("ItemGroupCode"),
        "uom":            item.get("InventoryUOM"),
        "purchase_uom":   item.get("PurchaseUnit"),
        "sales_uom":      item.get("SalesUnit"),
        "price":          item.get("LastPurchasePrice"),
        "cost":           item.get("AvgStdPrice"),
        "active":         item.get("Valid") == "tYES",
        "manage_serial":  item.get("ManageSerialNumbers"),
        "manage_batch":   item.get("ManageBatchNumbers"),
        "synced_at":      datetime.utcnow().isoformat(),
    }


def map_warehouse(item_code: str, wh: dict) -> dict:
    return {
        "item_code":      item_code,
        "warehouse_code": wh.get("WarehouseCode"),
        "warehouse_name": wh.get("WarehouseCode"),  # SAP no incluye nombre aquí
        "in_stock":       wh.get("InStock", 0),
        "committed":      wh.get("Committed", 0),
        "ordered":        wh.get("Ordered", 0),
        "available":      (wh.get("InStock") or 0) - (wh.get("Committed") or 0),
        "synced_at":      datetime.utcnow().isoformat(),
    }


def parse_date(val):
    if not val:
        return None
    try:
        return datetime.fromisoformat(val.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return None


def map_serial(item_code: str, sn: dict) -> dict:
    return {
        "item_code":        item_code,
        "serial_number":    sn.get("SerialNumber") or sn.get("InternalSerialNumber"),
        "batch_number":     sn.get("BatchNumber"),
        "warehouse_code":   sn.get("WarehouseCode"),
        "status":           sn.get("Status"),
        "expiry_date":      parse_date(sn.get("ExpiryDate")),
        "manufacture_date": parse_date(sn.get("ManufacturingDate")),
        "synced_at":        datetime.utcnow().isoformat(),
    }


# ── Upsert helpers ─────────────────────────────────────────────────────────────
def upsert_record(sb: Client, table: str, record: dict, conflict_col: str):
    """Upsert de un solo registro — actualiza si existe, inserta si no."""
    try:
        val = record.get(conflict_col)
        if val is None:
            sb.table(table).insert(record).execute()
            return "inserted"

        existing = sb.table(table).select("id").eq(conflict_col, val).execute()
        if existing.data:
            sb.table(table).update(record).eq(conflict_col, val).execute()
            return "updated"
        else:
            sb.table(table).insert(record).execute()
            return "inserted"
    except Exception as e:
        log.warning(f"Error en {table} [{conflict_col}={record.get(conflict_col)}]: {str(e)[:120]}")
        return "error"


def upsert_pair(sb: Client, table: str, record: dict, col1: str, col2: str):
    """Upsert por par de columnas (ej. item_code + warehouse_code)."""
    try:
        existing = (
            sb.table(table)
            .select("id")
            .eq(col1, record[col1])
            .eq(col2, record[col2])
            .execute()
        )
        if existing.data:
            sb.table(table).update(record).eq(col1, record[col1]).eq(col2, record[col2]).execute()
            return "updated"
        else:
            sb.table(table).insert(record).execute()
            return "inserted"
    except Exception as e:
        log.warning(f"Error en {table} [{col1}={record.get(col1)},{col2}={record.get(col2)}]: {str(e)[:120]}")
        return "error"


# ── Sincronización principal ───────────────────────────────────────────────────
def sync_items(sb: Client):
    log.info("--- Sincronizando Items ---")
    items = fetch_all("Items", PAGE_SIZE)

    stats = {"inserted": 0, "updated": 0, "error": 0}
    wh_stats = {"inserted": 0, "updated": 0, "error": 0}

    for item in items:
        item_code = item.get("ItemCode")
        if not item_code:
            continue

        # 1. Upsert item maestro
        result = upsert_record(sb, "sap_items", map_item(item), "item_code")
        stats[result] = stats.get(result, 0) + 1

        # 2. Stock por almacén (viene dentro del item como ItemWarehouseInfoCollection)
        warehouses = item.get("ItemWarehouseInfoCollection", [])
        for wh in warehouses:
            wh_record = map_warehouse(item_code, wh)
            if not wh_record["warehouse_code"]:
                continue
            r = upsert_pair(sb, "sap_item_warehouse", wh_record, "item_code", "warehouse_code")
            wh_stats[r] = wh_stats.get(r, 0) + 1

    log.info(f"Items → insertados: {stats['inserted']} | actualizados: {stats['updated']} | errores: {stats['error']}")
    log.info(f"Almacenes → insertados: {wh_stats['inserted']} | actualizados: {wh_stats['updated']} | errores: {wh_stats['error']}")


def sync_serials(sb: Client):
    log.info("--- Sincronizando Numeros de Serie ---")
    serials = fetch_all("SerialNumberDetails", PAGE_SIZE)

    stats = {"inserted": 0, "updated": 0, "error": 0}
    for sn in serials:
        item_code     = sn.get("ItemCode")
        serial_number = sn.get("SerialNumber") or sn.get("InternalSerialNumber")
        if not item_code or not serial_number:
            continue

        record = map_serial(item_code, sn)
        r = upsert_pair(sb, "sap_item_serial", record, "item_code", "serial_number")
        stats[r] = stats.get(r, 0) + 1

    log.info(f"Series → insertados: {stats['inserted']} | actualizados: {stats['updated']} | errores: {stats['error']}")


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    log.info("=== Inicio sincronizacion SAP Items → Supabase ===")

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    sap_login()

    try:
        sync_items(sb)
        sync_serials(sb)
    finally:
        sap_logout()

    log.info("=== Sincronizacion completada ===")


if __name__ == "__main__":
    main()
