from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
import device_client
from models import friendly_name as _friendly_name


app = FastAPI()

_STATIC_DIR = Path(__file__).parent / "static"

def to_gb(val_str: str) -> str:
    try:
        return f"{int(val_str)/1e9:.2f}"
    except Exception:
        return "N/A"

def adjust_battery_health(pct: float) -> float:
    """Clamp reported battery health to 100, then subtract by the value created by up to 2% as you approach 85%."""
    pct = min(pct, 100)
    t = max(0, min(1, (pct - 85) / (100 / 85)))
    subtractor = (1 - t) * 2 # 2 is the value being interpolated between
    if pct < 100:
        pct -= subtractor
    return pct

def _compute_battery_health(smart_battery: dict) -> str:
    try:
        # Newer chip generations nest these under "BatteryData"; older ones
        # have them at the top level of the IORegistry entry directly.
        data = smart_battery.get("BatteryData") or smart_battery
        nom = float(data.get("NominalChargeCapacity") or 0)
        des = float(data.get("DesignCapacity")        or 0)
        return f"{adjust_battery_health(nom / des * 100):.1f}" if des > 0 else "N/A"
    except Exception:
        return "N/A"

@app.get("/")
async def root() -> FileResponse:
    return FileResponse(_STATIC_DIR / "index.html")

@app.get("/device")
async def get_device() -> dict:
    info = await device_client.adevice_info()
    if not info or "DeviceName" not in info:
        return {"connected": False}

    device_client.clear_last_error()
    smart_battery = await device_client.aioreg(ioclass="IOPMPowerSource")
    battery_health = _compute_battery_health(smart_battery)
    battery_error = device_client.get_last_error() if not smart_battery else None

    device_client.clear_last_error()
    disk = await device_client.adevice_info("com.apple.disk_usage")
    storage_gb = to_gb(disk.get("TotalDiskCapacity", "N/A"))
    storage_error = device_client.get_last_error() if not disk else None

    product_type = info.get("ProductType", "")
    return {
        "connected":      True,
        "name":           info.get("DeviceName", ""),
        "product_type":   product_type,
        "model_name":     _friendly_name(product_type),
        "serial":         info.get("SerialNumber", ""),
        "ios_version":    info.get("ProductVersion", ""),
        "imei":           info.get("InternationalMobileEquipmentIdentity", ""),
        "color":          info.get("DeviceColor") or None,
        "storage":        storage_gb,
        "storage_error":  storage_error,
        "battery_health": battery_health,
        "battery_error":  battery_error,
    }
