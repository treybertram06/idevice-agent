"""
Sole owner of all pymobiledevice3 access.

Replaces the libimobiledevice CLI binaries (`ideviceinfo`, `idevicediagnostics`)
that device.py/collect.py used to shell out to. Every device read in the program
goes through this module.

Why a private event loop in a dedicated thread
----------------------------------------------
pymobiledevice3's API is entirely async, and `LockdownService.connect()` caches an
`asyncio.Future` bound to whichever loop first awaited it — so a service object may
only ever be used from that one loop. Running `asyncio.run()` per call would both
violate that and pay a fresh ~150ms lockdown handshake on each of collect()'s ~15
calls, plus emit `RuntimeError: Event loop is closed` from StreamWriter.__del__ on
every teardown. So: one loop, started once, never closed.

That loop backs two calling conventions, because this program has both:
    call(...)   — blocking, for collect.py and cli/ (450+ lines of sync code)
    acall(...)  — awaitable, for the FastAPI handlers in api/server.py

Serialisation
-------------
`LockdownClient._request` is a send-then-recv on a single socket, and
diagnostics_relay accepts one client at a time. An `asyncio.Lock` serialises all
control I/O, and the DiagnosticsService is cached rather than rebuilt per call —
a second concurrent service would be refused by the relay.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import plistlib
import sys
import threading
from typing import Any, Awaitable, Callable, Coroutine, TypeVar

from pymobiledevice3 import usbmux
from pymobiledevice3.exceptions import (
    ConnectionFailedError,
    ConnectionFailedToUsbmuxdError,
    ConnectionTerminatedError,
    DeprecationError,
    DeviceNotFoundError,
    MuxException,
    NoDeviceConnectedError,
    NotPairedError,
    NotTrustedError,
    PasswordRequiredError,
)
from pymobiledevice3.lockdown import LockdownClient, create_using_usbmux
from pymobiledevice3.services.diagnostics import DiagnosticsService

T = TypeVar("T")

# Errors that mean "the connection went away" rather than "the request was bad".
# Worth one invalidate-and-retry; anything else degrades immediately.
TRANSIENT = (
    NoDeviceConnectedError,
    DeviceNotFoundError,
    ConnectionFailedError,
    ConnectionFailedToUsbmuxdError,
    ConnectionTerminatedError,
    MuxException,
    BrokenPipeError,
    ConnectionResetError,
    OSError,
    asyncio.IncompleteReadError,
)

# Errors that mean "plugged in, but we can't talk to it" — no point retrying, and
# the operator needs to do something about it.
_USER_ACTIONABLE = (NotPairedError, NotTrustedError, PasswordRequiredError)

DEFAULT_TIMEOUT = 30.0
IOREG_TIMEOUT   = 60.0


_last_error: str | None = None


def _warn(msg: str) -> None:
    global _last_error
    _last_error = msg
    print(f"[device_client] {msg}", file=sys.stderr)


def get_last_error() -> str | None:
    """Most recent warning from _warn(), for surfacing swallowed exceptions to
    API callers instead of stderr-only. Not request-scoped — good enough for a
    single-technician tool, not a guarantee under concurrent device_client use."""
    return _last_error


def clear_last_error() -> None:
    global _last_error
    _last_error = None


# ── The device loop ───────────────────────────────────────────────────────────

_loop:       asyncio.AbstractEventLoop | None = None
_thread:     threading.Thread | None = None
_start_lock  = threading.Lock()


def _ensure_loop() -> asyncio.AbstractEventLoop:
    global _loop, _thread
    if _loop is not None:
        return _loop
    with _start_lock:
        if _loop is not None:
            return _loop
        loop = asyncio.new_event_loop()
        thread = threading.Thread(
            target=_run_loop, args=(loop,), name="device-io", daemon=True
        )
        thread.start()
        _loop, _thread = loop, thread
        return loop


def _run_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()


def submit(coro: Coroutine[Any, Any, T]) -> concurrent.futures.Future[T]:
    """Schedule a coroutine on the device loop from any thread."""
    return asyncio.run_coroutine_threadsafe(coro, _ensure_loop())


def call(coro: Coroutine[Any, Any, T], timeout: float = DEFAULT_TIMEOUT) -> T:
    """Blocking. For sync callers (collect.py, cli/)."""
    return submit(coro).result(timeout)


async def acall(coro: Coroutine[Any, Any, T]) -> T:
    """Awaitable. For FastAPI handlers — hands off to the device loop without
    blocking the server loop, and without burning a threadpool slot the way
    asyncio.to_thread would for the whole duration of a 60s ioreg read."""
    return await asyncio.wrap_future(submit(coro))


# ── Connection state (touched only from the device loop) ──────────────────────

_lockdown:    LockdownClient | None = None
_diagnostics: DiagnosticsService | None = None
_lock:        asyncio.Lock | None = None
_pinned_udid: str | None = None


def _get_lock() -> asyncio.Lock:
    # Safe to build lazily: only ever called from the single device loop.
    global _lock
    if _lock is None:
        _lock = asyncio.Lock()
    return _lock


async def _get_lockdown() -> LockdownClient:
    global _lockdown
    if _lockdown is None:
        # connection_type="USB" matters: usbmux also lists Wi-Fi-paired devices,
        # and diagnosing a phone over the network is never what we want here.
        _lockdown = await create_using_usbmux(
            serial=_pinned_udid, connection_type="USB"
        )
    return _lockdown


async def _get_diagnostics() -> DiagnosticsService:
    global _diagnostics
    if _diagnostics is None:
        _diagnostics = DiagnosticsService(await _get_lockdown())
    return _diagnostics


async def _drop_connection() -> None:
    global _lockdown, _diagnostics
    for obj in (_diagnostics, _lockdown):
        if obj is not None:
            try:
                await obj.close()
            except Exception:
                pass
    _lockdown = None
    _diagnostics = None


async def _guarded(what: str, fn: Callable[[], Awaitable[T]], default: T) -> T:
    """Run fn under the lock; on a transient failure drop the connection and retry
    once. Degrades to `default` rather than raising, mirroring how the old
    subprocess runners returned "" on any failure."""
    async with _get_lock():
        for attempt in (1, 2):
            try:
                return await fn()
            except _USER_ACTIONABLE as exc:
                await _drop_connection()
                _warn(f"{what}: {type(exc).__name__} — unlock the device and trust this computer")
                return default
            except TRANSIENT as exc:
                await _drop_connection()
                if attempt == 2:
                    _warn(f"{what}: {type(exc).__name__}: {exc}")
                    return default
            except Exception as exc:
                _warn(f"{what}: {type(exc).__name__}: {exc}")
                return default
    return default


# ── Coroutines (the actual device reads) ──────────────────────────────────────

async def _c_list_udids(usb_only: bool) -> list[str]:
    devices = await usbmux.list_devices()
    return [d.serial for d in devices
            if not usb_only or d.connection_type == "USB"]


async def _c_device_connected(udid: str | None) -> bool:
    # usbmux only — no lockdown handshake. ~5ms vs the ~300ms `ideviceinfo -k
    # ProductType` subprocess this replaces, which matters because the dashboard
    # polls it every 3 seconds. Note this proves "something is plugged in", NOT
    # that it is a trusted iPhone; use device_identity() when that matters.
    try:
        serials = await _c_list_udids(usb_only=True)
    except Exception:
        return False
    return bool(serials) if udid is None else udid in serials


async def _c_device_info(domain: str | None) -> dict:
    async def go() -> dict:
        lockdown = await _get_lockdown()
        if domain is None:
            return dict(lockdown.all_values)
        return await lockdown.get_value(domain=domain) or {}
    return await _guarded(f"device_info({domain})", go, {})


async def _c_device_identity() -> dict | None:
    async def go() -> dict | None:
        lockdown = await _get_lockdown()
        return lockdown.short_info
    return await _guarded("device_identity", go, None)


async def _c_gestalt(keys: tuple[str, ...]) -> dict:
    async def go() -> dict:
        diagnostics = await _get_diagnostics()
        try:
            return await diagnostics.mobilegestalt(list(keys)) or {}
        except DeprecationError:
            # iOS >= 17.4 refuses MobileGestalt outright. The old
            # `idevicediagnostics mobilegestalt` returned an empty parse here, so
            # every caller already handles {} — but an uncaught raise would take
            # all of collect() down on every modern device.
            return {}
    return await _guarded("gestalt", go, {})


async def _c_diag(diag_type: str) -> dict:
    async def go() -> dict:
        diagnostics = await _get_diagnostics()
        result = await diagnostics.info(diag_type) or {}
        # info() wraps its payload: {"GasGauge": {...}}. The old diag() dug the
        # inner dict out with a regex; unwrap it here instead.
        inner = result.get(diag_type)
        return inner if isinstance(inner, dict) else result
    return await _guarded(f"diag({diag_type})", go, {})


async def _c_ioreg(plane: str | None, name: str | None, ioclass: str | None) -> dict:
    async def go() -> dict:
        diagnostics = await _get_diagnostics()
        # Returns None for an entry this model doesn't have (e.g.
        # AppleARMPMUCharger) — callers all expect a dict.
        return await diagnostics.ioregistry(
            plane=plane, name=name, ioclass=ioclass
        ) or {}
    return await _guarded(f"ioreg(plane={plane}, name={name}, class={ioclass})", go, {})


# ── Sync API ──────────────────────────────────────────────────────────────────

def list_udids(usb_only: bool = True) -> list[str]:
    return call(_c_list_udids(usb_only))


def device_connected(udid: str | None = None) -> bool:
    return call(_c_device_connected(udid), timeout=10.0)


def device_identity() -> dict | None:
    return call(_c_device_identity())


def device_info(domain: str | None = None) -> dict:
    return call(_c_device_info(domain))


def gestalt(*keys: str) -> dict:
    return call(_c_gestalt(keys))


def diag(diag_type: str) -> dict:
    return call(_c_diag(diag_type))


def ioreg(plane: str | None = None, name: str | None = None,
          ioclass: str | None = None) -> dict:
    return call(_c_ioreg(plane, name, ioclass), timeout=IOREG_TIMEOUT)


def ioreg_text(plane: str = "IOService") -> str:
    """The IORegistry as an XML plist — the exact text `idevicediagnostics ioreg
    IOService` used to emit, verified byte-identical on iPhone15,3 / iOS 26.6.

    device.hw_present() runs ~60 case-insensitive regexes over this blob, several
    of which only work because of XML's specific shape, so the serialisation is
    load-bearing: JSON changes the verdicts. The {"IORegistry": ...} wrapper is
    what reproduces idevicediagnostics' nesting depth (and so its indentation).

    Serialised on the calling thread on purpose — plistlib.dumps of a 1MB tree
    costs ~18ms and does not belong on the loop that also feeds the log stream.
    """
    tree = ioreg(plane=plane)
    if not tree:
        return ""
    text = plistlib.dumps({"IORegistry": tree}).decode()
    if len(text) < 100_000 or "<key>children</key>" not in text:
        # Guards against a future iOS changing the IORegistry shape, which would
        # otherwise degrade every hw_present() check to "Not Detected" in silence.
        _warn(f"ioreg_text: suspicious payload ({len(text)} chars) — "
              "hardware detection may be unreliable")
    return text


# ── Async API (same reads, for FastAPI handlers) ──────────────────────────────

async def adevice_connected(udid: str | None = None) -> bool:
    return await acall(_c_device_connected(udid))


async def adevice_identity() -> dict | None:
    return await acall(_c_device_identity())


async def adevice_info(domain: str | None = None) -> dict:
    return await acall(_c_device_info(domain))


async def agestalt(*keys: str) -> dict:
    return await acall(_c_gestalt(keys))


async def adiag(diag_type: str) -> dict:
    return await acall(_c_diag(diag_type))


async def aioreg(plane: str | None = None, name: str | None = None,
                 ioclass: str | None = None) -> dict:
    return await acall(_c_ioreg(plane, name, ioclass))


# ── Lifecycle ─────────────────────────────────────────────────────────────────

def pin(udid: str | None) -> None:
    """Bind every subsequent connection to one device, so a second phone being
    plugged in mid-session can never be read from by accident."""
    global _pinned_udid
    if udid != _pinned_udid:
        _pinned_udid = udid
        invalidate()


def unpin() -> None:
    pin(None)


def invalidate() -> None:
    """Force the next call to reconnect. Safe to call from any thread."""
    try:
        submit(_drop_connection()).result(5.0)
    except Exception:
        pass


def ensure_usbmux() -> None:
    """Preflight for the CLI, replacing check_tools(). Exits if usbmuxd is
    unreachable — on Windows that means Apple Mobile Device Support (iTunes)
    isn't installed or its service isn't running."""
    try:
        call(_c_list_udids(usb_only=False), timeout=10.0)
    except Exception as exc:
        print(f"\n[ERROR] Cannot reach usbmuxd: {type(exc).__name__}: {exc}")
        if sys.platform == "win32":
            print("\nInstall Apple Devices (or iTunes) and make sure the "
                  "'Apple Mobile Device Service' is running.")
        else:
            print("\nusbmuxd ships with macOS — if this fails, reconnect the "
                  "device or reboot.")
        sys.exit(1)
