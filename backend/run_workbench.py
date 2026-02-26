from __future__ import annotations

import io
import logging
import logging.config
import os
import re
import socket
import subprocess
import sys
import time
import traceback
import webbrowser
from datetime import datetime
from pathlib import Path

import uvicorn


def base_dir() -> Path:
    """
    Packaged: folder containing the EXE
    Dev: repo root (one level above /backend)
    """
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[1]


def ensure_dirs() -> dict[str, Path]:
    bd = base_dir()
    paths = {
        "base": bd,
        "frontend": bd / "frontend",
        "data": bd / "data",
        "datasets": bd / "data" / "datasets",
        "exports": bd / "exports",
        "logs": bd / "logs",
    }
    paths["exports"].mkdir(parents=True, exist_ok=True)
    paths["logs"].mkdir(parents=True, exist_ok=True)
    paths["datasets"].mkdir(parents=True, exist_ok=True)
    return paths


def boot_log(msg: str) -> None:
    try:
        p = ensure_dirs()["logs"] / "boot.log"
        with p.open("a", encoding="utf-8") as f:
            f.write(f"{datetime.now().isoformat(timespec='seconds')}  {msg}\n")
    except Exception:
        pass


def _ensure_std_streams() -> None:
    """
    In PyInstaller --noconsole mode, sys.stdout/sys.stderr may be None.
    Some logging formatters call .isatty() which will crash if stream is None.
    """
    try:
        if sys.stdout is None:
            sys.stdout = io.TextIOWrapper(open(os.devnull, "wb"), encoding="utf-8", write_through=True)
        if sys.stderr is None:
            sys.stderr = io.TextIOWrapper(open(os.devnull, "wb"), encoding="utf-8", write_through=True)
    except Exception:
        pass


def configure_file_logging(log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    app_log = log_dir / "app.log"

    cfg = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"plain": {"format": "%(asctime)s %(levelname)s %(name)s %(message)s"}},
        "handlers": {
            "file": {
                "class": "logging.FileHandler",
                "filename": str(app_log),
                "encoding": "utf-8",
                "formatter": "plain",
                "level": "INFO",
            }
        },
        "root": {"handlers": ["file"], "level": "INFO"},
        "loggers": {
            "uvicorn": {"handlers": ["file"], "level": "INFO", "propagate": False},
            "uvicorn.error": {"handlers": ["file"], "level": "INFO", "propagate": False},
            "uvicorn.access": {"handlers": ["file"], "level": "INFO", "propagate": False},
        },
    }
    logging.config.dictConfig(cfg)


# ============================================================
# Reliable port check + PID discovery (Windows)
# ============================================================
def _port_is_free(host: str, port: int) -> bool:
    """
    Truth test: if we can bind, it's free. If we can't bind, it's not.
    No netstat parsing lies.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        return True
    except OSError:
        return False
    finally:
        try:
            s.close()
        except Exception:
            pass


def _netstat_listening_pids(port: int) -> list[int]:
    """
    Use netstat only to find PID(s) holding the port (LISTENING).
    """
    try:
        cmd = ["cmd.exe", "/c", f'netstat -ano -p tcp | findstr /R /C:":{port} "']
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="ignore")
    except subprocess.CalledProcessError:
        return []
    except Exception:
        return []

    pids: set[int] = set()
    for line in out.splitlines():
        if "LISTENING" not in line.upper():
            continue
        parts = re.split(r"\s+", line.strip())
        if parts and parts[-1].isdigit():
            pids.add(int(parts[-1]))
    return sorted(pids)


def _taskkill(pid: int) -> bool:
    try:
        subprocess.check_call(
            ["cmd.exe", "/c", f"taskkill /PID {pid} /F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return True
    except Exception:
        return False


def ensure_single_instance(host: str, port: int) -> int:
    """
    Packaged product behavior:
    - If host:port is taken, kill the listener PID(s) (packaged only),
      wait until bind succeeds, then reuse same port.
    - If we can't free it, bump to a free port.
    """
    logger = logging.getLogger("app")

    if _port_is_free(host, port):
        boot_log(f"ensure_single_instance: {host}:{port} is free (bind test)")
        logger.info("ensure_single_instance: %s:%s is free (bind test)", host, port)
        return port

    boot_log(f"ensure_single_instance: {host}:{port} is NOT free (bind test)")
    logger.warning("ensure_single_instance: %s:%s is NOT free (bind test)", host, port)

    # Only aggressive kill for packaged builds
    if getattr(sys, "frozen", False):
        pids = _netstat_listening_pids(port)
        boot_log(f"ensure_single_instance: netstat pids={pids}")
        logger.warning("ensure_single_instance: netstat pids=%s", pids)

        for pid in pids:
            boot_log(f"ensure_single_instance: taskkill /F pid={pid}")
            logger.warning("ensure_single_instance: taskkill /F pid=%s", pid)
            _taskkill(pid)

        # Wait up to ~5s for Windows to release the port
        for i in range(20):
            time.sleep(0.25)
            if _port_is_free(host, port):
                boot_log(f"ensure_single_instance: freed after {i+1}/20 checks")
                logger.warning("ensure_single_instance: freed after %s/20 checks", i + 1)
                return port

        boot_log("ensure_single_instance: kill attempted but port still not free")
        logger.warning("ensure_single_instance: kill attempted but port still not free")

    # Fallback: bump to an available port
    for p in range(port + 1, port + 50):
        if _port_is_free(host, p):
            boot_log(f"ensure_single_instance: switching to {p}")
            logger.warning("ensure_single_instance: switching to %s", p)
            return p

    return port


def main() -> int:
    _ensure_std_streams()
    paths = ensure_dirs()
    configure_file_logging(paths["logs"])

    # Add backend/ to sys.path if present (packaged layout)
    backend_dir = paths["base"] / "backend"
    if backend_dir.exists():
        sys.path.insert(0, str(backend_dir))

    # Set env vars used by app.main (stable runtime paths)
    os.environ["AW_FRONTEND_DIR"] = str(paths["frontend"].resolve())
    os.environ["AW_DATA_DIR"] = str(paths["data"].resolve())
    os.environ["AW_DATASETS_DIR"] = str(paths["datasets"].resolve())
    os.environ["AW_EXPORTS_DIR"] = str(paths["exports"].resolve())

    boot_log("=== START ===")
    boot_log(f"frozen={getattr(sys,'frozen',False)} exe={sys.executable}")
    boot_log(f"cwd={os.getcwd()}")
    boot_log(f"base_dir={paths['base']}")
    boot_log(f"frontend_dir={paths['frontend']}")
    boot_log(f"exports_dir={paths['exports']}")

    # Import FastAPI app directly (avoid uvicorn string importer issues)
    from app.main import app as fastapi_app  # noqa: WPS433

    host = os.environ.get("AW_HOST", "127.0.0.1")
    port = int(os.environ.get("AW_PORT", "8000"))

    # ✅ Single-instance behavior (reliable bind test)
    port = ensure_single_instance(host, port)

    url = f"http://{host}:{port}/ui/"

    def _open_browser_later() -> None:
        time.sleep(0.8)
        try:
            webbrowser.open(url)
            boot_log(f"Opened browser: {url}")
            logging.getLogger("app").info("Opened browser: %s", url)
        except Exception as e:
            boot_log(f"Browser open failed: {e}")

    import threading  # noqa: WPS433
    threading.Thread(target=_open_browser_later, daemon=True).start()

    boot_log(f"Starting uvicorn on {host}:{port}")
    logging.getLogger("app").info("Starting uvicorn on %s:%s", host, port)

    try:
        uvicorn.run(
            fastapi_app,
            host=host,
            port=port,
            log_config=None,  # important for --noconsole
            access_log=True,
        )
    except Exception as e:
        boot_log(f"uvicorn.run failed: {e}")
        boot_log(traceback.format_exc())
        raise

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        boot_log(f"FATAL: {e}")
        boot_log(traceback.format_exc())
        try:
            logging.getLogger("app").exception("FATAL")
        except Exception:
            pass
        raise
