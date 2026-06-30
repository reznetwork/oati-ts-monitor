"""Shared TUI helpers used by monitor.py and client_runtime.py."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

try:
    import curses
except ModuleNotFoundError:  # pragma: no cover - Windows without curses
    curses = None  # type: ignore


def addstr_clip(win, y: int, x: int, text: str, attr: int = 0):
    if curses is None:
        return
    max_x = curses.COLS - 1
    if y >= curses.LINES:
        return
    if x < 0:
        text = text[-x:]
        x = 0
    if x > max_x:
        return
    if x + len(text) > max_x:
        text = text[: max(0, max_x - x)]
    try:
        win.addstr(y, x, text, attr)
    except Exception:
        pass


SPARK_CHARS = "▁▂▃▄▅▆▇█"


def _sparkline(series, width: int) -> str:
    if width <= 0:
        return ""
    values = [
        item.get("value")
        for item in series
        if isinstance(item, dict) and isinstance(item.get("value"), (int, float))
    ]
    if not values:
        return "no data"
    values = values[-width:]
    min_val = min(values)
    max_val = max(values)
    span = max_val - min_val
    if span == 0:
        return SPARK_CHARS[-1] * len(values)
    chars = []
    for val in values:
        idx = int(round((val - min_val) / span * (len(SPARK_CHARS) - 1)))
        chars.append(SPARK_CHARS[idx])
    return "".join(chars)


def _last_numeric(series) -> Optional[float]:
    for item in reversed(series):
        val = item.get("value") if isinstance(item, dict) else None
        if isinstance(val, (int, float)):
            return val
    return None


def _format_coord(deg: Optional[float], pos: str, neg: str) -> Optional[str]:
    if deg is None:
        return None
    hemi = pos if deg >= 0 else neg
    return f"{abs(deg):.4f} {hemi}"


class Colors:
    GREEN = 1
    RED = 2
    YELLOW = 3
    CYAN = 4
    MAGENTA = 5
    WHITE = 6


STYLE_COLORS = {
    "handbrake": (Colors.RED, Colors.GREEN),
    "seatbelt": (Colors.GREEN, Colors.RED),
    "engine": (Colors.GREEN, Colors.RED),
    "indicator": (Colors.YELLOW, Colors.WHITE),
    "default": (Colors.GREEN, 0),
}


def infer_style(label: str, explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    l = label.lower()
    if "ручник" in l:
        return "handbrake"
    if "ремень" in l:
        return "seatbelt"
    if "двигател" in l:
        return "engine"
    if "поворотник" in l:
        return "indicator"
    return "default"
