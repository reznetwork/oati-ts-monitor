from __future__ import annotations

import base64

_UTF8_B64_PREFIX = "utf-8b64:"


def encode_http_header_text(value: str) -> str:
    """Return an HTTP header-safe representation of arbitrary Unicode text."""
    text = str(value or "")
    try:
        text.encode("latin-1")
        return text
    except UnicodeEncodeError:
        encoded = base64.b64encode(text.encode("utf-8")).decode("ascii")
        return f"{_UTF8_B64_PREFIX}{encoded}"


def decode_http_header_text(value: str) -> str:
    """Decode a header value produced by encode_http_header_text."""
    text = str(value or "")
    if text.startswith(_UTF8_B64_PREFIX):
        return base64.b64decode(text[len(_UTF8_B64_PREFIX) :], validate=True).decode("utf-8")
    return text
