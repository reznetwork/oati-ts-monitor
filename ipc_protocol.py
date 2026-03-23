import json
from typing import Any, Dict

PROTOCOL_VERSION = 1


def make_envelope(msg_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {"v": PROTOCOL_VERSION, "type": msg_type, "payload": payload}


def encode_message(msg_type: str, payload: Dict[str, Any]) -> bytes:
    return (json.dumps(make_envelope(msg_type, payload), ensure_ascii=False) + "\n").encode("utf-8")


def decode_message(line: str) -> Dict[str, Any]:
    msg = json.loads(line)
    if not isinstance(msg, dict):
        raise ValueError("message must be an object")
    version = int(msg.get("v", 0))
    if version != PROTOCOL_VERSION:
        raise ValueError(f"unsupported protocol version: {version}")
    msg_type = msg.get("type")
    if not isinstance(msg_type, str) or not msg_type:
        raise ValueError("message type is required")
    payload = msg.get("payload", {})
    if not isinstance(payload, dict):
        raise ValueError("payload must be an object")
    return {"v": version, "type": msg_type, "payload": payload}
