"""IO bridge expression evaluator."""
from __future__ import annotations

import ast
import logging
import time
from typing import Dict, List, Optional

from core.config import BridgeMappingCfg
from services.modbus import MBClient

def _evaluate_expression(expr: str, values: Dict[str, bool]) -> bool:
    """
    Evaluate a simple boolean expression using provided values.
    Supports AND/OR/NOT and XOR (using ^) operators.
    """

    def _eval(node: ast.AST) -> bool:
        if isinstance(node, ast.Expression):
            return _eval(node.body)
        if isinstance(node, ast.BoolOp):
            vals = [_eval(v) for v in node.values]
            if isinstance(node.op, ast.And):
                return all(vals)
            if isinstance(node.op, ast.Or):
                return any(vals)
            raise ValueError("Only AND/OR boolean operators are supported")
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitXor):
            return _eval(node.left) ^ _eval(node.right)
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            return not _eval(node.operand)
        if isinstance(node, ast.Name):
            if node.id not in values:
                raise ValueError(f"Unknown variable '{node.id}' in logic expression")
            return bool(values[node.id])
        if isinstance(node, ast.Constant) and isinstance(node.value, bool):
            return bool(node.value)
        raise ValueError("Unsupported expression; use AND, OR, NOT, XOR (^), and parentheses")

    parsed = ast.parse(expr, mode="eval")
    return bool(_eval(parsed))


class IOBridgeEvaluator:
    def __init__(self, mappings: List[BridgeMappingCfg]):
        self.mappings = list(mappings or [])
        self._last_written: Dict[str, Optional[bool]] = {m.name: None for m in self.mappings}
        self._candidate_state: Dict[str, Optional[bool]] = {m.name: None for m in self.mappings}
        self._candidate_since: Dict[str, Optional[float]] = {m.name: None for m in self.mappings}
        self._logger = logging.getLogger("io_bridge")

    def extra_refs_for(self, controller_name: str) -> List[int]:
        refs: List[int] = []
        for m in self.mappings:
            for inp in m.inputs:
                if inp.controller == controller_name:
                    refs.append(int(inp.ref))
        return refs

    def evaluate(
        self,
        ref_values: Dict[str, Dict[int, Optional[bool]]],
        clients: Dict[str, MBClient],
    ) -> None:
        now = time.monotonic()
        for m in self.mappings:
            desired = self._compute_desired(m, ref_values)
            debounce_s = max(0.0, float(m.debounce_ms or 0) / 1000.0)

            if debounce_s <= 0:
                self._maybe_write(m, desired, clients)
                continue

            cand = self._candidate_state.get(m.name)
            since = self._candidate_since.get(m.name)
            if cand is None or cand != desired:
                self._candidate_state[m.name] = desired
                self._candidate_since[m.name] = now
                continue

            if since is not None and (now - since) >= debounce_s:
                self._maybe_write(m, desired, clients)

    def _compute_desired(
        self,
        m: BridgeMappingCfg,
        ref_values: Dict[str, Dict[int, Optional[bool]]],
    ) -> Optional[bool]:
        vals: Dict[str, bool] = {}
        for idx, inp in enumerate(m.inputs):
            per_ctrl = ref_values.get(inp.controller) or {}
            raw = per_ctrl.get(int(inp.ref))
            if raw is None:
                return self._on_error_value(m.on_error)
            name = inp.name or f"in{idx+1}"
            vals[name] = bool(raw)

        try:
            if m.logic:
                out = _evaluate_expression(m.logic, vals)
            else:
                if len(vals) != 1:
                    raise ValueError(f"mapping '{m.name}' requires logic for multiple inputs")
                out = next(iter(vals.values()))
        except Exception as e:
            self._logger.warning("Bridge mapping '%s' logic error: %s", m.name, e)
            return self._on_error_value(m.on_error)

        out = (not out) if m.invert else out
        return bool(out)

    def _on_error_value(self, on_error: str) -> Optional[bool]:
        mode = str(on_error or "hold").lower()
        if mode == "force_off":
            return False
        if mode == "force_on":
            return True
        return None  # hold

    def _maybe_write(self, m: BridgeMappingCfg, desired: Optional[bool], clients: Dict[str, MBClient]) -> None:
        if desired is None:
            return
        last = self._last_written.get(m.name)
        if last is not None and last == desired:
            return
        client = clients.get(m.output.controller)
        if client is None:
            self._logger.warning("Bridge mapping '%s' output controller not found: %s", m.name, m.output.controller)
            return
        ok = client.write_coil(int(m.output.address), bool(desired))
        if ok:
            self._last_written[m.name] = bool(desired)
        else:
            self._logger.warning(
                "Bridge mapping '%s' coil write failed (%s:%s desired=%s): %s",
                m.name,
                m.output.controller,
                m.output.address,
                int(bool(desired)),
                client.last_error,
            )
