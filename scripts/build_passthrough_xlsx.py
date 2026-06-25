#!/usr/bin/env python3
"""Build passthrough_map.xlsx or passthrough_map.json from monitor_config.json (stdlib only)."""
from __future__ import annotations

import argparse
import json
import zipfile
from collections import defaultdict
from pathlib import Path
from xml.sax.saxutils import escape

ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / "monitor_config.json"
DEFAULT_XLSX = ROOT / "passthrough_map.xlsx"
DEFAULT_JSON = ROOT / "passthrough_map.json"


def col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def collect_descriptions(cfg: dict) -> dict[str, str]:
    labels: dict[str, set[str]] = defaultdict(set)

    for v in cfg["vehicles"]:
        for c in v.get("controllers", []):
            gear_pts = c.get("gear_points") or {}
            for p in c.get("points", []):
                g = p.get("passthrough")
                if g and p.get("label"):
                    labels[g].add(str(p["label"]).strip())
            for ref_s, g in (c.get("passthrough_gears") or {}).items():
                gear = gear_pts.get(ref_s) or gear_pts.get(str(ref_s))
                if gear:
                    labels[g].add(f"Gear {gear}")
            extra_pts = c.get("extra_points") or {}
            for ref_s, g in (c.get("passthrough_extra") or {}).items():
                extra = extra_pts.get(ref_s) or extra_pts.get(str(ref_s))
                if extra:
                    labels[g].add(str(extra))

    descriptions: dict[str, str] = {}
    for group in cfg["passthrough"]["groups"]:
        if labels[group]:
            descriptions[group] = " / ".join(sorted(labels[group]))
        else:
            descriptions[group] = group.replace(":", " ").replace("_", " ")
    return descriptions


def build_map(cfg: dict) -> dict:
    groups = cfg["passthrough"]["groups"]
    function_order = [group for group, _addr in sorted(groups.items(), key=lambda x: (x[1], x[0]))]
    group_addrs = dict(groups.items())
    vehicle_order: list[str] = []
    vehicles: dict[str, dict[str, int]] = {}

    for v in cfg["vehicles"]:
        sn = v.get("shortName") or v["name"]
        vehicle_order.append(sn)
        per_vehicle: dict[str, int] = {}
        for c in v.get("controllers", []):
            for p in c.get("points", []):
                g = p.get("passthrough")
                if g and g in group_addrs:
                    per_vehicle[g] = group_addrs[g]
            for _ref_s, g in (c.get("passthrough_gears") or {}).items():
                if g in group_addrs:
                    per_vehicle[g] = group_addrs[g]
            for _ref_s, g in (c.get("passthrough_extra") or {}).items():
                if g in group_addrs:
                    per_vehicle[g] = group_addrs[g]
        vehicles[sn] = per_vehicle

    return {
        "vehicle_order": vehicle_order,
        "function_order": function_order,
        "descriptions": collect_descriptions(cfg),
        "vehicles": vehicles,
    }


def build_rows(data: dict) -> list[list[str | int]]:
    vehicle_order = data["vehicle_order"]
    header: list[str | int] = ["Description", "Function"] + vehicle_order
    rows: list[list[str | int]] = [header]
    for group in data["function_order"]:
        row: list[str | int] = [data["descriptions"].get(group, ""), group]
        for sn in vehicle_order:
            row.append(data["vehicles"][sn].get(group, ""))
        rows.append(row)
    return rows


def write_json(path: Path, data: dict) -> None:
    payload = {sn: data["vehicles"][sn] for sn in data["vehicle_order"] if data["vehicles"][sn]}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")


def write_xlsx(path: Path, rows: list[list[str | int]]) -> None:
    shared: list[str] = []
    shared_idx: dict[str, int] = {}

    def register(val: str) -> int:
        if val not in shared_idx:
            shared_idx[val] = len(shared)
            shared.append(val)
        return shared_idx[val]

    sheet_rows: list[str] = []
    for r_i, row in enumerate(rows, start=1):
        cells: list[str] = []
        for c_i, val in enumerate(row, start=1):
            if val == "" or val is None:
                continue
            ref = f"{col_letter(c_i)}{r_i}"
            if isinstance(val, int):
                cells.append(f'<c r="{ref}"><v>{val}</v></c>')
            else:
                idx = register(str(val))
                cells.append(f'<c r="{ref}" t="s"><v>{idx}</v></c>')
        sheet_rows.append(f'<row r="{r_i}">{"".join(cells)}</row>')

    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f"<dimension ref=\"A1:{col_letter(len(rows[0]))}{len(rows)}\"/>"
        '<sheetViews><sheetView workbookViewId="0" tabSelected="1">'
        '<pane ySplit="1" topLeftCell="C2" activePane="bottomRight" state="frozen"/>'
        "</sheetView></sheetViews>"
        f"<sheetData>{''.join(sheet_rows)}</sheetData></worksheet>"
    )

    sst_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        f'count="{len(shared)}" uniqueCount="{len(shared)}">'
        + "".join(f"<si><t>{escape(s)}</t></si>" for s in shared)
        + "</sst>"
    )

    content_types = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
</Types>"""

    root_rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>"""

    workbook_rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>"""

    workbook = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
 xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets><sheet name="Passthrough map" sheetId="1" r:id="rId1"/></sheets>
</workbook>"""

    styles = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>
  <fills count="1"><fill><patternFill patternType="none"/></fill></fills>
  <borders count="1"><border/></borders>
  <cellStyleXfs count="1"><xf/></cellStyleXfs>
  <cellXfs count="1"><xf xfId="0"/></cellXfs>
</styleSheet>"""

    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types)
        zf.writestr("_rels/.rels", root_rels)
        zf.writestr("xl/workbook.xml", workbook)
        zf.writestr("xl/_rels/workbook.xml.rels", workbook_rels)
        zf.writestr("xl/worksheets/sheet1.xml", sheet_xml)
        zf.writestr("xl/sharedStrings.xml", sst_xml)
        zf.writestr("xl/styles.xml", styles)


def main() -> None:
    ap = argparse.ArgumentParser(description="Build passthrough mapping table from monitor_config.json")
    ap.add_argument(
        "--format",
        "-f",
        choices=("xlsx", "json"),
        default="xlsx",
        help="output format (default: xlsx)",
    )
    ap.add_argument(
        "--output",
        "-o",
        type=Path,
        default=None,
        help="output file path (default: passthrough_map.xlsx or passthrough_map.json)",
    )
    ap.add_argument(
        "--config",
        "-c",
        type=Path,
        default=CONFIG,
        help=f"monitor config path (default: {CONFIG})",
    )
    args = ap.parse_args()

    with args.config.open(encoding="utf-8") as f:
        cfg = json.load(f)

    data = build_map(cfg)
    n_functions = len(data["function_order"])
    n_vehicles = len(data["vehicle_order"])

    if args.format == "json":
        output = args.output or DEFAULT_JSON
        write_json(output, data)
    else:
        output = args.output or DEFAULT_XLSX
        write_xlsx(output, build_rows(data))

    print(f"Wrote {output} ({n_functions} functions, {n_vehicles} vehicles)")


if __name__ == "__main__":
    main()
