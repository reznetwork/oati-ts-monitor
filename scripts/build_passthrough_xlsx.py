#!/usr/bin/env python3
"""Build passthrough_map.xlsx from monitor_config.json (stdlib only)."""
from __future__ import annotations

import json
import zipfile
from collections import defaultdict
from pathlib import Path
from xml.sax.saxutils import escape

ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / "monitor_config.json"
OUTPUT = ROOT / "passthrough_map.xlsx"


def col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def build_rows(cfg: dict) -> list[list[str]]:
    groups = cfg["passthrough"]["groups"]
    cols_sorted = sorted(groups.items(), key=lambda x: (x[1], x[0]))
    matrix: dict[str, dict[str, str]] = defaultdict(dict)
    vehicles: list[tuple[str, str]] = []

    for v in cfg["vehicles"]:
        sn = v.get("shortName") or v["name"]
        vehicles.append((sn, str(v.get("name", sn))))
        for c in v.get("controllers", []):
            cname = c["name"]
            for p in c.get("points", []):
                g = p.get("passthrough")
                if g:
                    matrix[sn][g] = f"{cname} / ref {p['ref']}"
            for ref_s, g in (c.get("passthrough_gears") or {}).items():
                matrix[sn][g] = f"{cname} / ref {ref_s}"
            for ref_s, g in (c.get("passthrough_extra") or {}).items():
                matrix[sn][g] = f"{cname} / ref {ref_s}"

    header = ["Vehicle", "shortName"] + [f"DI {addr} | {group}" for group, addr in cols_sorted]
    rows = [header]
    for sn, display in vehicles:
        row = [display, sn]
        for group, _addr in cols_sorted:
            row.append(matrix[sn].get(group, ""))
        rows.append(row)
    return rows


def write_xlsx(path: Path, rows: list[list[str]]) -> None:
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
            text = "" if val is None else str(val)
            if text == "":
                continue
            ref = f"{col_letter(c_i)}{r_i}"
            idx = register(text)
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
    with CONFIG.open(encoding="utf-8") as f:
        cfg = json.load(f)
    rows = build_rows(cfg)
    write_xlsx(OUTPUT, rows)
    print(f"Wrote {OUTPUT} ({len(rows) - 1} vehicles, {len(rows[0]) - 2} passthrough columns)")


if __name__ == "__main__":
    main()
