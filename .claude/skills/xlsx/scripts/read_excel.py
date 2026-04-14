"""
Read Excel file content and output structured JSON.

Usage:
    python read_excel.py <file> [--sheet <name>] [--max-rows <n>]

Output JSON:
    {
      "file": "path.xlsx",
      "sheets": [
        {
          "name": "Sheet1",
          "dimensions": "A1:K29",
          "max_row": 29,
          "max_column": 11,
          "headers": ["A: Name", "B: Date", ...],
          "rows": [
            {"row": 1, "cells": {"A": "value", "B": 123, ...}},
            ...
          ],
          "empty_cells": [
            {"row": 5, "col": "F", "header": "Score"},
            ...
          ]
        }
      ]
    }

empty_cells lists cells that are None/empty in rows that have at least one
non-empty cell — these are the fill candidates.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

from openpyxl import load_workbook
from openpyxl.utils import get_column_letter


def col_letter(col_idx: int) -> str:
    return get_column_letter(col_idx)


def cell_value_to_json(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    return str(value)


def read_sheet(ws, max_rows: Optional[int] = None):
    # Collect merged cell ranges so planner knows to skip them
    merged = [str(r) for r in ws.merged_cells.ranges] if ws.merged_cells else []

    result = {
        "name": ws.title,
        "dimensions": ws.dimensions,
        "max_row": ws.max_row,
        "max_column": ws.max_column,
    }
    if merged:
        result["merged_cells"] = merged

    # Detect header row: first row with multiple non-empty cells
    header_row_idx = None
    headers = {}
    for row_idx in range(1, min((ws.max_row or 0) + 1, 10)):
        non_empty = 0
        for col_idx in range(1, (ws.max_column or 0) + 1):
            val = ws.cell(row=row_idx, column=col_idx).value
            if val is not None and str(val).strip():
                non_empty += 1
        if non_empty >= 2:
            header_row_idx = row_idx
            for col_idx in range(1, (ws.max_column or 0) + 1):
                val = ws.cell(row=row_idx, column=col_idx).value
                if val is not None and str(val).strip():
                    headers[col_letter(col_idx)] = str(val).strip()
            break

    result["header_row"] = header_row_idx
    result["headers"] = [f"{col}: {name}" for col, name in sorted(headers.items())]

    # Build a set of cells that are inside a merged range but NOT the top-left cell.
    # These should be skipped in empty-cell reporting since they can't be written to.
    merged_interior = set()
    for mr in (ws.merged_cells.ranges if ws.merged_cells else []):
        first = True
        for row in range(mr.min_row, mr.max_row + 1):
            for col in range(mr.min_col, mr.max_col + 1):
                if first:
                    first = False
                    continue
                merged_interior.add((row, col))

    # Read rows
    rows = []
    empty_cells = []
    end_row = ws.max_row or 0
    if max_rows and header_row_idx:
        end_row = min(end_row, header_row_idx + max_rows)

    for row_idx in range(1, end_row + 1):
        cells = {}
        row_has_data = False
        row_empty_cols = []
        for col_idx in range(1, (ws.max_column or 0) + 1):
            col = col_letter(col_idx)
            cell = ws.cell(row=row_idx, column=col_idx)
            val = cell_value_to_json(cell.value)
            if val is not None:
                cells[col] = val
                row_has_data = True
            elif (row_idx, col_idx) not in merged_interior:
                row_empty_cols.append(col)

        if cells:
            rows.append({"row": row_idx, "cells": cells})

        # Track empty cells in data rows (rows that have some content)
        if row_has_data and header_row_idx and row_idx > header_row_idx:
            row_empties = [col for col in row_empty_cols if col in headers]
            if row_empties:
                empty_cells.append({
                    "row": row_idx,
                    "cols": row_empties,
                })

    result["rows"] = rows
    result["empty_cells"] = empty_cells
    total_empty = sum(len(e["cols"]) for e in empty_cells)
    result["empty_cell_count"] = total_empty

    # Summary: how many empty cells per column (helps planner know what to fill)
    col_counts = {}
    for entry in empty_cells:
        for col in entry["cols"]:
            col_counts[col] = col_counts.get(col, 0) + 1
    if col_counts:
        result["fill_summary"] = {
            f"{col} ({headers.get(col, '?')})": count
            for col, count in sorted(col_counts.items())
        }

    return result


def read_excel(file_path: str, sheet_name: Optional[str] = None, max_rows: Optional[int] = None):
    path = Path(file_path)
    if not path.exists():
        return {"error": f"File not found: {file_path}"}

    try:
        wb = load_workbook(str(path), data_only=True)
    except Exception as e:
        return {"error": f"Failed to open {file_path}: {e}"}

    result = {"file": str(path), "sheets": []}

    target_sheets = [sheet_name] if sheet_name else wb.sheetnames
    for name in target_sheets:
        if name not in wb.sheetnames:
            result["sheets"].append({"name": name, "error": f"Sheet '{name}' not found"})
            continue
        ws = wb[name]
        result["sheets"].append(read_sheet(ws, max_rows))

    wb.close()
    return result


def main():
    parser = argparse.ArgumentParser(description="Read Excel file content as JSON")
    parser.add_argument("file", help="Path to .xlsx/.xlsm file")
    parser.add_argument("--sheet", help="Read only this sheet (default: all)")
    parser.add_argument("--max-rows", type=int, default=None,
                        help="Max data rows to read per sheet (default: all)")
    parser.add_argument("--empty-only", action="store_true",
                        help="Only output headers and empty cells (compact mode for fill tasks)")
    args = parser.parse_args()

    result = read_excel(args.file, args.sheet, args.max_rows)

    if args.empty_only:
        # Compact text output optimized for LLM consumption
        for sheet in result.get("sheets", []):
            rows_data = sheet.get("rows", [])  # keep for context lookup
            print(f"Sheet: {sheet['name']} ({sheet.get('dimensions', '?')})")
            if sheet.get("merged_cells"):
                print(f"Merged: {', '.join(sheet['merged_cells'])}")
            if sheet.get("headers"):
                clean_headers = [h.replace('\n', ' ') for h in sheet["headers"]]
                print(f"Headers (row {sheet.get('header_row', '?')}): {' | '.join(clean_headers)}")
            if sheet.get("fill_summary"):
                print("Columns needing fill:")
                for col_header, count in sheet["fill_summary"].items():
                    print(f"  {col_header.replace(chr(10), ' ')}: {count} empty cells")
            if sheet.get("empty_cells"):
                print("Empty cells to fill:")
                for entry in sheet["empty_cells"]:
                    # Show row context (non-empty cells in that row) so planner knows what the row is about
                    context = ""
                    matching_rows = [r for r in sheet.get("rows", []) if r["row"] == entry["row"]]
                    if matching_rows:
                        ctx_parts = []
                        for col_letter_key, val in sorted(matching_rows[0]["cells"].items()):
                            if col_letter_key in entry["cols"]:
                                continue  # skip empty cols
                            s = str(val).replace('\n', ' ')[:40]
                            ctx_parts.append(f"{col_letter_key}={s}")
                        if ctx_parts:
                            context = f"  ({', '.join(ctx_parts[:4])})"
                    print(f"  row {entry['row']}: {', '.join(entry['cols'])}{context}")
            print(f"Total: {sheet.get('empty_cell_count', 0)} cells to fill")
    else:
        json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
        print()

    if "error" in result:
        sys.exit(1)


if __name__ == "__main__":
    main()
