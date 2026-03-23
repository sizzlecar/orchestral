use serde_json::{json, Value};

use super::model::{load_xlsx_model, CellContent, CellData, WorkbookModel};
use super::support::{column_letter, display_path, preview};

const TOTAL_ROW_MARKERS: &[&str] = &["总计", "合计", "TOTAL", "SUM"];

pub(super) fn inspect_workbook(path: &std::path::Path) -> Result<Value, String> {
    let workbook = load_xlsx_model(path)?;
    let regions = detect_candidate_regions(&workbook);
    let selected = regions.first().cloned().unwrap_or_else(empty_region);
    Ok(json!({
        "path": display_path(&workbook.path),
        "sheet_name": workbook.sheet_name,
        "sheet_path": workbook.sheet_path,
        "max_row": workbook.max_row,
        "max_column": workbook.max_col,
        "max_column_letter": column_letter(workbook.max_col),
        "candidate_regions": regions,
        "selected_region": selected,
        "patchable_cell_count": selected.get("patchable_cells").and_then(Value::as_array).map(|v| v.len()).unwrap_or(0),
    }))
}

pub(super) fn cell_display_text(cell: &CellData) -> String {
    match &cell.content {
        CellContent::Empty => String::new(),
        CellContent::Number(value)
        | CellContent::Bool(value)
        | CellContent::PlainString(value)
        | CellContent::InlineString(value) => value.clone(),
        CellContent::SharedString(_, text) => text.clone(),
    }
}

fn detect_candidate_regions(workbook: &WorkbookModel) -> Vec<Value> {
    let mut header_candidates = Vec::new();
    let scan_limit = workbook.max_row.min(20);
    for row in 1..=scan_limit {
        let cells = row_non_empty_cells(workbook, row, 1, None);
        if cells.len() < 4 {
            continue;
        }
        let start_col = cells
            .iter()
            .filter_map(|cell| cell.get("column_index").and_then(Value::as_u64))
            .min()
            .unwrap_or(1) as u32;
        let end_col = cells
            .iter()
            .filter_map(|cell| cell.get("column_index").and_then(Value::as_u64))
            .max()
            .unwrap_or(start_col as u64) as u32;
        let mut support_rows = 0;
        for probe_row in (row + 1)..=(workbook.max_row.min(row + 3)) {
            if row_non_empty_cells(workbook, probe_row, start_col, Some(end_col)).len() >= 2 {
                support_rows += 1;
            }
        }
        if support_rows == 0 {
            continue;
        }
        header_candidates.push((
            cells.len() as i32 * 2 + support_rows,
            row,
            start_col,
            end_col,
        ));
    }
    header_candidates.sort_by(|left, right| right.cmp(left));
    let mut regions = Vec::new();
    for (_, header_row, start_col, end_col) in header_candidates {
        if regions.iter().any(|region: &Value| {
            region
                .get("header_row")
                .and_then(Value::as_u64)
                .map(|existing| header_row.abs_diff(existing as u32) <= 1)
                .unwrap_or(false)
        }) {
            continue;
        }
        let region = build_candidate_region(
            workbook,
            regions.len() as u32 + 1,
            header_row,
            start_col,
            end_col,
        );
        if region.get("row_count").and_then(Value::as_u64).unwrap_or(0) == 0 {
            continue;
        }
        regions.push(region);
        if regions.len() >= 3 {
            break;
        }
    }
    regions
}

fn build_candidate_region(
    workbook: &WorkbookModel,
    region_index: u32,
    header_row: u32,
    start_col: u32,
    end_col: u32,
) -> Value {
    let mut columns = Vec::new();
    let mut active_columns = Vec::new();
    for col in start_col..=end_col {
        let text = workbook
            .rows
            .get(&header_row)
            .and_then(|row| row.cells.get(&col))
            .map(cell_display_text)
            .unwrap_or_default();
        if text.is_empty() {
            continue;
        }
        active_columns.push(col);
        columns.push(json!({
            "column": column_letter(col),
            "header": preview(&text, 160),
        }));
    }

    let mut rows = Vec::new();
    let mut patchable_cells = Vec::new();
    let mut blank_streak = 0u32;
    for row_index in (header_row + 1)..=(workbook.max_row.min(header_row + 80)) {
        let mut leading_texts = Vec::new();
        for col in active_columns.iter().take(3) {
            let text = workbook
                .rows
                .get(&row_index)
                .and_then(|row| row.cells.get(col))
                .map(cell_display_text)
                .unwrap_or_default();
            if !text.is_empty() {
                leading_texts.push(text);
            }
        }
        if leading_texts
            .iter()
            .any(|text| TOTAL_ROW_MARKERS.iter().any(|marker| text.contains(marker)))
        {
            break;
        }

        let mut row_cells = Vec::new();
        let mut missing_cells = Vec::new();
        let mut non_empty_count = 0u32;
        for col in &active_columns {
            let coord = format!("{}{}", column_letter(*col), row_index);
            if workbook.merged_followers.contains(&coord) {
                continue;
            }
            let header = columns
                .iter()
                .find(|entry| {
                    entry.get("column").and_then(Value::as_str)
                        == Some(column_letter(*col).as_str())
                })
                .and_then(|entry| entry.get("header"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let cell = workbook
                .rows
                .get(&row_index)
                .and_then(|row| row.cells.get(col));
            let value = cell.map(cell_display_text).unwrap_or_default();
            let is_formula = cell.and_then(|c| c.formula.as_ref()).is_some();
            row_cells.push(json!({
                "cell": coord,
                "column": column_letter(*col),
                "header": header,
                "value": preview(&value, 200),
                "is_formula": is_formula,
            }));
            if !value.is_empty() || is_formula {
                non_empty_count += 1;
            }
            if (cell.is_none() || value.trim().is_empty()) && !is_formula {
                let missing = json!({
                    "cell": coord,
                    "column": column_letter(*col),
                    "header": header,
                });
                missing_cells.push(missing.clone());
                patchable_cells.push(missing);
            }
        }
        if non_empty_count == 0 && missing_cells.is_empty() {
            blank_streak += 1;
            if blank_streak >= 2 {
                break;
            }
            continue;
        }
        blank_streak = 0;
        let label = row_cells
            .iter()
            .filter_map(|cell| cell.get("value").and_then(Value::as_str))
            .find(|value| !value.trim().is_empty())
            .unwrap_or("")
            .to_string();
        rows.push(json!({
            "row_index": row_index,
            "label": if label.is_empty() { format!("row_{}", row_index) } else { label },
            "cells": row_cells,
            "missing_cells": missing_cells,
        }));
    }

    json!({
        "region_id": format!("region_{}", region_index),
        "header_row": header_row,
        "row_count": rows.len(),
        "columns": columns,
        "rows": rows,
        "patchable_cells": patchable_cells,
    })
}

fn row_non_empty_cells(
    workbook: &WorkbookModel,
    row_index: u32,
    start_col: u32,
    end_col: Option<u32>,
) -> Vec<Value> {
    let end = end_col.unwrap_or(workbook.max_col.max(start_col));
    let mut cells = Vec::new();
    for col in start_col..=end {
        let Some(cell) = workbook
            .rows
            .get(&row_index)
            .and_then(|row| row.cells.get(&col))
        else {
            continue;
        };
        let text = cell_display_text(cell);
        if text.trim().is_empty() {
            continue;
        }
        cells.push(json!({
            "cell": cell.coord,
            "column": column_letter(col),
            "column_index": col,
            "value": preview(&text, 240),
        }));
    }
    cells
}

fn empty_region() -> Value {
    json!({
        "region_id": "region_1",
        "header_row": 1,
        "row_count": 0,
        "columns": [],
        "rows": [],
        "patchable_cells": [],
    })
}
