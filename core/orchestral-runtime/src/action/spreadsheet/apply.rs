use std::path::Path;

use serde_json::Value;

use super::model::{load_xlsx_model, save_xlsx_model};
use super::support::{display_path, parse_cell_ref};

pub(super) fn apply_patch(path: &Path, patch_spec: &Value) -> Result<(String, usize), String> {
    let mut workbook = load_xlsx_model(path)?;
    let fills = patch_spec
        .get("fills")
        .and_then(Value::as_array)
        .ok_or_else(|| "patch_spec.fills must be an array".to_string())?;
    let mut applied = 0usize;
    for fill in fills {
        let Some(cell_ref) = fill.get("cell").and_then(Value::as_str) else {
            continue;
        };
        let (row, col) = parse_cell_ref(cell_ref)?;
        let value = fill
            .get("value")
            .ok_or_else(|| format!("fill {} missing value", cell_ref))?;
        workbook.set_cell_value(cell_ref, row, col, value.clone());
        applied += 1;
    }
    save_xlsx_model(&workbook)?;
    Ok((display_path(&workbook.path), applied))
}
