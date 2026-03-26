use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use quick_xml::events::Event;
use quick_xml::Reader;
use serde_json::Value;
use zip::write::SimpleFileOptions;
use zip::{ZipArchive, ZipWriter};

use super::support::{
    column_letter, decode_text, normalize_path, parse_cell_ref, xml_escape_attr, xml_escape_text,
};

#[derive(Debug, Clone)]
pub(super) struct WorkbookModel {
    pub(super) path: PathBuf,
    pub(super) sheet_path: String,
    pub(super) sheet_name: String,
    original_sheet_xml: String,
    pub(super) rows: BTreeMap<u32, RowData>,
    pub(super) merged_followers: HashSet<String>,
    pub(super) max_row: u32,
    pub(super) max_col: u32,
}

impl WorkbookModel {
    pub(super) fn set_cell_value(&mut self, cell_ref: &str, row: u32, col: u32, value: Value) {
        let inferred_style = infer_style(&self.rows, row, col);
        let row_entry = self.rows.entry(row).or_insert_with(|| RowData {
            row_index: row,
            attrs: vec![("r".to_string(), row.to_string())],
            cells: BTreeMap::new(),
        });
        let mut cell = row_entry.cells.remove(&col).unwrap_or_else(|| CellData {
            coord: cell_ref.to_string(),
            row,
            col,
            style: inferred_style,
            extra_attrs: Vec::new(),
            content: CellContent::Empty,
            formula: None,
        });
        cell.coord = cell_ref.to_string();
        cell.row = row;
        cell.col = col;
        cell.formula = None;
        cell.content = match value {
            Value::Number(number) => CellContent::Number(number.to_string()),
            Value::Bool(boolean) => CellContent::Bool(if boolean { "1" } else { "0" }.to_string()),
            Value::String(text) => CellContent::InlineString(text),
            Value::Null => CellContent::InlineString(String::new()),
            other => CellContent::InlineString(other.to_string()),
        };
        row_entry.cells.insert(col, cell);
        self.max_row = self.max_row.max(row);
        self.max_col = self.max_col.max(col);
    }
}

#[derive(Debug, Clone)]
pub(super) struct RowData {
    pub(super) row_index: u32,
    attrs: Vec<(String, String)>,
    pub(super) cells: BTreeMap<u32, CellData>,
}

#[derive(Debug, Clone)]
pub(super) struct CellData {
    pub(super) coord: String,
    pub(super) row: u32,
    pub(super) col: u32,
    style: Option<String>,
    extra_attrs: Vec<(String, String)>,
    pub(super) content: CellContent,
    pub(super) formula: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) enum CellContent {
    Empty,
    Number(String),
    Bool(String),
    SharedString(String, String),
    InlineString(String),
    PlainString(String),
}

pub(super) fn load_xlsx_model(path: &Path) -> Result<WorkbookModel, String> {
    let path = normalize_path(path)?;
    let file = File::open(&path).map_err(|e| format!("open workbook failed: {}", e))?;
    let mut archive = ZipArchive::new(file).map_err(|e| format!("open zip failed: {}", e))?;
    let workbook_xml = read_zip_entry(&mut archive, "xl/workbook.xml")?;
    let rels_xml = read_zip_entry(&mut archive, "xl/_rels/workbook.xml.rels")?;
    let (sheet_name, sheet_path) = resolve_first_sheet(&workbook_xml, &rels_xml)?;
    let shared_strings = match read_optional_zip_entry(&mut archive, "xl/sharedStrings.xml") {
        Some(result) => parse_shared_strings(result?)?,
        None => Vec::new(),
    };
    let sheet_xml = read_zip_entry(&mut archive, &sheet_path)?;
    let parsed_sheet = parse_sheet(&sheet_xml, &shared_strings)?;
    Ok(WorkbookModel {
        path,
        sheet_path,
        sheet_name,
        original_sheet_xml: sheet_xml,
        rows: parsed_sheet.rows,
        merged_followers: parsed_sheet.merged_followers,
        max_row: parsed_sheet.max_row,
        max_col: parsed_sheet.max_col,
    })
}

pub(super) fn save_xlsx_model(workbook: &WorkbookModel) -> Result<(), String> {
    let source = File::open(&workbook.path).map_err(|e| format!("open workbook failed: {}", e))?;
    let mut archive = ZipArchive::new(source).map_err(|e| format!("open zip failed: {}", e))?;
    let replacement_sheet_xml = replace_sheet_data(
        &workbook.original_sheet_xml,
        &build_sheet_data_xml(&workbook.rows),
    )?;
    let temp_path = workbook.path.with_extension("xlsx.orchestral.tmp");
    let temp_file =
        File::create(&temp_path).map_err(|e| format!("create temp workbook failed: {}", e))?;
    let mut writer = ZipWriter::new(temp_file);

    for index in 0..archive.len() {
        let mut entry = archive
            .by_index(index)
            .map_err(|e| format!("read zip entry failed: {}", e))?;
        let name = entry.name().to_string();
        if entry.is_dir() {
            writer
                .add_directory(name, SimpleFileOptions::default())
                .map_err(|e| format!("write zip dir failed: {}", e))?;
            continue;
        }
        let mut bytes = Vec::new();
        entry
            .read_to_end(&mut bytes)
            .map_err(|e| format!("read zip file failed: {}", e))?;
        let options = SimpleFileOptions::default().compression_method(entry.compression());
        writer
            .start_file(name.clone(), options)
            .map_err(|e| format!("write zip file header failed: {}", e))?;
        if name == workbook.sheet_path {
            writer
                .write_all(replacement_sheet_xml.as_bytes())
                .map_err(|e| format!("write replacement sheet failed: {}", e))?;
        } else {
            writer
                .write_all(&bytes)
                .map_err(|e| format!("copy zip entry failed: {}", e))?;
        }
    }

    writer
        .finish()
        .map_err(|e| format!("finish workbook write failed: {}", e))?;
    std::fs::rename(&temp_path, &workbook.path)
        .map_err(|e| format!("replace workbook failed: {}", e))?;
    Ok(())
}

fn infer_style(rows: &BTreeMap<u32, RowData>, row: u32, col: u32) -> Option<String> {
    if let Some(style) = rows.get(&row).and_then(|data| {
        data.cells.values().find_map(|cell| {
            if cell.col == col || cell.col.abs_diff(col) <= 1 {
                cell.style.clone()
            } else {
                None
            }
        })
    }) {
        return Some(style);
    }

    rows.values().find_map(|data| {
        data.cells
            .get(&col)
            .and_then(|cell| cell.style.clone())
            .or_else(|| {
                data.cells.values().find_map(|cell| {
                    if cell.col.abs_diff(col) <= 1 {
                        cell.style.clone()
                    } else {
                        None
                    }
                })
            })
    })
}

fn resolve_first_sheet(workbook_xml: &str, rels_xml: &str) -> Result<(String, String), String> {
    let mut reader = Reader::from_str(workbook_xml);
    reader.config_mut().trim_text(true);
    let mut first_sheet_name: Option<String> = None;
    let mut first_sheet_rid: Option<String> = None;
    loop {
        match reader.read_event() {
            Ok(Event::Start(ref event)) | Ok(Event::Empty(ref event))
                if event.name().as_ref() == b"sheet" =>
            {
                first_sheet_name = attr_value(event, b"name");
                first_sheet_rid = attr_value(event, b"r:id").or_else(|| attr_value(event, b"id"));
                break;
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => {
                return Err(format!("parse workbook.xml failed: {}", error));
            }
        }
    }
    let rid = first_sheet_rid.ok_or_else(|| "workbook missing first sheet r:id".to_string())?;
    let sheet_name = first_sheet_name.unwrap_or_else(|| "Sheet1".to_string());

    let mut reader = Reader::from_str(rels_xml);
    reader.config_mut().trim_text(true);
    loop {
        match reader.read_event() {
            Ok(Event::Start(ref event)) | Ok(Event::Empty(ref event))
                if event.name().as_ref() == b"Relationship" =>
            {
                let id = attr_value(event, b"Id");
                if id.as_deref() != Some(rid.as_str()) {
                    continue;
                }
                let raw_target = attr_value(event, b"Target")
                    .ok_or_else(|| "workbook relationship missing Target".to_string())?;
                let target = raw_target.trim_start_matches('/');
                let sheet_path = if target.starts_with("xl/") {
                    target.to_string()
                } else {
                    format!("xl/{}", target)
                };
                return Ok((sheet_name, sheet_path));
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => {
                return Err(format!("parse workbook rels failed: {}", error));
            }
        }
    }
    Err("failed to resolve first worksheet target".to_string())
}

fn parse_shared_strings(xml: String) -> Result<Vec<String>, String> {
    let mut reader = Reader::from_str(&xml);
    reader.config_mut().trim_text(false);
    let mut strings = Vec::new();
    let mut in_si = false;
    let mut in_t = false;
    let mut current = String::new();
    loop {
        match reader.read_event() {
            Ok(Event::Start(ref event)) if event.name().as_ref() == b"si" => {
                in_si = true;
                current.clear();
            }
            Ok(Event::End(ref event)) if event.name().as_ref() == b"si" => {
                if in_si {
                    strings.push(current.clone());
                }
                in_si = false;
                in_t = false;
            }
            Ok(Event::Start(ref event)) if event.name().as_ref() == b"t" && in_si => {
                in_t = true;
            }
            Ok(Event::End(ref event)) if event.name().as_ref() == b"t" && in_si => {
                in_t = false;
            }
            Ok(Event::Text(text)) if in_t => {
                current.push_str(&decode_text(text.as_ref()));
            }
            Ok(Event::CData(text)) if in_t => {
                current.push_str(&String::from_utf8_lossy(text.as_ref()));
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => return Err(format!("parse shared strings failed: {}", error)),
        }
    }
    Ok(strings)
}

struct ParsedSheet {
    rows: BTreeMap<u32, RowData>,
    merged_followers: HashSet<String>,
    max_row: u32,
    max_col: u32,
}

fn parse_sheet(xml: &str, shared_strings: &[String]) -> Result<ParsedSheet, String> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(false);

    let mut rows = BTreeMap::new();
    let mut current_row: Option<RowData> = None;
    let mut current_cell: Option<CellParseState> = None;
    let mut capture_value = false;
    let mut capture_formula = false;
    let mut capture_inline = false;
    let mut merged_followers = HashSet::new();
    let mut max_row = 0u32;
    let mut max_col = 0u32;

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref event)) if event.name().as_ref() == b"row" => {
                let row_index = attr_value(event, b"r")
                    .and_then(|v| v.parse::<u32>().ok())
                    .unwrap_or(max_row + 1);
                max_row = max_row.max(row_index);
                current_row = Some(RowData {
                    row_index,
                    attrs: collect_attrs(event),
                    cells: BTreeMap::new(),
                });
            }
            Ok(Event::End(ref event)) if event.name().as_ref() == b"row" => {
                if let Some(row) = current_row.take() {
                    max_col = max_col.max(*row.cells.keys().max().unwrap_or(&0));
                    rows.insert(row.row_index, row);
                }
            }
            Ok(Event::Start(ref event)) if event.name().as_ref() == b"c" => {
                let coord = attr_value(event, b"r").unwrap_or_default();
                let (row, col) = parse_cell_ref(&coord)?;
                max_row = max_row.max(row);
                max_col = max_col.max(col);
                current_cell = Some(CellParseState {
                    coord,
                    row,
                    col,
                    cell_type: attr_value(event, b"t"),
                    style: attr_value(event, b"s"),
                    extra_attrs: collect_extra_cell_attrs(event),
                    value: String::new(),
                    formula: None,
                    inline_text: String::new(),
                });
            }
            Ok(Event::Empty(ref event)) if event.name().as_ref() == b"c" => {
                let coord = attr_value(event, b"r").unwrap_or_default();
                let (row, col) = parse_cell_ref(&coord)?;
                max_row = max_row.max(row);
                max_col = max_col.max(col);
                let cell = CellData {
                    coord,
                    row,
                    col,
                    style: attr_value(event, b"s"),
                    extra_attrs: collect_extra_cell_attrs(event),
                    content: CellContent::Empty,
                    formula: None,
                };
                if let Some(row_data) = current_row.as_mut() {
                    row_data.cells.insert(col, cell);
                }
            }
            Ok(Event::End(ref event)) if event.name().as_ref() == b"c" => {
                if let Some(state) = current_cell.take() {
                    let col = state.col;
                    let cell = finalize_cell(state, shared_strings);
                    if let Some(row_data) = current_row.as_mut() {
                        row_data.cells.insert(col, cell);
                    }
                }
            }
            Ok(Event::Start(ref event)) if event.name().as_ref() == b"v" => {
                capture_value = true;
            }
            Ok(Event::End(ref event)) if event.name().as_ref() == b"v" => {
                capture_value = false;
            }
            Ok(Event::Start(ref event)) if event.name().as_ref() == b"f" => {
                capture_formula = true;
            }
            Ok(Event::End(ref event)) if event.name().as_ref() == b"f" => {
                capture_formula = false;
            }
            Ok(Event::Start(ref event)) if event.name().as_ref() == b"t" => {
                if current_cell
                    .as_ref()
                    .and_then(|cell| cell.cell_type.as_deref())
                    == Some("inlineStr")
                {
                    capture_inline = true;
                }
            }
            Ok(Event::End(ref event)) if event.name().as_ref() == b"t" => {
                capture_inline = false;
            }
            Ok(Event::Empty(ref event)) if event.name().as_ref() == b"mergeCell" => {
                if let Some(range_ref) = attr_value(event, b"ref") {
                    extend_merged_followers(&mut merged_followers, &range_ref);
                }
            }
            Ok(Event::Text(text)) => {
                let decoded = decode_text(text.as_ref());
                if capture_value {
                    if let Some(cell) = current_cell.as_mut() {
                        cell.value.push_str(&decoded);
                    }
                } else if capture_formula {
                    if let Some(cell) = current_cell.as_mut() {
                        let formula = cell.formula.get_or_insert_with(String::new);
                        formula.push_str(&decoded);
                    }
                } else if capture_inline {
                    if let Some(cell) = current_cell.as_mut() {
                        cell.inline_text.push_str(&decoded);
                    }
                }
            }
            Ok(Event::CData(text)) => {
                let decoded = String::from_utf8_lossy(text.as_ref()).to_string();
                if capture_inline {
                    if let Some(cell) = current_cell.as_mut() {
                        cell.inline_text.push_str(&decoded);
                    }
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => return Err(format!("parse sheet xml failed: {}", error)),
        }
    }

    Ok(ParsedSheet {
        rows,
        merged_followers,
        max_row,
        max_col,
    })
}

struct CellParseState {
    coord: String,
    row: u32,
    col: u32,
    cell_type: Option<String>,
    style: Option<String>,
    extra_attrs: Vec<(String, String)>,
    value: String,
    formula: Option<String>,
    inline_text: String,
}

fn finalize_cell(state: CellParseState, shared_strings: &[String]) -> CellData {
    let content = match state.cell_type.as_deref() {
        Some("s") => {
            let text = state
                .value
                .parse::<usize>()
                .ok()
                .and_then(|index| shared_strings.get(index).cloned())
                .unwrap_or_default();
            CellContent::SharedString(state.value.clone(), text)
        }
        Some("inlineStr") => CellContent::InlineString(state.inline_text.clone()),
        Some("str") => CellContent::PlainString(state.value.clone()),
        Some("b") => CellContent::Bool(state.value.clone()),
        _ => {
            if state.value.is_empty() && state.inline_text.is_empty() {
                CellContent::Empty
            } else if !state.inline_text.is_empty() {
                CellContent::InlineString(state.inline_text.clone())
            } else {
                CellContent::Number(state.value.clone())
            }
        }
    };
    CellData {
        coord: state.coord,
        row: state.row,
        col: state.col,
        style: state.style,
        extra_attrs: state.extra_attrs,
        content,
        formula: state.formula,
    }
}

fn build_sheet_data_xml(rows: &BTreeMap<u32, RowData>) -> String {
    let mut out = String::new();
    out.push_str("<sheetData>");
    for row in rows.values() {
        out.push_str("<row");
        let mut has_r = false;
        for (key, value) in &row.attrs {
            if key == "r" {
                has_r = true;
            }
            out.push(' ');
            out.push_str(key);
            out.push_str("=\"");
            out.push_str(&xml_escape_attr(value));
            out.push('"');
        }
        if !has_r {
            out.push_str(&format!(" r=\"{}\"", row.row_index));
        }
        out.push('>');
        for cell in row.cells.values() {
            out.push_str(&build_cell_xml(cell));
        }
        out.push_str("</row>");
    }
    out.push_str("</sheetData>");
    out
}

fn build_cell_xml(cell: &CellData) -> String {
    let mut out = String::new();
    out.push_str("<c");
    out.push_str(&format!(" r=\"{}\"", cell.coord));
    if let Some(style) = &cell.style {
        out.push_str(&format!(" s=\"{}\"", xml_escape_attr(style)));
    }
    for (key, value) in &cell.extra_attrs {
        if key == "r" || key == "s" || key == "t" {
            continue;
        }
        out.push(' ');
        out.push_str(key);
        out.push_str("=\"");
        out.push_str(&xml_escape_attr(value));
        out.push('"');
    }

    match &cell.content {
        CellContent::InlineString(text) => {
            out.push_str(" t=\"inlineStr\">");
            out.push_str("<is><t xml:space=\"preserve\">");
            out.push_str(&xml_escape_text(text));
            out.push_str("</t></is>");
        }
        CellContent::SharedString(index, _) => {
            out.push_str(" t=\"s\">");
            if let Some(formula) = &cell.formula {
                out.push_str("<f>");
                out.push_str(&xml_escape_text(formula));
                out.push_str("</f>");
            }
            out.push_str("<v>");
            out.push_str(&xml_escape_text(index));
            out.push_str("</v>");
        }
        CellContent::PlainString(text) => {
            out.push_str(" t=\"str\">");
            if let Some(formula) = &cell.formula {
                out.push_str("<f>");
                out.push_str(&xml_escape_text(formula));
                out.push_str("</f>");
            }
            out.push_str("<v>");
            out.push_str(&xml_escape_text(text));
            out.push_str("</v>");
        }
        CellContent::Bool(value) => {
            out.push_str(" t=\"b\">");
            if let Some(formula) = &cell.formula {
                out.push_str("<f>");
                out.push_str(&xml_escape_text(formula));
                out.push_str("</f>");
            }
            out.push_str("<v>");
            out.push_str(&xml_escape_text(value));
            out.push_str("</v>");
        }
        CellContent::Number(value) => {
            out.push('>');
            if let Some(formula) = &cell.formula {
                out.push_str("<f>");
                out.push_str(&xml_escape_text(formula));
                out.push_str("</f>");
            }
            if !value.is_empty() {
                out.push_str("<v>");
                out.push_str(&xml_escape_text(value));
                out.push_str("</v>");
            }
        }
        CellContent::Empty => {
            out.push('>');
            if let Some(formula) = &cell.formula {
                out.push_str("<f>");
                out.push_str(&xml_escape_text(formula));
                out.push_str("</f>");
            }
        }
    }

    out.push_str("</c>");
    out
}

fn replace_sheet_data(xml: &str, replacement: &str) -> Result<String, String> {
    let start = xml
        .find("<sheetData>")
        .ok_or_else(|| "sheetData start not found".to_string())?;
    let end = xml
        .find("</sheetData>")
        .ok_or_else(|| "sheetData end not found".to_string())?;
    let end_index = end + "</sheetData>".len();
    let mut out = String::with_capacity(xml.len() + replacement.len());
    out.push_str(&xml[..start]);
    out.push_str(replacement);
    out.push_str(&xml[end_index..]);
    Ok(out)
}

fn read_zip_entry(archive: &mut ZipArchive<File>, name: &str) -> Result<String, String> {
    let mut entry = archive
        .by_name(name)
        .map_err(|e| format!("read zip entry '{}' failed: {}", name, e))?;
    let mut text = String::new();
    entry
        .read_to_string(&mut text)
        .map_err(|e| format!("read zip entry '{}' content failed: {}", name, e))?;
    Ok(text)
}

fn read_optional_zip_entry(
    archive: &mut ZipArchive<File>,
    name: &str,
) -> Option<Result<String, String>> {
    match archive.by_name(name) {
        Ok(mut entry) => {
            let mut text = String::new();
            Some(
                entry
                    .read_to_string(&mut text)
                    .map(|_| text)
                    .map_err(|e| format!("read zip entry '{}' content failed: {}", name, e)),
            )
        }
        Err(_) => None,
    }
}

fn attr_value(event: &quick_xml::events::BytesStart<'_>, key: &[u8]) -> Option<String> {
    event
        .attributes()
        .flatten()
        .find(|attr| attr.key.as_ref() == key)
        .map(|attr| String::from_utf8_lossy(attr.value.as_ref()).to_string())
}

fn collect_attrs(event: &quick_xml::events::BytesStart<'_>) -> Vec<(String, String)> {
    event
        .attributes()
        .flatten()
        .map(|attr| {
            (
                String::from_utf8_lossy(attr.key.as_ref()).to_string(),
                String::from_utf8_lossy(attr.value.as_ref()).to_string(),
            )
        })
        .collect()
}

fn collect_extra_cell_attrs(event: &quick_xml::events::BytesStart<'_>) -> Vec<(String, String)> {
    event
        .attributes()
        .flatten()
        .filter_map(|attr| {
            let key = String::from_utf8_lossy(attr.key.as_ref()).to_string();
            if key == "r" || key == "s" || key == "t" {
                None
            } else {
                Some((
                    key,
                    String::from_utf8_lossy(attr.value.as_ref()).to_string(),
                ))
            }
        })
        .collect()
}

fn extend_merged_followers(followers: &mut HashSet<String>, range_ref: &str) {
    let Some((start, end)) = range_ref.split_once(':') else {
        return;
    };
    let Ok((start_row, start_col)) = parse_cell_ref(start) else {
        return;
    };
    let Ok((end_row, end_col)) = parse_cell_ref(end) else {
        return;
    };
    for row in start_row..=end_row {
        for col in start_col..=end_col {
            let coord = format!("{}{}", column_letter(col), row);
            if coord != start {
                followers.insert(coord);
            }
        }
    }
}
