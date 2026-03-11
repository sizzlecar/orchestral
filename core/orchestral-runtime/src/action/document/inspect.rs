use std::fs;
use std::path::Path;

use serde_json::{json, Value};

use super::model::inspect_document_content;

pub(super) fn inspect_documents(source_paths: &[String]) -> Result<Value, String> {
    let mut files = Vec::new();
    let mut total_todos = 0usize;
    let mut missing_titles = 0usize;

    for path in source_paths {
        let content = fs::read_to_string(path)
            .map_err(|err| format!("read document '{}' failed: {}", path, err))?;
        let file_info = inspect_document_content(Path::new(path), &content);
        total_todos += file_info.todo_count;
        if file_info.missing_title {
            missing_titles += 1;
        }
        files.push(json!({
            "path": path,
            "file_name": file_info.file_name,
            "stem": file_info.stem,
            "title": file_info.title,
            "missing_title": file_info.missing_title,
            "todo_count": file_info.todo_count,
            "todo_lines": file_info.todo_lines,
            "heading_count": file_info.heading_count,
            "headings": file_info.headings,
            "line_count": file_info.line_count,
            "content": content,
        }));
    }

    Ok(json!({
        "target_count": files.len(),
        "missing_title_count": missing_titles,
        "todo_count": total_todos,
        "files": files,
    }))
}
