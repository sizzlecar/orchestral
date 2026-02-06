#!/usr/bin/env python3
import json
import importlib.metadata as metadata
import re
import site
import sys
from pathlib import Path
from typing import Any, Dict, List


MARKDOWN_EXTENSIONS = {".md", ".markdown"}
MARKITDOWN_CAPABILITY_NOTE = (
    "markitdown-based transform supports source_document -> markdown only"
)


def error(message: str) -> Dict[str, Any]:
    return {"type": "error", "message": message}


def success(exports: Dict[str, Any]) -> Dict[str, Any]:
    return {"type": "success", "exports": exports}


def load_markitdown():
    try:
        import markitdown as markitdown_pkg  # type: ignore
    except ModuleNotFoundError as exc:
        user_site = site.getusersitepackages()
        raise RuntimeError(
            "markitdown import failed: module not found. "
            f"python={sys.executable}, user_site={user_site}. "
            "Install with the same interpreter: "
            f"'{sys.executable} -m pip install --user markitdown'"
        ) from exc
    except Exception as exc:
        raise RuntimeError(
            "markitdown import failed with runtime error: "
            f"{exc} (python={sys.executable})"
        ) from exc
    markitdown_cls = getattr(markitdown_pkg, "MarkItDown", None)
    if markitdown_cls is None:
        try:
            version = metadata.version("markitdown")
        except Exception:
            version = "unknown"
        raise RuntimeError(
            "installed markitdown package is incompatible "
            f"(version={version}): missing MarkItDown class. "
            "Install Microsoft markitdown from GitHub: "
            f"'{sys.executable} -m pip install -U git+https://github.com/microsoft/markitdown.git'"
        )
    return markitdown_cls()


def read_params(req: Dict[str, Any]) -> Dict[str, Any]:
    input_obj = req.get("input") or {}
    return input_obj.get("params") or {}


def convert_to_markdown(converter: Any, source_path: str) -> str:
    result = converter.convert(source_path)
    if hasattr(result, "text_content") and result.text_content:
        return str(result.text_content)
    if hasattr(result, "markdown") and result.markdown:
        return str(result.markdown)
    if hasattr(result, "content") and result.content:
        return str(result.content)
    return str(result)


def parse_headings(markdown: str) -> List[Dict[str, Any]]:
    headings: List[Dict[str, Any]] = []
    for idx, line in enumerate(markdown.splitlines(), start=1):
        if not line.startswith("#"):
            continue
        m = re.match(r"^(#+)\s+(.*)$", line.strip())
        if not m:
            continue
        headings.append(
            {"line": idx, "level": len(m.group(1)), "title": m.group(2).strip()}
        )
    return headings


def summarize_text(markdown: str, max_sentences: int) -> str:
    text = re.sub(r"\s+", " ", markdown).strip()
    if not text:
        return ""
    parts = re.split(r"(?<=[.!?])\s+", text)
    selected = parts[: max(1, max_sentences)]
    return " ".join(selected).strip()


def build_outline(headings: List[Dict[str, Any]]) -> str:
    if not headings:
        return "- (No headings found)"
    lines: List[str] = []
    for item in headings:
        indent = "  " * max(0, item["level"] - 1)
        lines.append(f"{indent}- {item['title']}")
    return "\n".join(lines)


def doc_parse(converter: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    source_path = params.get("source_path")
    if not source_path:
        return error("doc_parse requires params.source_path")

    markdown = convert_to_markdown(converter, source_path)
    headings = parse_headings(markdown)
    words = [w for w in re.split(r"\s+", markdown.strip()) if w]

    return success(
        {
            "source_path": source_path,
            "markdown": markdown,
            "headings": headings,
            "word_count": len(words),
            "char_count": len(markdown),
        }
    )


def doc_transform(converter: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    source_path = params.get("source_path")
    if not source_path:
        return error("doc_transform requires params.source_path")

    markdown = convert_to_markdown(converter, source_path)
    target_path = params.get("target_path")
    wrote_file = False
    if target_path:
        target = Path(target_path)
        if target.suffix.lower() not in MARKDOWN_EXTENSIONS:
            return error(
                "doc_transform only writes markdown files (.md/.markdown). "
                "For PDF/DOCX generation, use another toolchain."
            )
        if target.parent:
            target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(markdown, encoding="utf-8")
        wrote_file = True

    return success(
        {
            "source_path": source_path,
            "target_path": target_path if target_path else None,
            "wrote_file": wrote_file,
            "markdown": markdown,
            "supported_conversions": ["source_document -> markdown"],
            "capability_note": MARKITDOWN_CAPABILITY_NOTE,
        }
    )


def doc_generate(converter: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    source_path = params.get("source_path")
    if not source_path:
        return error("doc_generate requires params.source_path")

    mode = (params.get("mode") or "summary").lower()
    markdown = convert_to_markdown(converter, source_path)
    headings = parse_headings(markdown)
    summary_sentences = int(params.get("summary_sentences") or 5)
    summary_text = summarize_text(markdown, summary_sentences)
    outline = build_outline(headings)
    word_count = len([w for w in re.split(r"\s+", markdown.strip()) if w])

    if mode == "outline":
        generated = "# Document Outline\n\n" + outline + "\n"
    elif mode == "overview":
        generated = (
            "# Document Overview\n\n"
            f"- Source: {source_path}\n"
            f"- Word count: {word_count}\n"
            f"- Heading count: {len(headings)}\n\n"
            "## Summary\n\n"
            f"{summary_text}\n"
        )
    else:
        generated = (
            "# Generated Summary\n\n"
            f"{summary_text}\n\n"
            "## Key Sections\n\n"
            f"{outline}\n"
        )

    target_path = params.get("target_path")
    wrote_file = False
    if target_path:
        target = Path(target_path)
        if target.parent:
            target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(generated, encoding="utf-8")
        wrote_file = True

    return success(
        {
            "source_path": source_path,
            "mode": mode,
            "target_path": target_path if target_path else None,
            "wrote_file": wrote_file,
            "generated_markdown": generated,
        }
    )


def dispatch(req: Dict[str, Any]) -> Dict[str, Any]:
    action = req.get("action") or {}
    entrypoint = action.get("entrypoint") or action.get("name")
    params = read_params(req)
    converter = load_markitdown()

    if entrypoint == "doc_parse":
        return doc_parse(converter, params)
    if entrypoint == "doc_transform":
        return doc_transform(converter, params)
    if entrypoint == "doc_generate":
        return doc_generate(converter, params)
    return error(f"unknown entrypoint: {entrypoint}")


def main() -> int:
    try:
        request = json.load(sys.stdin)
        response = dispatch(request)
    except Exception as exc:
        response = error(f"plugin failed: {exc}")

    json.dump(response, sys.stdout, ensure_ascii=False)
    sys.stdout.write("\n")
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
