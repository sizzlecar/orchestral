#!/usr/bin/env python3
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


def error(message: str) -> Dict[str, Any]:
    return {"type": "error", "message": message}


def success(exports: Dict[str, Any]) -> Dict[str, Any]:
    return {"type": "success", "exports": exports}


def read_params(req: Dict[str, Any]) -> Dict[str, Any]:
    input_obj = req.get("input") or {}
    return input_obj.get("params") or {}


def normalize_extra_args(value: Any) -> List[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("extra_args must be an array of strings")
    out: List[str] = []
    for item in value:
        if not isinstance(item, str):
            raise ValueError("extra_args must contain only strings")
        trimmed = item.strip()
        if not trimmed:
            continue
        out.append(trimmed)
    return out


def normalize_to_format(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return "gfm-raw_html"
    # Prefer standards-friendly markdown output without raw html blocks.
    if raw in {"md", "markdown", "gfm"}:
        return "gfm-raw_html"
    return raw


def parse_headings(markdown: str) -> List[Dict[str, Any]]:
    headings: List[Dict[str, Any]] = []
    for idx, line in enumerate(markdown.splitlines(), start=1):
        if not line.startswith("#"):
            continue
        match = re.match(r"^(#+)\s+(.*)$", line.strip())
        if not match:
            continue
        headings.append(
            {"line": idx, "level": len(match.group(1)), "title": match.group(2).strip()}
        )
    return headings


def summarize_text(markdown: str, max_sentences: int) -> str:
    text = re.sub(r"\s+", " ", markdown).strip()
    if not text:
        return ""
    parts = re.split(r"(?<=[.!?])\s+", text)
    return " ".join(parts[: max(1, max_sentences)]).strip()


def build_outline(headings: List[Dict[str, Any]]) -> str:
    if not headings:
        return "- (No headings found)"
    lines = []
    for item in headings:
        indent = "  " * max(0, item["level"] - 1)
        lines.append(f"{indent}- {item['title']}")
    return "\n".join(lines)


def build_generated_markdown(source_path: str, markdown: str, mode: str, summary_sentences: int) -> str:
    headings = parse_headings(markdown)
    summary = summarize_text(markdown, summary_sentences)
    if mode == "outline":
        return "# Document Outline\n\n" + build_outline(headings) + "\n"
    if mode == "overview":
        word_count = len([w for w in re.split(r"\s+", markdown.strip()) if w])
        return (
            "# Document Overview\n\n"
            f"- Source: {source_path}\n"
            f"- Word count: {word_count}\n"
            f"- Heading count: {len(headings)}\n\n"
            "## Summary\n\n"
            f"{summary}\n"
        )
    return "# Generated Summary\n\n" + summary + "\n\n## Key Sections\n\n" + build_outline(headings) + "\n"


def run_pandoc(
    source_path: str,
    to_format: str,
    from_format: Optional[str],
    output_path: Optional[str],
    extra_args: List[str],
) -> Dict[str, Any]:
    command = ["pandoc", source_path, "--to", to_format]
    if from_format:
        command.extend(["--from", from_format])
    command.extend(extra_args)
    if output_path:
        command.extend(["--output", output_path])

    proc = subprocess.run(
        command,
        capture_output=True,
        text=True,
    )

    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        detail = stderr or stdout or f"exit code {proc.returncode}"
        raise RuntimeError(f"pandoc failed: {detail}")

    content = "" if output_path else proc.stdout
    return {
        "content": content,
        "stderr": (proc.stderr or "").strip(),
        "command": " ".join(command),
    }


def doc_convert(params: Dict[str, Any]) -> Dict[str, Any]:
    source_path = params.get("source_path")
    if not source_path or not isinstance(source_path, str):
        return error("doc_convert requires params.source_path")
    if not Path(source_path).exists():
        return error(f"source file not found: {source_path}")

    if shutil.which("pandoc") is None:
        return error("pandoc is not installed or not found in PATH")

    mode = str(params.get("mode") or "convert").strip().lower()
    from_format = params.get("from_format")
    from_format = str(from_format).strip() if from_format else None
    to_format = normalize_to_format(params.get("to_format") or "gfm")
    target_path = params.get("target_path")
    target_path = str(target_path).strip() if target_path else None
    summary_sentences = int(params.get("summary_sentences") or 5)

    try:
        extra_args = normalize_extra_args(params.get("extra_args"))
    except ValueError as exc:
        return error(str(exc))

    try:
        if mode in {"summary", "outline", "overview"}:
            # Build generated markdown using pandoc as the parser.
            parsed = run_pandoc(
                source_path=source_path,
                to_format="markdown",
                from_format=from_format,
                output_path=None,
                extra_args=extra_args,
            )
            generated = build_generated_markdown(
                source_path=source_path,
                markdown=parsed["content"],
                mode=mode,
                summary_sentences=summary_sentences,
            )

            wrote_file = False
            bytes_written = None
            if target_path:
                target = Path(target_path)
                if target.parent:
                    target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(generated, encoding="utf-8")
                wrote_file = True
                bytes_written = target.stat().st_size

            return success(
                {
                    "source_path": source_path,
                    "mode": mode,
                    "from_format": from_format,
                    "to_format": "markdown",
                    "target_path": target_path if target_path else None,
                    "wrote_file": wrote_file,
                    "bytes": bytes_written,
                    "generated_markdown": generated,
                    "pandoc_command": parsed["command"],
                }
            )

        converted = run_pandoc(
            source_path=source_path,
            to_format=to_format,
            from_format=from_format,
            output_path=target_path,
            extra_args=extra_args,
        )
        wrote_file = bool(target_path)
        bytes_written = None
        if target_path and Path(target_path).exists():
            bytes_written = Path(target_path).stat().st_size

        return success(
            {
                "source_path": source_path,
                "mode": "convert",
                "from_format": from_format,
                "to_format": to_format,
                "target_path": target_path if target_path else None,
                "wrote_file": wrote_file,
                "bytes": bytes_written,
                "content": converted["content"],
                "pandoc_command": converted["command"],
            }
        )
    except Exception as exc:
        return error(str(exc))


def dispatch(req: Dict[str, Any]) -> Dict[str, Any]:
    action = req.get("action") or {}
    entrypoint = action.get("entrypoint") or action.get("name")
    params = read_params(req)

    if entrypoint == "doc_convert":
        return doc_convert(params)
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
