#!/usr/bin/env python3
"""
Project describer using constrained tool calls (rg, read, tree) plus optional
README image understanding and browser fallback.

Environment variables:
- OPENAI_API_KEY: OpenAI API key

Optional environment variables:
- PROJECT_DESCRIBER_MODEL: text model (default: gpt-5.2)
- PROJECT_DESCRIBER_VISION_MODEL: vision model (default: gpt-4.1-mini)
- PROJECT_DESCRIBER_MAX_TOOL_CALLS: max tool calls (default: 18)
- PROJECT_DESCRIBER_CONFIDENCE_THRESHOLD: browser fallback threshold (default: 0.65)
- PROJECT_DESCRIBER_MAX_IMAGES: max README images to analyze (default: 4)

Usage:
  python scripts/ysws_project_describer.py --repo <repo_url_or_path> \
    --live-url <optional_live_url> --out <optional_output_json>
"""

import argparse
import base64
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from urllib.parse import unquote, urlparse
import textwrap
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, Field


DEFAULT_MODEL = os.getenv("PROJECT_DESCRIBER_MODEL", "gpt-5.2")
DEFAULT_VISION_MODEL = os.getenv("PROJECT_DESCRIBER_VISION_MODEL", "gpt-4.1-mini")
DEFAULT_MAX_TOOL_CALLS = int(os.getenv("PROJECT_DESCRIBER_MAX_TOOL_CALLS", "18"))
DEFAULT_CONFIDENCE_THRESHOLD = float(
    os.getenv("PROJECT_DESCRIBER_CONFIDENCE_THRESHOLD", "0.65")
)
DEFAULT_MAX_IMAGES = int(os.getenv("PROJECT_DESCRIBER_MAX_IMAGES", "4"))
LOG_TRUNCATE = int(os.getenv("PROJECT_DESCRIBER_LOG_TRUNCATE", "0"))


def estimate_cost_usd(model: str, usage: Dict[str, int]) -> Optional[float]:
    if model == "gpt-5.2":
        non_cached = usage.get("non_cached_tokens", 0)
        cached = usage.get("cached_tokens", 0)
        output = usage.get("output_tokens", 0)
        non_cached_cost = non_cached * 1.75 / 1_000_000
        cached_cost = cached * 0.175 / 1_000_000
        output_cost = output * 14.0 / 1_000_000
        return non_cached_cost + cached_cost + output_cost
    return None


def update_usage(usage: Dict[str, int], response: Any) -> None:
    resp_usage = getattr(response, "usage", None)
    if not resp_usage:
        return
    input_tokens = getattr(resp_usage, "input_tokens", 0)
    output_tokens = getattr(resp_usage, "output_tokens", 0)
    cached_tokens = 0
    if hasattr(resp_usage, "input_tokens_details"):
        cached_tokens = getattr(resp_usage.input_tokens_details, "cached_tokens", 0)
    non_cached_tokens = input_tokens - cached_tokens
    usage["input_tokens"] = usage.get("input_tokens", 0) + input_tokens
    usage["output_tokens"] = usage.get("output_tokens", 0) + output_tokens
    usage["cached_tokens"] = usage.get("cached_tokens", 0) + cached_tokens
    usage["non_cached_tokens"] = usage.get("non_cached_tokens", 0) + non_cached_tokens
    usage["total_tokens"] = usage.get("total_tokens", 0) + input_tokens + output_tokens

    log(
        "📊 Usage update: "
        f"input={input_tokens}, output={output_tokens}, cached={cached_tokens}, "
        f"non_cached={non_cached_tokens}"
    )


def parse_first_json(raw: str) -> Dict[str, Any]:
    raw = raw.strip()
    if not raw:
        raise json.JSONDecodeError("Empty JSON", raw, 0)
    decoder = json.JSONDecoder()
    if raw[0] != "{":
        first = raw.find("{")
        if first == -1:
            raise json.JSONDecodeError("No JSON object found", raw, 0)
        raw = raw[first:]
    obj, end = decoder.raw_decode(raw)
    rest = raw[end:].strip()
    if rest:
        log("⚠️ Extra content after JSON object was ignored.")
    return obj

READ_MAX_LINES = 400
READ_WRAP_WIDTH = 120
RG_MAX_RESULTS = 200
TREE_MAX_ENTRIES = 2000


class ProjectSummary(BaseModel):
    name: Optional[str] = None
    description: str
    tags: List[str] = Field(default_factory=list)
    stack: List[str] = Field(default_factory=list)
    key_features: List[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)


def fetch_project_from_airtable(record_id: str, airtable_token: str) -> Dict[str, Any]:
    from pyairtable import Api

    if not airtable_token:
        raise ValueError("No AIRTABLE_PERSONAL_ACCESS_TOKEN provided")

    api = Api(airtable_token)
    base_id = "app3A5kJwYqxMLOgh"
    table_id = "tblzWWGUYHVH7Zyqf"
    table = api.table(base_id, table_id)

    record = table.get(record_id)
    fields = record.get("fields", {})
    return {
        "live_url": fields.get("Playable URL", ""),
        "code_url": fields.get("Code URL", ""),
        "first_name": fields.get("First Name", ""),
        "last_name": fields.get("Last Name", ""),
        "author_country": fields.get("Geocoded - Country", ""),
        "record_id": record_id,
    }


def run_cmd(cmd: List[str], cwd: Optional[Path] = None) -> Tuple[int, str, str]:
    result = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    return result.returncode, result.stdout, result.stderr


def log(msg: str) -> None:
    print(msg, flush=True)


def safe_path(root: Path, candidate: str) -> Path:
    root = root.resolve()
    path = (root / candidate).resolve()
    try:
        path.relative_to(root)
    except ValueError as e:
        raise ValueError(f"path outside of root: {candidate}") from e
    return path


def tool_read(root: Path, path: str, offset: int = 1, limit: int = 200, wrap: int = READ_WRAP_WIDTH) -> str:
    if offset < 1:
        offset = 1
    if limit < 1:
        limit = 1
    if limit > READ_MAX_LINES:
        limit = READ_MAX_LINES
    target = safe_path(root, path)
    if not target.exists() or not target.is_file():
        return f"ERROR: file not found: {path}"
    try:
        text = target.read_text(errors="replace")
    except Exception as e:
        return f"ERROR: failed to read {path}: {e}"
    lines = text.splitlines()
    start = offset - 1
    end = min(start + limit, len(lines))
    selected = lines[start:end]

    output_lines: List[str] = []
    line_no = offset
    for line in selected:
        wrapped = [line]
        if wrap and len(line) > wrap:
            wrapped = textwrap.wrap(line, width=wrap, replace_whitespace=False, drop_whitespace=False)
        if not wrapped:
            wrapped = [""]
        output_lines.append(f"{line_no:6d} | {wrapped[0]}")
        for cont in wrapped[1:]:
            output_lines.append("       | " + cont)
        line_no += 1

    suffix = ""
    if end < len(lines):
        suffix = f"\n... (truncated, total lines: {len(lines)})"
    return "\n".join(output_lines) + suffix


def tool_tree(root: Path, path: str = ".", offset: int = 1, limit: int = 200) -> str:
    if offset < 1:
        offset = 1
    if limit < 1:
        limit = 1
    base = safe_path(root, path)
    if not base.exists() or not base.is_dir():
        return f"ERROR: directory not found: {path}"

    entries: List[str] = []
    for dirpath, dirnames, filenames in os.walk(base):
        # Exclude .git directories entirely
        dirnames[:] = [d for d in dirnames if d != ".git"]
        rel_dir = Path(dirpath).relative_to(root)
        for d in sorted(dirnames):
            entries.append(str(rel_dir / d) + "/")
        for f in sorted(filenames):
            if f == ".git":
                continue
            entries.append(str(rel_dir / f))

    entries = sorted(set(entries))
    total = len(entries)
    if total > TREE_MAX_ENTRIES:
        entries = entries[:TREE_MAX_ENTRIES]

    start = offset - 1
    end = min(start + limit, len(entries))
    sliced = entries[start:end]
    suffix = ""
    if end < total:
        suffix = f"\n... (truncated, total entries: {total})"
    return "\n".join(sliced) + suffix


def tool_rg(root: Path, pattern: str, path: str = ".", max_results: int = RG_MAX_RESULTS) -> str:
    base = safe_path(root, path)
    if not base.exists():
        return f"ERROR: path not found: {path}"
    cmd = ["rg", "--line-number", "--no-heading", pattern, str(base)]
    code, out, err = run_cmd(cmd, cwd=root)
    if code not in (0, 1):
        return f"ERROR: rg failed: {err.strip()}"
    lines = out.splitlines()
    if not lines:
        return "NO_MATCHES"
    if len(lines) > max_results:
        lines = lines[:max_results] + [f"... (truncated, total matches: {len(lines)})"]
    return "\n".join(lines)


def find_readme(root: Path) -> Optional[Path]:
    candidates = sorted(root.glob("README*"))
    if not candidates:
        candidates = sorted(root.glob("readme*"))
    for c in candidates:
        if c.is_file():
            return c
    return None


IMAGE_MD_RE = re.compile(r"!\[(?P<alt>[^\]]*)\]\((?P<url>[^)]+)\)")
IMAGE_HTML_RE = re.compile(r"<img[^>]*src=[\"'](?P<url>[^\"']+)[\"'][^>]*>", re.IGNORECASE)


def extract_readme_images(readme_path: Path, text: str) -> List[Dict[str, str]]:
    images: List[Dict[str, str]] = []
    for match in IMAGE_MD_RE.finditer(text):
        images.append({"alt": match.group("alt").strip(), "url": match.group("url").strip()})
    for match in IMAGE_HTML_RE.finditer(text):
        images.append({"alt": "", "url": match.group("url").strip()})

    # Deduplicate by url
    seen = set()
    unique = []
    for img in images:
        if img["url"] in seen:
            continue
        seen.add(img["url"])
        unique.append(img)

    # Resolve relative paths to local files when possible
    resolved = []
    for img in unique:
        url = img["url"]
        if not re.match(r"^https?://", url):
            local = (readme_path.parent / url).resolve()
            if local.exists():
                resolved.append({"alt": img["alt"], "url": str(local)})
            else:
                resolved.append(img)
        else:
            resolved.append(img)
    return resolved


def select_images(images: List[Dict[str, str]], max_images: int) -> List[Dict[str, str]]:
    if not images:
        return []

    def score(img: Dict[str, str]) -> int:
        alt = (img.get("alt") or "").lower()
        url = img.get("url") or ""
        score = 0
        for kw in ["pcb", "schematic", "wiring", "board", "hardware", "sensor", "robot", "prototype"]:
            if kw in alt:
                score += 2
            if kw in url.lower():
                score += 1
        return score

    ranked = sorted(images, key=score, reverse=True)
    return ranked[:max_images]


def load_image_bytes(image_url_or_path: str, max_bytes: int = 1_500_000) -> Optional[Tuple[bytes, str]]:
    if re.match(r"^https?://", image_url_or_path):
        try:
            resp = requests.get(image_url_or_path, timeout=10)
            resp.raise_for_status()
            data = resp.content
        except Exception:
            return None
    else:
        try:
            data = Path(image_url_or_path).read_bytes()
        except Exception:
            return None
    if len(data) > max_bytes:
        return None

    ext = image_url_or_path.lower()
    if ext.endswith(".png"):
        mime = "image/png"
    elif ext.endswith(".jpg") or ext.endswith(".jpeg"):
        mime = "image/jpeg"
    elif ext.endswith(".webp"):
        mime = "image/webp"
    else:
        mime = "image/png"
    return data, mime


def describe_images(
    client: OpenAI,
    images: List[Dict[str, str]],
    model: str,
    usage_accum: Dict[str, int],
) -> List[Dict[str, str]]:
    summaries = []
    for img in images:
        blob = load_image_bytes(img["url"])
        if not blob:
            continue
        data, mime = blob
        b64 = base64.b64encode(data).decode("ascii")
        prompt = (
            "Describe this image in 1-2 sentences. Focus only on clues about "
            "what the project is, and hardware/software signals (components, boards, devices)."
        )
        response = client.responses.create(
            model=model,
            input=[
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": prompt},
                        {"type": "input_image", "image_url": f"data:{mime};base64,{b64}"},
                    ],
                }
            ],
            max_output_tokens=200,
        )
        update_usage(usage_accum, response)
        summaries.append(
            {
                "url": img["url"],
                "alt": img.get("alt", ""),
                "summary": response.output_text.strip(),
            }
        )
    return summaries


def git_clone(repo: str) -> Tuple[Path, Optional[Path]]:
    if os.path.isdir(repo):
        return Path(repo).resolve(), None

    temp_root = Path(tempfile.mkdtemp(prefix="ysws_project_describer_")).resolve()
    target = temp_root / "repo"
    log(f"📥 Cloning repo into temp dir: {target}")
    code, out, err = run_cmd(["git", "clone", "--depth", "1", repo, str(target)])
    if code != 0:
        shutil.rmtree(temp_root, ignore_errors=True)
        raise RuntimeError(f"git clone failed: {err.strip() or out.strip()}")
    return target, temp_root


def derive_repo_and_subdir(repo_input: str, live_url: str) -> Tuple[str, Optional[str]]:
    """
    Returns (repo_url, subdir) from a code_url/live_url.
    Supports GitHub blob/tree/raw URLs and GitHub Pages.
    """
    repo_input = repo_input.strip()
    if not repo_input:
        return repo_input, None

    parsed = urlparse(repo_input)
    if parsed.scheme in ("http", "https"):
        host = parsed.netloc.lower()
        path = unquote(parsed.path or "")

        if host == "github.com":
            parts = [p for p in path.split("/") if p]
            if len(parts) >= 2:
                owner, repo = parts[0], parts[1]
                repo_url = f"https://github.com/{owner}/{repo}"
                subdir = None
                if len(parts) >= 4 and parts[2] in ("blob", "tree"):
                    subpath = "/".join(parts[4:])
                    if subpath:
                        subdir = subpath
                return repo_url, subdir

        if host == "raw.githubusercontent.com":
            parts = [p for p in path.split("/") if p]
            if len(parts) >= 3:
                owner, repo = parts[0], parts[1]
                repo_url = f"https://github.com/{owner}/{repo}"
                subdir = "/".join(parts[3:]) if len(parts) > 3 else None
                return repo_url, subdir

        if host.endswith(".github.io"):
            owner = host.split(".")[0]
            repo_url = f"https://github.com/{owner}/{owner}.github.io"
            return repo_url, None

    return repo_input, None


def is_probable_git_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return False
    host = parsed.netloc.lower()
    if host in ("github.com", "gitlab.com", "bitbucket.org"):
        return True
    if parsed.path.endswith(".git"):
        return True
    return False


def can_git_clone(url: str) -> bool:
    if not is_probable_git_url(url):
        return False
    code, out, err = run_cmd(["git", "ls-remote", url], cwd=None)
    return code == 0


def normalize_subdir(repo_root: Path, subdir: Optional[str]) -> Optional[str]:
    if not subdir:
        return None
    # If subdir looks like a file path, use its parent directory.
    if re.search(r"\\.[A-Za-z0-9]+$", subdir):
        parent = str(Path(subdir).parent)
        if parent == ".":
            return None
        subdir = parent
    candidate = (repo_root / subdir).resolve()
    if candidate.exists() and candidate.is_dir():
        return subdir
    parent = str(Path(subdir).parent)
    if parent != ".":
        return parent
    return None


def build_system_prompt(root: Path) -> str:
    return f"""You are an expert coding assistant. You help with project inspection tasks by using the available tools.

Available tools:
- rg: search code with ripgrep (pattern, optional path)
- read: read file contents (path, offset line number, limit lines, wrap width)
- tree: list file tree (optional folder, offset, limit)

Rules:
- You can only access files within this root: {root}
- Use as few tool calls as possible.
- Prefer tree -> read README -> targeted rg searches -> read key files.
- You MUST use tools before answering. Start by calling tree on ".".
- If you are confident after inspecting, stop without extra tool calls.
- Return exactly one action per response.
- Do not describe URLs or links themselves. Describe the project.

Tool protocol:
- You must respond with JSON only.
- For a tool call, respond with:
  {{"action":"tool","tool":"tree|read|rg","args":{{...}}}}
- For the final answer, respond with:
  {{"action":"final","summary":{{...}}}}

Output:
- Return ONLY a JSON object matching the protocol above.
"""


TOOL_DEFS = [
    {
        "type": "function",
        "name": "rg",
        "description": "Search within the repository using ripgrep.",
        "parameters": {
            "type": "object",
            "properties": {
                "pattern": {"type": "string"},
                "path": {"type": "string", "default": "."},
                "max_results": {"type": "integer", "default": RG_MAX_RESULTS},
            },
            "required": ["pattern"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "read",
        "description": "Read a file. offset is 1-based line number, limit is number of lines.",
        "parameters": {
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "offset": {"type": "integer", "default": 1},
                "limit": {"type": "integer", "default": 200},
                "wrap": {"type": "integer", "default": READ_WRAP_WIDTH},
            },
            "required": ["path"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "tree",
        "description": "List files and folders under a directory. offset is 1-based index.",
        "parameters": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "default": "."},
                "offset": {"type": "integer", "default": 1},
                "limit": {"type": "integer", "default": 200},
            },
            "additionalProperties": False,
        },
    },
]


def run_agent(
    client: OpenAI,
    root: Path,
    image_summaries: List[Dict[str, str]],
    model: str,
    max_tool_calls: int,
    usage_accum: Dict[str, int],
) -> ProjectSummary:
    log("🤖 Starting agent loop")
    system_prompt = build_system_prompt(root)
    schema = ProjectSummary.model_json_schema()
    image_block = ""
    if image_summaries:
        image_lines = [f"- {s['summary']} (alt: {s.get('alt','')})" for s in image_summaries]
        image_block = "README image summaries:\n" + "\n".join(image_lines)

    tool_action_schema = {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["tool"]},
            "tool": {"type": "string", "enum": ["rg", "read", "tree"]},
            "args_json": {"type": "string"},
        },
        "required": ["action", "tool", "args_json"],
        "additionalProperties": False,
    }
    final_action_schema = {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["final"]},
            "summary": ProjectSummary.model_json_schema(),
        },
        "required": ["action", "summary"],
        "additionalProperties": False,
    }
    protocol_schema = {"anyOf": [tool_action_schema, final_action_schema]}

    user_prompt = (
        "Inspect this repo and produce a detailed project summary.\n"
        f"Repo root: {root}\n"
        "Tags must be a single flat list. Include 'hardware' and/or 'software' if present.\n"
        "Include stack tags when clear (e.g., nextjs, godot, arduino, esp32, python, rust).\n"
        "Provide a detailed overview: 3-5 sentences in description and 4-7 key_features.\n"
        "Do NOT describe the links themselves. Use page content to describe the project.\n"
        "Be token-efficient and avoid unnecessary tool calls.\n"
        "IMPORTANT: Respond with JSON only using the tool protocol. Output exactly ONE JSON object per response.\n"
        "Example tool call:\n"
        '{"action":"tool","tool":"tree","args_json":"{\\"path\\":\\".\\",\\"offset\\":1,\\"limit\\":200}"}\n'
        "Example final:\n"
        '{"action":"final","summary":{"name":null,"description":"...","tags":[],"stack":[],"key_features":[],"confidence":0.5}}\n'
        f"JSON Schema:\n{json.dumps(schema)}\n"
    )
    if image_block:
        user_prompt += "\n" + image_block

    messages: List[Dict[str, Any]] = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    tool_calls_used = 0
    while True:
        response = client.responses.create(
            model=model,
            input=messages,
            text={"format": {"type": "json_object"}},
            max_output_tokens=900,
        )
        update_usage(usage_accum, response)

        raw = (response.output_text or "").strip()
        if not raw:
            raise RuntimeError("Empty response from model")

        try:
            payload = parse_first_json(raw)
        except json.JSONDecodeError:
            log("⚠️ Model response was not JSON. Requesting repair.")
            log(raw[:2000] + ("..." if len(raw) > 2000 else ""))
            repair_prompt = (
                "Your previous response was not valid JSON. "
                "Return ONLY valid JSON using the protocol described. One action per response."
            )
            messages.append({"role": "user", "content": repair_prompt})
            continue

        action = payload.get("action")
        if action == "final":
            log("✅ Model returned final summary")
            summary = payload.get("summary", {})
            return ProjectSummary.model_validate(summary)

        if action != "tool":
            log("⚠️ Model returned invalid action, asking again")
            messages.append(
                {
                    "role": "user",
                    "content": "Invalid action. Use action=tool or action=final. Return JSON only.",
                }
            )
            continue

        if tool_calls_used >= max_tool_calls:
            log("⚠️ Tool budget exceeded, forcing final summary")
            messages.append(
                {"role": "user", "content": "Tool call budget exceeded. Provide final summary now."}
            )
            continue

        tool_name = payload.get("tool")
        args_json = payload.get("args_json")
        if not tool_name or args_json is None:
            messages.append(
                {
                    "role": "user",
                    "content": "Tool action must include both tool and args_json fields.",
                }
            )
            continue
        try:
            args = json.loads(args_json)
        except json.JSONDecodeError:
            messages.append(
                {
                    "role": "user",
                    "content": "args_json must be valid JSON object. Try again.",
                }
            )
            continue

        tool_calls_used += 1
        log(f"🔧 TOOL CALL {tool_calls_used}/{max_tool_calls}: {tool_name} {json.dumps(args)}")

        if tool_name == "rg":
            if "pattern" not in args:
                messages.append(
                    {
                        "role": "user",
                        "content": "rg requires args_json with at least {\"pattern\": \"...\"}. Try again.",
                    }
                )
                continue
            result = tool_rg(root, **args)
        elif tool_name == "read":
            if "path" not in args:
                messages.append(
                    {
                        "role": "user",
                        "content": "read requires args_json with at least {\"path\": \"...\"}. Try again.",
                    }
                )
                continue
            result = tool_read(root, **args)
        elif tool_name == "tree":
            result = tool_tree(root, **args)
        else:
            result = f"ERROR: unknown tool: {tool_name}"

        if LOG_TRUNCATE and len(result) > LOG_TRUNCATE:
            log(f"📄 TOOL RESULT (truncated to {LOG_TRUNCATE} chars)")
            log(result[:LOG_TRUNCATE] + "...")
        else:
            log("📄 TOOL RESULT")
            log(result)

        tool_result_msg = (
            "TOOL_RESULT\n"
            f"tool: {tool_name}\n"
            f"args: {json.dumps(args)}\n"
            f"output:\n{result}"
        )
        messages.append({"role": "assistant", "content": raw})
        messages.append({"role": "user", "content": tool_result_msg})


def maybe_browser_fallback(
    client: OpenAI,
    urls: List[str],
    model: str,
    existing: ProjectSummary,
    confidence_threshold: float,
    usage_accum: Dict[str, int],
    force: bool = False,
) -> Optional[ProjectSummary]:
    if not force and existing.confidence >= confidence_threshold:
        return None
    urls = [u for u in urls if u]
    if not urls:
        return None
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        log(f"⚠️ Browser fallback unavailable (playwright): {e}")
        return None

    log(f"🌐 Running browser fallback (confidence {existing.confidence:.2f} < {confidence_threshold:.2f})")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        snapshots = []
        try:
            for url in urls:
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=12000)
                    title = page.title()
                    description = page.locator("meta[name='description']").get_attribute("content") or ""
                    text = page.evaluate("() => document.body ? document.body.innerText : ''") or ""
                    text = re.sub(r"\\s+", " ", text).strip()
                    text = text[:4000]
                    snapshots.append(
                        {
                            "url": url,
                            "title": title,
                            "description": description,
                            "text": text,
                        }
                    )
                except Exception:
                    continue
        finally:
            browser.close()

    if not snapshots:
        log("⚠️ Browser fallback produced no snapshots")
        return None

    schema = ProjectSummary.model_json_schema()
    snapshot_blocks = []
    for snap in snapshots:
        snapshot_blocks.append(
            "URL: {url}\nTitle: {title}\nMeta: {description}\nText: {text}".format(**snap)
        )
    snapshot_text = "\n\n".join(snapshot_blocks)
    prompt = (
        "You are refining a project summary using live website snapshots.\n"
        f"Existing summary JSON:\n{existing.model_dump_json()}\n\n"
        f"Snapshots:\n{snapshot_text}\n\n"
        "Update the summary JSON if needed. Make the description 3-5 sentences and include 4-7 key_features.\n"
        "Do NOT describe the links themselves. Use page content to describe the project.\n"
        "Keep tags as a single flat list.\n"
        f"JSON Schema:\n{json.dumps(schema)}"
    )
    response = client.responses.create(
        model=model,
        input=[{"role": "user", "content": prompt}],
        text={"format": {"type": "json_object"}},
        max_output_tokens=600,
    )
    update_usage(usage_accum, response)
    data = json.loads(response.output_text)
    return ProjectSummary.model_validate(data)




def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--record-id", default="", help="Airtable record ID from YSWS DB")
    parser.add_argument("--repo", default="", help="Repo URL or local path (overrides record id)")
    parser.add_argument("--live-url", default="", help="Live project URL for browser fallback")
    parser.add_argument("--out", default="", help="Output JSON path")
    args = parser.parse_args()

    project_meta: Dict[str, Any] = {}
    repo_input = args.repo
    live_url = args.live_url
    code_url = ""

    if args.record_id:
        airtable_token = os.getenv("AIRTABLE_PERSONAL_ACCESS_TOKEN")
        project_meta = fetch_project_from_airtable(args.record_id, airtable_token)
        if not repo_input:
            repo_input = project_meta.get("code_url") or project_meta.get("live_url") or ""
        code_url = project_meta.get("code_url") or ""
        if not live_url:
            live_url = project_meta.get("live_url") or ""

    if not repo_input:
        raise SystemExit("No repo provided and no repo found from Airtable record.")

    token_usage: Dict[str, int] = {}
    repo_url, subdir = derive_repo_and_subdir(repo_input, live_url)
    non_git_repo = False
    if can_git_clone(repo_url):
        repo_root, temp_root = git_clone(repo_url)
        subdir = normalize_subdir(repo_root, subdir)
        if subdir:
            candidate = (repo_root / subdir).resolve()
            if candidate.exists() and candidate.is_dir():
                repo_root = candidate
            else:
                log(f"⚠️ Subdir from URL not found: {subdir} (using repo root)")
    else:
        non_git_repo = True
        log(f"⚠️ Not a git repo URL; creating a stub repo for inspection: {repo_url}")
        temp_root = Path(tempfile.mkdtemp(prefix="ysws_project_describer_")).resolve()
        repo_root = temp_root / "repo"
        repo_root.mkdir(parents=True, exist_ok=True)
        stub = repo_root / "README.md"
        stub.write_text(
            "# Project Links\n"
            f"- Code URL: {repo_url}\n"
            f"- Live URL: {live_url}\n"
        )
    try:
        readme = find_readme(repo_root)
        image_summaries: List[Dict[str, str]] = []

        client = OpenAI()

        if readme:
            try:
                log(f"🖼️ Scanning README images: {readme}")
                readme_text = readme.read_text(errors="replace")
                images = extract_readme_images(readme, readme_text)
                selected = select_images(images, DEFAULT_MAX_IMAGES)
                if selected:
                    image_summaries = describe_images(
                        client, selected, DEFAULT_VISION_MODEL, token_usage
                    )
            except Exception:
                pass

        summary = run_agent(
            client=client,
            root=repo_root,
            image_summaries=image_summaries,
            model=DEFAULT_MODEL,
            max_tool_calls=DEFAULT_MAX_TOOL_CALLS,
            usage_accum=token_usage,
        )

        if live_url or code_url:
            refined = maybe_browser_fallback(
                client=client,
                urls=[code_url, live_url],
                model=DEFAULT_MODEL,
                existing=summary,
                confidence_threshold=DEFAULT_CONFIDENCE_THRESHOLD,
                usage_accum=token_usage,
                force=non_git_repo,
            )
            if refined:
                summary = refined
    finally:
        if temp_root:
            log(f"🧹 Cleaning up temp dir: {temp_root}")
            shutil.rmtree(temp_root, ignore_errors=True)

    out_payload = summary.model_dump()
    out_payload["model"] = DEFAULT_MODEL
    out_payload["vision_model"] = DEFAULT_VISION_MODEL
    out_payload["token_usage"] = token_usage
    out_payload["estimated_cost_usd"] = estimate_cost_usd(DEFAULT_MODEL, token_usage)
    out_json = json.dumps(out_payload, indent=2)
    if args.out:
        Path(args.out).write_text(out_json)
    print(out_json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
