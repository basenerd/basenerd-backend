import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import markdown
import yaml

ARTICLES_DIR = "articles"

MD_EXTENSIONS = [
    "fenced_code",
    "tables",
    "toc",
    "smarty",
]

def _parse_front_matter(raw: str) -> Tuple[Dict[str, Any], str]:
    """
    Supports YAML front matter delimited by '---' at file start.
    Returns (meta, markdown_body).
    """
    raw = raw.lstrip("\ufeff")  # handle BOM if present
    if raw.startswith("---"):
        parts = raw.split("---", 2)
        if len(parts) >= 3:
            meta_text = parts[1].strip()
            body = parts[2].lstrip()
            meta = yaml.safe_load(meta_text) or {}
            return meta, body
    return {}, raw

def _coerce_date(d: Any) -> str:
    """
    Accepts 'YYYY-MM-DD' string, datetime/date, or None.
    Returns 'YYYY-MM-DD' string for sorting/display.
    """
    if not d:
        return "1970-01-01"
    if isinstance(d, str):
        return d.strip()
    if hasattr(d, "strftime"):
        return d.strftime("%Y-%m-%d")
    return str(d)

def _slug_from_filename(filename: str) -> str:
    base = os.path.basename(filename)
    name, _ = os.path.splitext(base)
    return name

def load_articles() -> List[Dict[str, Any]]:
    """
    Loads all .md files in /articles and returns a list of article dicts.
    """
    if not os.path.isdir(ARTICLES_DIR):
        return []

    articles: List[Dict[str, Any]] = []

    for fname in os.listdir(ARTICLES_DIR):
        if not fname.lower().endswith(".md"):
            continue

        path = os.path.join(ARTICLES_DIR, fname)
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read()

        meta, body_md = _parse_front_matter(raw)

        title = meta.get("title") or _slug_from_filename(fname).replace("-", " ").title()
        slug = meta.get("slug") or _slug_from_filename(fname)
        date_str = _coerce_date(meta.get("date"))
        author = meta.get("author") or ""

        body_html = markdown.markdown(body_md, extensions=MD_EXTENSIONS, output_format="html5")

        articles.append({
            "title": title,
            "slug": slug,
            "date": date_str,
            "author": author,
            "content_html": body_html,
        })

    # newest first (string sort works for YYYY-MM-DD)
    articles.sort(key=lambda a: a.get("date") or "1970-01-01", reverse=True)
    return articles

def get_article(slug: str) -> Optional[Dict[str, Any]]:
    slug = (slug or "").strip()
    if not slug:
        return None
    for a in load_articles():
        if a.get("slug") == slug:
            return a
    return None
