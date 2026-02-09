#!/usr/bin/env python3

"""
Monday C3 PO Extractor Toolkit.

Given a Monday.com item_id, fetches the Updates (email) section for that item
and extracts all PO numbers from the "Purchase Order Number" column of any
table in the update body (e.g. table with columns "Purchase Order Number" and "Supplier").
"""

import json
import os
import re
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

import requests


CAPABILITY_NAME = "extract_c3_po_numbers"
MONDAY_API_URL = "https://api.monday.com/v2"

# Column header to look for in update body tables (case-insensitive)
PO_COLUMN_HEADER = "purchase order number"


def _get_api_token(api_token: Optional[str]) -> str:
    """Resolve Monday API token from args or environment."""
    token = api_token or os.environ.get("MONDAY_API_TOKEN")
    if not token:
        raise ValueError(
            "Monday API token is required. "
            "Pass 'api_token' in args or set MONDAY_API_TOKEN environment variable."
        )
    return token


def _monday_graphql(
    api_token: str,
    query: str,
    variables: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Execute a GraphQL request against Monday.com."""
    headers = {
        "Authorization": api_token,
        "Content-Type": "application/json",
    }
    payload: Dict[str, Any] = {"query": query}
    if variables:
        payload["variables"] = variables

    response = requests.post(MONDAY_API_URL, headers=headers, json=payload, timeout=60)
    try:
        data = response.json()
    except Exception:
        response.raise_for_status()
        raise

    if "errors" in data and data["errors"]:
        raise RuntimeError(f"Monday API error: {data['errors']}")
    return data


def _strip_html(html: str) -> str:
    """Remove HTML tags and decode entities for text content."""
    text = re.sub(r"<[^>]+>", " ", html or "")
    text = re.sub(r"&nbsp;", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"&amp;", "&", text, flags=re.IGNORECASE)
    text = re.sub(r"&lt;", "<", text, flags=re.IGNORECASE)
    text = re.sub(r"&gt;", ">", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_po_column_from_html_table(body: str) -> List[str]:
    """
    Parse HTML table(s) in body, find column with header 'Purchase Order Number',
    return all cell values from that column (order preserved, duplicates possible).
    """
    pos: List[str] = []
    # Find all <table>...</table> blocks and process each
    table_blocks = re.findall(r"<table[^>]*>(.*?)</table>", body, re.DOTALL | re.IGNORECASE)
    for block in table_blocks:
        rows: List[List[str]] = []
        row_matches = re.findall(r"<tr[^>]*>(.*?)</tr>", block, re.DOTALL | re.IGNORECASE)
        for row_html in row_matches:
            cells = re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", row_html, re.DOTALL | re.IGNORECASE)
            rows.append([_strip_html(c).strip() for c in cells])
        if not rows:
            continue
        headers = [h.lower().strip() for h in rows[0]]
        col_idx = -1
        for i, h in enumerate(headers):
            if PO_COLUMN_HEADER in h or "purchase order" in h:
                col_idx = i
                break
        if col_idx < 0:
            continue
        for row in rows[1:]:
            if col_idx < len(row):
                val = row[col_idx].strip()
                if val and re.match(r"^\d+$", val):
                    pos.append(val)
    return pos


def _extract_po_column_from_markdown_table(body: str) -> List[str]:
    """
    Parse markdown-style table (| col1 | col2 |). Find 'Purchase Order Number' column, return values.
    """
    pos: List[str] = []
    lines = [ln.strip() for ln in (body or "").splitlines()]
    for i, line in enumerate(lines):
        if not line.startswith("|") or "|" not in line[1:]:
            continue
        # Split by | and drop leading/trailing empty (e.g. "| A | B |" -> ["A", "B"])
        cells = [c.strip() for c in line.split("|")][1:-1]
        if not cells:
            continue
        headers = [h.lower().strip() for h in cells]
        col_idx = -1
        for j, h in enumerate(headers):
            if PO_COLUMN_HEADER in h or "purchase order" in h:
                col_idx = j
                break
        if col_idx < 0:
            continue
        # Data rows: next lines that look like | val | val |
        for j in range(i + 1, len(lines)):
            data_line = lines[j].strip()
            if not data_line.startswith("|"):
                break
            data_cells = [c.strip() for c in data_line.split("|")][1:-1]
            if len(data_cells) <= col_idx:
                continue
            val = data_cells[col_idx].strip()
            if not val:
                continue
            # Skip separator row (|---|---|)
            if re.match(r"^[\s\-:]+$", val):
                continue
            if re.match(r"^\d+$", val):
                pos.append(val)
        break
    return pos


def _extract_po_numbers_from_update_body(body: str) -> List[str]:
    """
    Extract all PO numbers from the 'Purchase Order Number' column in any table
    in the update body. Tries HTML table first, then markdown table.
    Returns list of PO strings (may contain duplicates; caller can dedupe).
    """
    if not body or not isinstance(body, str):
        return []
    body = body.strip()
    if not body:
        return []
    pos = _extract_po_column_from_html_table(body)
    if not pos:
        pos = _extract_po_column_from_markdown_table(body)
    return pos


def _fetch_updates_for_items(
    api_token: str,
    item_ids: List[str],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch updates (body text) for a list of item IDs.
    Returns map: item_id -> list of { id, body }.
    """
    if not item_ids:
        return {}
    # Monday API accepts multiple item ids
    ids_arg = json.dumps([int(i) for i in item_ids])
    query = f"""
    query {{
      items(ids: {ids_arg}) {{
        id
        updates(limit: 50) {{
          id
          body
        }}
      }}
    }}
    """
    result = _monday_graphql(api_token, query)
    items = result.get("data", {}).get("items") or []
    by_id: Dict[str, List[Dict[str, Any]]] = {}
    for item in items:
        iid = str(item.get("id", ""))
        by_id[iid] = item.get("updates") or []
    return by_id


def _extract_po_numbers_for_item(updates: List[Dict[str, Any]]) -> Tuple[List[str], int]:
    """
    Given a list of update dicts (each with 'body'), extract all PO numbers
    from the 'Purchase Order Number' column in any table. Returns (po_numbers, update_count).
    """
    all_pos: List[str] = []
    for upd in updates:
        body = (upd.get("body") or "").strip()
        if body:
            all_pos.extend(_extract_po_numbers_from_update_body(body))
    seen: Set[str] = set()
    po_numbers: List[str] = []
    for po in all_pos:
        if po not in seen:
            seen.add(po)
            po_numbers.append(po)
    return po_numbers, len(updates)


def extract_c3_po_numbers(
    item_id: Optional[int] = None,
    item_ids: Optional[List[int]] = None,
    api_token: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Fetches the Updates (email body) for the given Monday.com item(s), parses any
    table with a 'Purchase Order Number' column, and returns all PO numbers per item.

    Accepts either item_id (single) or item_ids (list). If both are provided, item_ids is used.

    Returns:
        result: { items: [ { item_id, po_numbers, update_count }, ... ] }
        or error key on failure.
    """
    try:
        if item_ids is not None and len(item_ids) > 0:
            ids = [int(i) for i in item_ids]
        elif item_id is not None:
            ids = [int(item_id)]
        else:
            return {
                "error": "Either item_id or item_ids is required",
                "capability": CAPABILITY_NAME,
            }

        token = _get_api_token(api_token)
        id_strs = [str(i) for i in ids]

        # Batch fetch updates (Monday may limit query size)
        batch_size = 50
        updates_by_item: Dict[str, List[Dict[str, Any]]] = {}
        for i in range(0, len(id_strs), batch_size):
            batch = id_strs[i : i + batch_size]
            updates_by_item.update(_fetch_updates_for_items(token, batch))

        items_result: List[Dict[str, Any]] = []
        for iid_str in id_strs:
            updates = updates_by_item.get(iid_str) or []
            po_numbers, update_count = _extract_po_numbers_for_item(updates)
            items_result.append({
                "item_id": iid_str,
                "po_numbers": po_numbers,
                "update_count": update_count,
            })

        return {
            "result": {"items": items_result},
            "capability": CAPABILITY_NAME,
        }
    except Exception as e:
        return {
            "error": str(e),
            "capability": CAPABILITY_NAME,
        }


def main() -> None:
    """Read JSON from stdin, dispatch by capability, output JSON to stdout."""
    try:
        input_data = json.load(sys.stdin)
        capability = input_data.get("capability")
        args = input_data.get("args", {})

        if capability == CAPABILITY_NAME:
            item_id = args.get("item_id")
            item_ids = args.get("item_ids")
            # If item_id was passed as a list (e.g. item_id: [1,2,3]), treat as item_ids
            if isinstance(item_id, list):
                if len(item_id) > 0:
                    item_ids = item_ids if item_ids is not None else item_id
                item_id = None
            has_single = item_id is not None
            has_multi = item_ids is not None and len(item_ids) > 0
            if not has_single and not has_multi:
                print(
                    json.dumps(
                        {"error": "Either item_id or item_ids is required", "capability": CAPABILITY_NAME},
                        indent=2,
                    )
                )
            else:
                ids_arg = [int(x) for x in item_ids] if has_multi else None
                single_arg = int(item_id) if has_single else None
                result = extract_c3_po_numbers(
                    item_id=single_arg,
                    item_ids=ids_arg,
                    api_token=args.get("api_token"),
                )
                print(json.dumps(result, indent=2))
        else:
            print(
                json.dumps(
                    {
                        "error": f"Unknown capability: {capability}",
                        "capability": capability or "unknown",
                    },
                    indent=2,
                )
            )
    except Exception as e:
        print(
            json.dumps(
                {
                    "error": str(e),
                    "capability": "unknown",
                },
                indent=2,
            )
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
