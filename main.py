#!/usr/bin/env python3

"""
Monday C3 PO Extractor Toolkit.

- extract_c3_po_numbers: Given item_id(s), extracts PO numbers from the "Purchase Order Number"
  column in update body tables.
- extract_c3_appointment_details: Fetches first N rows from the C3 board where Row Type is empty,
  and extracts 6 fields (Appointment number, Client, Consignee, Appointment Date and time,
  C3 Response, PO's) from Sobeys or Loblaw email format.
"""

import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

# Filename for saving extracted appointment items (in user Downloads)
NEW_RECORDS_FILENAME = "new_records.txt"


CAPABILITY_NAME = "extract_c3_po_numbers"
CAPABILITY_APPOINTMENT = "extract_c3_appointment_details"
MONDAY_API_URL = "https://api.monday.com/v2"

# C3 board and group (BOT: C3 Appointment Management)
DEFAULT_BOARD_ID = 18392325187
DEFAULT_GROUP_NAME = "C3 Inbox"
ROW_TYPE_COLUMN_TITLE = "Row Type"
DEV_LIMIT_ROWS = 2
# Loblaw "past 2 weeks" filter: column for date (board-specific)
LOBLAW_DATE_COLUMN_ID = "pulse_log_mkypz8ad"
ROW_TYPE_NEW = "New"
ROW_TYPE_UPDATE = "Update"

# Column titles on the C3 board for populating extracted values
COL_APPOINTMENT_NUMBER = "Appointment #"
COL_CLIENT = "Client"
COL_CONSIGNEE = "Consignee"
COL_APPOINTMENT_DATE = "Appointment Date"
COL_REQUESTED_DELIVERY_APPOINTMENT = "Requested Delivery Appointment"
COL_PO = "PO"
COL_C3_RESPONSE = "C3 Response"

# C3 Response: raw label from email -> Monday column label (status)
# Sobeys
C3_RESPONSE_MAP = {
    "reservation approval": "Approved",
    "update": "Update",
    "no show appointment": "No show",
    "missing/incorrect paperwork": "Missing/Incorrect Paperwork",
    "signed paperwork": "Signed Paperwork",
    # Loblaw
    "appointment confirmation approved": "Approved",
    "appointment cancellation request approved": "Cancelled",
    "appointment rejection": "Rejected",
    "no show notification": "No Show",
    "amendment accepted": "Amendment Accepted",
}

# Column header to look for in update body tables (case-insensitive)
PO_COLUMN_HEADER = "purchase order number"


def _map_c3_response_to_column_label(raw: Optional[str]) -> Optional[str]:
    """Map raw C3 response from email to Monday C3 Response column label. Match is case-insensitive, stripped."""
    if not raw or not isinstance(raw, str):
        return None
    key = raw.strip().lower()
    if not key:
        return None
    return C3_RESPONSE_MAP.get(key) or C3_RESPONSE_MAP.get(key.replace("  ", " "))


def _get_downloads_path() -> Path:
    """Return the user's Downloads folder (Windows or Mac/Linux)."""
    if os.name == "nt":
        # Windows: USERPROFILE or fallback to HOME
        base = os.environ.get("USERPROFILE") or os.environ.get("HOME") or os.path.expanduser("~")
    else:
        base = os.environ.get("HOME") or os.path.expanduser("~")
    return Path(base) / "Downloads"


def _save_items_to_downloads(items: List[Dict[str, Any]], filename: str = NEW_RECORDS_FILENAME) -> str:
    """
    Save items as JSON text to a file in the user's Downloads folder.
    Returns the absolute path of the saved file.
    """
    folder = _get_downloads_path()
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2, ensure_ascii=False)
    return str(path.resolve())


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


# ---- Sobeys subject: "Client - C3 Response [for|:] APPT_NO on DATE for Consignee" ----
# Example 1: Sobeys - Update for 46554663 on 2026/01/14 05:00 AST for Oromocto RSC29
# Example 2: Sobeys - Reservation Approval: 46578951 on 2026/01/14 02:50 CST for Winnipeg RSC08
# C3 Response can be followed by " for " or ":" (optional space after colon) then number
SOBEYS_SUBJECT_RE = re.compile(
    r"^(.+?)\s+-\s+(.+?)\s*(?:for\s+|:\s*)(\d+)\s+on\s+(.+?)\s+for\s+(.+)$",
    re.IGNORECASE | re.DOTALL,
)


def _parse_sobeys_subject(subject: str) -> Dict[str, Optional[str]]:
    """
    Parse Sobeys email subject.
    Returns dict: appointment_number, client, consignee, appointment_date_time, c3_response, po_numbers (None; filled from body).
    """
    out = {
        "appointment_number": None,
        "client": None,
        "consignee": None,
        "appointment_date_time": None,
        "c3_response": None,
        "po_numbers": None,
    }
    if not subject or not isinstance(subject, str):
        return out
    subject = subject.strip()
    m = SOBEYS_SUBJECT_RE.match(subject)
    if not m:
        return out
    client, c3_response, appt_no, date_time, consignee = m.groups()
    out["client"] = (client or "").strip()
    out["c3_response"] = (c3_response or "").strip()
    out["appointment_number"] = (appt_no or "").strip()
    out["appointment_date_time"] = (date_time or "").strip()
    out["consignee"] = (consignee or "").strip()
    return out


# ---- Loblaw subject: "C3 Response - from Client Appointing: ..." ----
# Example: Appointment Confirmation Approved - from Loblaw Appointing: for Matrix Calgary DC on 16/01/2026 14:30 MST
LOBLAW_C3_CLIENT_RE = re.compile(
    r"^(.+?)\s+-\s+from\s+(.+?)(?:\s+Appointing|\s*:|\s*$)",
    re.IGNORECASE,
)


def _parse_loblaw_subject(subject: str) -> Dict[str, Optional[str]]:
    """Parse Loblaw subject for C3 Response and Client. Rest from body."""
    out = {
        "appointment_number": None,
        "client": None,
        "consignee": None,
        "appointment_date_time": None,
        "c3_response": None,
        "po_numbers": None,
    }
    if not subject or not isinstance(subject, str):
        return out
    subject = subject.strip()
    m = LOBLAW_C3_CLIENT_RE.match(subject)
    if m:
        c3_response, client = m.groups()
        out["c3_response"] = (c3_response or "").strip()
        out["client"] = (client or "").strip()
    return out


# ---- Loblaw body: key-value pairs (Reference #, Scheduled Date, Warehouse, PO(s)) ----
def _parse_loblaw_body(body: str) -> Dict[str, Optional[str]]:
    """Extract Reference #, Scheduled Date, Warehouse, PO(s) from Loblaw email body (plain or HTML-stripped)."""
    out = {
        "appointment_number": None,
        "appointment_date_time": None,
        "consignee": None,
        "po_numbers": None,
    }
    if not body or not isinstance(body, str):
        return out
    text = _strip_html(body).replace("\r\n", "\n")
    # Reference #: 9920891 (digits only to avoid trailing junk)
    ref_m = re.search(r"Reference\s*#\s*:\s*(\d+)", text, re.IGNORECASE)
    if ref_m:
        out["appointment_number"] = ref_m.group(1).strip()
    # Scheduled Date: 16/01/2026 14:30 MST - capture only the date/time pattern to avoid rest of email
    sched_m = re.search(
        r"Scheduled\s+Date\s*:\s*(\d{2}/\d{2}/\d{4}\s+\d{1,2}:\d{2}\s+[A-Z]{3,4})",
        text,
        re.IGNORECASE,
    )
    if sched_m:
        out["appointment_date_time"] = sched_m.group(1).strip()
    # Site: Matrix Calgary -> Consignee (Loblaw uses Site, not Warehouse)
    site_m = re.search(
        r"Site\s*:\s*([^\n]+?)(?=\s+Warehouse\s*:|\s{2,}|\n|PO\s*\(|Comments:|$)",
        text,
        re.IGNORECASE,
    )
    if site_m:
        out["consignee"] = site_m.group(1).strip()
    # PO(s): 0008909460,0008909797 - also collect from table if present
    po_m = re.search(r"PO\s*\(\s*s\s*\)\s*:\s*([\d,\s]+)", text, re.IGNORECASE)
    if not po_m:
        po_m = re.search(r"PO\s*:\s*([\d,\s]+)", text, re.IGNORECASE)
    if po_m:
        raw = re.sub(r"\s+", "", po_m.group(1))
        out["po_numbers"] = [p for p in raw.split(",") if p]
    return out


def _merge_loblaw_po_from_table(body: str, from_body: Optional[List[str]]) -> List[str]:
    """Combine PO(s) from body key-value and from PO table; dedupe."""
    from_table = _extract_po_numbers_from_update_body(body)
    combined = list(from_body or []) + from_table
    seen: Set[str] = set()
    result: List[str] = []
    for p in combined:
        p = (p or "").strip()
        if p and p not in seen:
            seen.add(p)
            result.append(p)
    return result


def _fetch_board_columns(api_token: str, board_id: int) -> List[Dict[str, Any]]:
    """Fetch board columns (id, title, type) to resolve column id by title and format values."""
    query = """
    query BoardColumns($boardIds: [ID!]!) {
      boards(ids: $boardIds) {
        columns { id title type }
      }
    }
    """
    result = _monday_graphql(api_token, query, {"boardIds": [board_id]})
    boards = result.get("data", {}).get("boards") or []
    if not boards:
        return []
    return boards[0].get("columns") or []


def _column_ids_by_title(columns: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Build map: column title (normalized lower) -> {id, type} for lookup."""
    by_title: Dict[str, Dict[str, Any]] = {}
    for col in columns or []:
        title = (col.get("title") or "").strip()
        if not title:
            continue
        by_title[title.lower()] = {"id": (col.get("id") or "").strip(), "type": (col.get("type") or "").lower()}
    return by_title


def _parse_appointment_datetime_for_monday(value: Optional[str]) -> Dict[str, str]:
    """Convert '2026/01/14 02:50 CST' to Monday date format {date, time}. Returns {} if invalid.
    Monday API expects time as HH:mm:ss (with seconds)."""
    if not value or not isinstance(value, str):
        return {}
    value = value.strip()
    # Match DD/MM/YYYY or YYYY/MM/DD and optional time (HH:mm or HH:mm:ss)
    m = re.match(r"(\d{4})/(\d{2})/(\d{2})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?", value)
    if m:
        y, mo, d, h, mi, sec = m.groups()
        time_str = f"{int(h):02d}:{mi}:{(sec or '00')}"
        return {"date": f"{y}-{mo}-{d}", "time": time_str}
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?", value)
    if m:
        d, mo, y, h, mi, sec = m.groups()
        time_str = f"{int(h):02d}:{mi}:{(sec or '00')}"
        return {"date": f"{y}-{mo}-{d}", "time": time_str}
    return {}


def _fetch_items_page(
    api_token: str,
    board_id: int,
    limit: int = 100,
    cursor: Optional[str] = None,
    include_column_values: bool = True,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Fetch one page of items from board with name, group, and optionally column_values."""
    item_fields = "id name group { id title }"
    if include_column_values:
        item_fields += " column_values { id text }"
    if cursor:
        query = f"""
        query NextItems($cursor: String!) {{
          next_items_page(cursor: $cursor) {{
            cursor
            items {{ {item_fields} }}
          }}
        }}
        """
        result = _monday_graphql(api_token, query, {"cursor": cursor})
        page = result.get("data", {}).get("next_items_page") or {}
    else:
        query = f"""
        query BoardItems($boardIds: [ID!]!, $limit: Int!) {{
          boards(ids: $boardIds) {{
            items_page(limit: $limit) {{
              cursor
              items {{ {item_fields} }}
            }}
          }}
        }}
        """
        result = _monday_graphql(api_token, query, {"boardIds": [board_id], "limit": min(limit, 500)})
        boards = result.get("data", {}).get("boards") or []
        page = (boards[0].get("items_page") or {}) if boards else {}
    items = page.get("items") or []
    return items, page.get("cursor")


def _get_column_value_text(item: Dict[str, Any], column_title: str) -> Optional[str]:
    """Get column value text by column title. Requires column_values to have title (legacy)."""
    for cv in item.get("column_values") or []:
        if (cv.get("title") or "").strip().lower() == column_title.strip().lower():
            t = (cv.get("text") or "").strip()
            return t if t else None
    return None


def _get_column_value_text_by_id(
    column_values: List[Dict[str, Any]], column_id: str
) -> Optional[str]:
    """Get column value text by column id (Monday API returns id, text only on ColumnValue)."""
    for cv in column_values or []:
        if (cv.get("id") or "").strip().lower() == column_id.strip().lower():
            t = (cv.get("text") or "").strip()
            return t if t else None
    return None


def _fetch_column_values_for_items(
    api_token: str,
    item_ids: List[str],
) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch column_values for given item IDs. Returns item_id -> list of column_values."""
    if not item_ids:
        return {}
    ids_arg = json.dumps([int(i) for i in item_ids])
    query = f"""
    query {{
      items(ids: {ids_arg}) {{
        id
        column_values {{ id text }}
      }}
    }}
    """
    result = _monday_graphql(api_token, query)
    items = result.get("data", {}).get("items") or []
    return {str(i["id"]): (i.get("column_values") or []) for i in items}


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


def _update_row_type_column(
    api_token: str,
    board_id: int,
    item_id: int,
    column_id: str,
    value: str,
) -> Dict[str, Any]:
    """Set Row Type column to 'New' or 'Update'. Uses change_multiple_column_values."""
    column_values = {column_id: value}
    mutation = """
    mutation ChangeColumn($board_id: ID!, $item_id: ID!, $column_values: JSON!) {
      change_multiple_column_values(
        board_id: $board_id,
        item_id: $item_id,
        column_values: $column_values
      ) { id }
    }
    """
    variables = {
        "board_id": board_id,
        "item_id": item_id,
        "column_values": json.dumps(column_values),
    }
    return _monday_graphql(api_token, mutation, variables)


def _update_item_with_extracted_columns(
    api_token: str,
    board_id: int,
    item_id: int,
    column_by_title: Dict[str, Dict[str, Any]],
    extracted: Dict[str, Any],
    row_type_value: str,
) -> None:
    """
    Populate Monday columns for one item: Appointment #, Client, Consignee,
    Appointment Date, Requested Delivery Appointment, PO, Row Type.
    """
    payload: Dict[str, Any] = {}

    def _set(col_title: str, value: Any) -> None:
        info = column_by_title.get(col_title.lower())
        if not info or not info.get("id"):
            return
        cid = info["id"]
        col_type = (info.get("type") or "").lower()
        if value is None:
            return
        # Only send {"date", "time"} for columns that are actually date type; text columns get the raw string
        if "date" in (col_type or ""):
            if isinstance(value, str):
                dt = _parse_appointment_datetime_for_monday(value)
                payload[cid] = dt if dt else value
            elif isinstance(value, dict) and "date" in value:
                payload[cid] = value
            else:
                payload[cid] = str(value).strip() if value else ""
        elif isinstance(value, list):
            payload[cid] = ", ".join(str(v) for v in value if v)
        else:
            payload[cid] = str(value).strip() if value else ""

    _set(COL_APPOINTMENT_NUMBER, extracted.get("appointment_number"))
    _set(COL_CLIENT, extracted.get("client"))
    _set(COL_CONSIGNEE, extracted.get("consignee"))
    _set(COL_APPOINTMENT_DATE, extracted.get("appointment_date_time"))
    _set(COL_REQUESTED_DELIVERY_APPOINTMENT, extracted.get("appointment_date_time"))
    _set(COL_PO, extracted.get("po_numbers") or [])

    # C3 Response is status type - map raw label to column label and set
    c3_response_label = _map_c3_response_to_column_label(extracted.get("c3_response"))
    if c3_response_label:
        c3_cid = (column_by_title.get(COL_C3_RESPONSE.lower()) or {}).get("id")
        if c3_cid:
            payload[c3_cid] = {"label": c3_response_label}

    # Row Type is status type (color_mkzek1eh) - use label format
    row_type_cid = None
    for title_lower, info in column_by_title.items():
        if title_lower == ROW_TYPE_COLUMN_TITLE.lower() and info.get("id"):
            row_type_cid = info["id"]
            break
    if row_type_cid:
        payload[row_type_cid] = {"label": row_type_value}

    if not payload:
        return
    mutation = """
    mutation ChangeColumns($board_id: ID!, $item_id: ID!, $column_values: JSON!) {
      change_multiple_column_values(
        board_id: $board_id,
        item_id: $item_id,
        column_values: $column_values
      ) { id }
    }
    """
    variables = {
        "board_id": board_id,
        "item_id": item_id,
        "column_values": json.dumps(payload),
    }
    _monday_graphql(api_token, mutation, variables)


def _count_items_with_name_containing(
    api_token: str,
    board_id: int,
    search_text: str,
) -> int:
    """Count board items whose name contains search_text (e.g. Sobeys appointment number). Paginates."""
    if not search_text or not search_text.strip():
        return 0
    total = 0
    cursor = None
    while True:
        items_page, cursor = _fetch_items_page(
            api_token, board_id, limit=500, cursor=cursor, include_column_values=False
        )
        for item in items_page:
            if search_text.strip() in (item.get("name") or ""):
                total += 1
        if not cursor or not items_page:
            break
    return total


def _fetch_loblaw_items_past_weeks(
    api_token: str,
    board_id: int,
    weeks: int = 2,
) -> List[Dict[str, Any]]:
    """Fetch items where name contains 'Loblaw' and created within the last N weeks. Fallback: all Loblaw by name."""
    try:
        query = """
        query LoblawItems($boardIds: [ID!]!, $limit: Int!, $queryParams: QueryParamsInput) {
          boards(ids: $boardIds) {
            items_page(limit: $limit, query_params: $queryParams) {
              cursor
              items { id name }
            }
          }
        }
        """
        query_params = {
            "rules": [
                {"column_id": "name", "compare_value": ["Loblaw"], "operator": "contains_text"},
                {
                    "column_id": LOBLAW_DATE_COLUMN_ID,
                    "compare_value": [str(weeks), "WEEKS"],
                    "compare_attribute": "CREATED_AT",
                    "operator": "within_the_last",
                },
            ],
            "operator": "and",
        }
        variables = {
            "boardIds": [board_id],
            "limit": 500,
            "queryParams": query_params,
        }
        result = _monday_graphql(api_token, query, variables)
        boards = result.get("data", {}).get("boards") or []
        page = (boards[0].get("items_page") or {}) if boards else {}
        items = page.get("items") or []
        all_items = list(items)
        cursor = page.get("cursor")
        while cursor and items:
            next_result = _monday_graphql(
                api_token,
                """
                query NextLoblaw($cursor: String!) {
                  next_items_page(cursor: $cursor) { cursor items { id name } }
                }
                """,
                {"cursor": cursor},
            )
            next_page = next_result.get("data", {}).get("next_items_page") or {}
            items = next_page.get("items") or []
            all_items.extend(items)
            cursor = next_page.get("cursor")
        return all_items
    except Exception:
        all_items = []
        cursor = None
        while True:
            items_page, cursor = _fetch_items_page(
                api_token, board_id, limit=500, cursor=cursor, include_column_values=False
            )
            for item in items_page:
                if "Loblaw" in (item.get("name") or ""):
                    all_items.append(item)
            if not cursor or not items_page:
                break
        return all_items


def _get_loblaw_reference_number_counts(
    api_token: str,
    board_id: int,
    weeks: int = 2,
) -> Dict[str, int]:
    """For Loblaw items in past N weeks, extract Reference # from each item's updates; return ref_number -> count."""
    loblaw_items = _fetch_loblaw_items_past_weeks(api_token, board_id, weeks)
    if not loblaw_items:
        return {}
    item_ids = [str(i["id"]) for i in loblaw_items]
    updates_by_item: Dict[str, List[Dict[str, Any]]] = {}
    for i in range(0, len(item_ids), 50):
        updates_by_item.update(_fetch_updates_for_items(api_token, item_ids[i : i + 50]))
    counts: Dict[str, int] = {}
    for iid in item_ids:
        for upd in updates_by_item.get(iid) or []:
            body = (upd.get("body") or "").strip()
            if not body:
                continue
            ref_m = re.search(r"Reference\s*#\s*:\s*(\d+)", body, re.IGNORECASE)
            if ref_m:
                ref = ref_m.group(1).strip()
                counts[ref] = counts.get(ref, 0) + 1
    return counts


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


def extract_c3_appointment_details(
    api_token: Optional[str] = None,
    board_id: Optional[int] = None,
    group_name: Optional[str] = None,
    limit: int = DEV_LIMIT_ROWS,
) -> Dict[str, Any]:
    """
    Fetches items from the C3 board where Row Type is empty (first N rows in dev),
    and extracts 6 fields per item: appointment_number, client, consignee,
    appointment_date_time, c3_response, po_numbers. Supports Sobeys and Loblaw email formats.
    """
    try:
        token = _get_api_token(api_token)
        bid = board_id if board_id is not None else DEFAULT_BOARD_ID
        group_filter = (group_name or DEFAULT_GROUP_NAME).strip()
        max_items = max(1, min(limit, 50))

        # Resolve column ids from board (for Row Type filter and for populating extracted values)
        columns = _fetch_board_columns(token, bid)
        row_type_column_id: Optional[str] = None
        for col in columns:
            if (col.get("title") or "").strip().lower() == ROW_TYPE_COLUMN_TITLE.lower():
                row_type_column_id = (col.get("id") or "").strip()
                break
        column_by_title = _column_ids_by_title(columns)

        # Fetch items from board (id, name, group only); then fetch column_values to filter Row Type empty
        group_candidates: List[Dict[str, Any]] = []
        cursor = None
        while len(group_candidates) < max(max_items, 50):
            items_page, cursor = _fetch_items_page(
                token, bid, limit=100, cursor=cursor, include_column_values=False
            )
            for item in items_page:
                g = item.get("group") or {}
                if (g.get("title") or "").strip() != group_filter:
                    continue
                group_candidates.append(item)
            if not cursor or not items_page:
                break

        # Batch fetch column_values for candidates (id, text only), then filter Row Type empty
        candidate_ids = [str(i["id"]) for i in group_candidates[:50]]
        cv_by_id = _fetch_column_values_for_items(token, candidate_ids)
        all_candidates: List[Dict[str, Any]] = []
        for item in group_candidates:
            iid = str(item.get("id", ""))
            cvs = cv_by_id.get(iid) or []
            if row_type_column_id:
                row_type_text = _get_column_value_text_by_id(cvs, row_type_column_id)
            else:
                row_type_text = None
            if row_type_text is not None and str(row_type_text).strip() != "":
                continue
            all_candidates.append(item)
            if len(all_candidates) >= max_items:
                break

        items_to_process = all_candidates[:max_items]
        if not items_to_process:
            return {
                "result": {
                    "items": [],
                    "message": "No items found with Row Type empty in group.",
                    "saved_path": "",
                },
                "capability": CAPABILITY_APPOINTMENT,
            }

        item_ids = [str(i["id"]) for i in items_to_process]
        updates_by_item = {}
        for i in range(0, len(item_ids), 50):
            batch = item_ids[i : i + 50]
            updates_by_item.update(_fetch_updates_for_items(token, batch))

        # Precompute Loblaw Reference # counts (past 2 weeks) for New vs Update
        loblaw_ref_counts = _get_loblaw_reference_number_counts(token, bid, weeks=2)

        results: List[Dict[str, Any]] = []
        for item in items_to_process:
            iid = str(item.get("id", ""))
            name = (item.get("name") or "").strip()
            updates = updates_by_item.get(iid) or []
            body = ""
            if updates:
                body = (updates[0].get("body") or "").strip()
                if not body and len(updates) > 1:
                    body = (updates[1].get("body") or "").strip()

            # Detect client and parse
            appointment_number: Optional[str] = None
            client: Optional[str] = None
            consignee: Optional[str] = None
            appointment_date_time: Optional[str] = None
            c3_response: Optional[str] = None
            po_numbers: List[str] = []

            if "Sobeys" in name or (name and name.strip().lower().startswith("sobeys")):
                parsed = _parse_sobeys_subject(name)
                appointment_number = parsed.get("appointment_number")
                client = parsed.get("client")
                consignee = parsed.get("consignee")
                appointment_date_time = parsed.get("appointment_date_time")
                c3_response = parsed.get("c3_response")
                for upd in updates:
                    b = (upd.get("body") or "").strip()
                    if b:
                        po_numbers.extend(_extract_po_numbers_from_update_body(b))
                seen_po: Set[str] = set()
                unique_pos: List[str] = []
                for p in po_numbers:
                    if p not in seen_po:
                        seen_po.add(p)
                        unique_pos.append(p)
                po_numbers = unique_pos
            else:
                parsed_subj = _parse_loblaw_subject(name)
                parsed_body = _parse_loblaw_body(body)
                c3_response = parsed_subj.get("c3_response")
                client = parsed_subj.get("client")
                appointment_number = parsed_body.get("appointment_number")
                appointment_date_time = parsed_body.get("appointment_date_time")
                consignee = parsed_body.get("consignee")
                po_numbers = _merge_loblaw_po_from_table(body, parsed_body.get("po_numbers"))

            # Determine Row Type: New (appointment number appears once) or Update (repeats)
            row_type_value = ROW_TYPE_NEW
            appt = (appointment_number or "").strip()
            if "Sobeys" in name or (name and name.strip().lower().startswith("sobeys")):
                count = _count_items_with_name_containing(token, bid, appt) if appt else 0
                row_type_value = ROW_TYPE_NEW if count <= 1 else ROW_TYPE_UPDATE
            else:
                count = loblaw_ref_counts.get(appt, 0)
                row_type_value = ROW_TYPE_NEW if count <= 1 else ROW_TYPE_UPDATE

            extracted_row = {
                "appointment_number": appointment_number,
                "client": client,
                "consignee": consignee,
                "appointment_date_time": appointment_date_time,
                "c3_response": c3_response,
                "po_numbers": po_numbers,
            }

            # Populate all columns on Monday: Appointment #, Client, Consignee, Appointment Date,
            # Requested Delivery Appointment, PO, Row Type
            try:
                _update_item_with_extracted_columns(
                    token, bid, int(iid), column_by_title, extracted_row, row_type_value
                )
            except Exception as update_err:
                row_type_value = f"{row_type_value} (update failed: {update_err})"

            results.append({
                "item_id": iid,
                "appointment_number": appointment_number,
                "client": client,
                "consignee": consignee,
                "appointment_date_time": appointment_date_time,
                "c3_response": c3_response,
                "po_numbers": po_numbers,
                "row_type": row_type_value,
            })

        # Save items to Downloads/new_records.txt and return path
        saved_path = ""
        if results:
            try:
                saved_path = _save_items_to_downloads(results, NEW_RECORDS_FILENAME)
            except Exception as save_err:
                saved_path = f"(save failed: {save_err})"

        return {
            "result": {
                "items": results,
                "count": len(results),
                "saved_path": saved_path,
            },
            "capability": CAPABILITY_APPOINTMENT,
        }
    except Exception as e:
        return {
            "error": str(e),
            "capability": CAPABILITY_APPOINTMENT,
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
        elif capability == CAPABILITY_APPOINTMENT:
            result = extract_c3_appointment_details(
                api_token=args.get("api_token"),
                board_id=args.get("board_id"),
                group_name=args.get("group_name"),
                limit=int(args.get("limit") or DEV_LIMIT_ROWS),
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
