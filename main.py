#!/usr/bin/env python3

"""
Monday C3 PO Extractor Toolkit.

- extract_c3_po_numbers: Given item_id(s), extracts PO numbers from the "Purchase Order Number"
  column in update body tables.
- extract_c3_appointment_details: Fetches first N rows from the C3 board where Row Type is empty,
  and extracts appointment number, Client, Consignee, Appointment Date and time, C3 Response, POs
  from Sobeys or Loblaw email format. When config_path is provided, also looks up Altruos Shipment ID
  (per PO via shipments/search) and Altruos Load ID (per Shipment ID via movements/search/shipments)
  and populates those Monday columns; the output CSV includes shipment_id and load_id per PO row.
- start_extract_c3_appointment: Starts the same extraction in the background; returns job_id and
  status "started" immediately so agentic platforms can avoid timeouts. Poll check_job_status with job_id.
- check_job_status: Returns job status (started/running/completed/failed), progress, and when
  completed the final_result (items, count, saved_path).
"""

import csv
import json
import os
import re
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import requests

try:
    from requests_aws4auth import AWS4Auth
    _HAS_AWS4AUTH = True
except ImportError:
    _HAS_AWS4AUTH = False

# Filename for saving extracted appointment items (user Downloads) as CSV, one row per PO
NEW_RECORDS_FILENAME = "new_records.csv"

# CSV column headers (one row per PO; date=YYYYMMDD, delivery_time=HHMMSS)
CSV_HEADERS = [
    "item_id",
    "appointment_number",
    "client",
    "consignee",
    "appointment_date",
    "delivery_time",
    "c3_response",
    "po",
    "shipment_id",
    "load_id",
    "row_type",
]


CAPABILITY_NAME = "extract_c3_po_numbers"
CAPABILITY_APPOINTMENT = "extract_c3_appointment_details"
CAPABILITY_START_APPOINTMENT = "start_extract_c3_appointment"
CAPABILITY_CHECK_JOB = "check_job_status"
CAPABILITY_RUN_BACKGROUND_JOB = "_run_background_extract_job"
MONDAY_API_URL = "https://api.monday.com/v2"

# =============================================================================
# Job Management (file-based, same pattern as monday-toolkit)
# =============================================================================


class JobManager:
    """Manages background extract job state using file-based storage (persists across process restarts)."""

    STATUS_PENDING = "pending"
    STATUS_RUNNING = "running"
    STATUS_COMPLETED = "completed"
    STATUS_FAILED = "failed"

    def __init__(self, jobs_dir: Optional[str] = None) -> None:
        if jobs_dir is None:
            self.jobs_dir = Path(
                os.environ.get("TEMP", os.environ.get("TMP", "/tmp"))
            ) / ".monday_c3_po_extractor_jobs"
        else:
            self.jobs_dir = Path(jobs_dir)
        self.jobs_dir.mkdir(parents=True, exist_ok=True)

    def _get_job_file(self, job_id: str) -> Path:
        return self.jobs_dir / f"{job_id}.json"

    def create_job(self, job_id: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new job entry."""
        job_data = {
            "job_id": job_id,
            "status": self.STATUS_PENDING,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "args": args,
            "progress": {"items_total": 0, "items_processed": 0},
            "result": None,
            "error": None,
        }
        self._save_job(job_id, job_data)
        return job_data

    def update_job_status(
        self,
        job_id: str,
        status: str,
        progress: Optional[Dict[str, Any]] = None,
        result: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        """Update job status and optionally progress, result, or error."""
        job_data = self.get_job(job_id)
        if job_data:
            job_data["status"] = status
            job_data["updated_at"] = datetime.now(timezone.utc).isoformat()
            if progress is not None:
                job_data["progress"].update(progress)
            if result is not None:
                job_data["result"] = result
            if error is not None:
                job_data["error"] = error
            self._save_job(job_id, job_data)

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job data by ID."""
        job_file = self._get_job_file(job_id)
        if job_file.exists():
            try:
                with open(job_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return None
        return None

    def _save_job(self, job_id: str, job_data: Dict[str, Any]) -> None:
        """Save job data to file."""
        job_file = self._get_job_file(job_id)
        with open(job_file, "w", encoding="utf-8") as f:
            json.dump(job_data, f, indent=2)


_job_manager = JobManager()

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
COL_ALTRUOS_SHIPMENT_ID = "Altruos Shipment ID"
COL_ALTRUOS_LOAD_ID = "Altruos Load ID"

# C3 Response: raw label from email -> Monday column label (status)
# Standard subject lines (dates/times/numbers vary); match is case-insensitive and allows suffixes like "(POs Added/Removed)", "[Scheduled Date/Time...]", " notification".
# Sobeys: Reservation Approval->Approved, Update->Update, No Show Appointment->No show, Missing/Incorrect paperwork->Missing/Incorrect Paperwork, Signed paperwork->Signed Paperwork
# Loblaw: Appointment Confirmation Approved->Approved, Appointment Cancellation Request Approved->Cancelled, Appointment Rejection->Rejected, No Show Notification->No Show,
#         Amendment Accepted (any variant)->Amendment Accepted
C3_RESPONSE_MAP = {
    # Sobeys
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
    """Map raw C3 response from email to Monday C3 Response column label.
    Match is case-insensitive; raw may have prefixes/suffixes (e.g. 'Amendment Accepted (POs Added/Removed)').
    Uses longest matching key so 'Appointment Cancellation Request Approved' wins over 'Appointment Confirmation'."""
    if not raw or not isinstance(raw, str):
        return None
    normalized = raw.strip().lower()
    if not normalized:
        return None
    # Exact match first
    if normalized in C3_RESPONSE_MAP:
        return C3_RESPONSE_MAP[normalized]
    # Else find keys that are contained in the raw string; use longest match
    best_key: Optional[str] = None
    for key in sorted(C3_RESPONSE_MAP.keys(), key=len, reverse=True):
        if key in normalized:
            best_key = key
            break
    return C3_RESPONSE_MAP[best_key] if best_key else None


def _format_appointment_date_time_for_csv(value: Optional[str]) -> Tuple[str, str]:
    """
    Parse '15/01/2026 13:00 EST' or '2026/01/15 13:00 EST' and return (date_yyyymmdd, time_hhmmss).
    Date format: YYYYMMDD e.g. 20260115. Time format: HHMMSS e.g. 130000.
    Returns ('', '') if unparseable.
    """
    if not value or not isinstance(value, str):
        return ("", "")
    value = value.strip()
    # Match date and time: DD/MM/YYYY or YYYY/MM/DD, then optional HH:MM or HH:MM:SS
    m = re.match(r"(\d{4})/(\d{2})/(\d{2})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?", value)
    if m:
        y, mo, d, h, mi, sec = m.groups()
        date_str = f"{y}{mo}{d}"
        time_str = f"{int(h):02d}{mi}{(sec or '00')}"
        return (date_str, time_str)
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?", value)
    if m:
        d, mo, y, h, mi, sec = m.groups()
        date_str = f"{y}{mo}{d}"
        time_str = f"{int(h):02d}{mi}{(sec or '00')}"
        return (date_str, time_str)
    return ("", "")


def _get_downloads_path() -> Path:
    """Return the user's Downloads folder (Windows or Mac/Linux)."""
    if os.name == "nt":
        # Windows: USERPROFILE or fallback to HOME
        base = os.environ.get("USERPROFILE") or os.environ.get("HOME") or os.path.expanduser("~")
    else:
        base = os.environ.get("HOME") or os.path.expanduser("~")
    return Path(base) / "Downloads"


def _items_to_csv_rows(items: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """
    Expand items to one row per PO: same item fields duplicated, with a single 'po' per row.
    Date as YYYYMMDD, time as HHMMSS in separate columns. Includes shipment_id and load_id
    when po_details is present. If an item has no POs, output one row with po empty.
    """
    rows: List[Dict[str, str]] = []
    for it in items:
        raw_datetime = str(it.get("appointment_date_time") or "")
        appointment_date, delivery_time = _format_appointment_date_time_for_csv(raw_datetime)
        base = {
            "item_id": str(it.get("item_id") or ""),
            "appointment_number": str(it.get("appointment_number") or ""),
            "client": str(it.get("client") or ""),
            "consignee": str(it.get("consignee") or ""),
            "appointment_date": appointment_date,
            "delivery_time": delivery_time,
            "c3_response": str(it.get("c3_response") or ""),
            "row_type": str(it.get("row_type") or ""),
        }
        po_details = it.get("po_details") or []
        if po_details:
            for detail in po_details:
                rows.append({
                    **base,
                    "po": str(detail.get("po") or "").strip(),
                    "shipment_id": str(detail.get("shipment_id") or "").strip(),
                    "load_id": str(detail.get("load_id") or "").strip(),
                })
        else:
            pos = it.get("po_numbers") or []
            if not pos:
                rows.append({**base, "po": "", "shipment_id": "", "load_id": ""})
            else:
                for po in pos:
                    rows.append({**base, "po": str(po).strip(), "shipment_id": "", "load_id": ""})
    return rows


def _save_items_as_csv_to_downloads(
    items: List[Dict[str, Any]],
    filename: str = NEW_RECORDS_FILENAME,
) -> str:
    """
    Save items as CSV in the user's Downloads folder: one row per PO, headers as column names.
    Returns the absolute path of the saved file.
    """
    folder = _get_downloads_path()
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / filename
    rows = _items_to_csv_rows(items)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(rows)
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


def _load_altruos_config(config_path: Optional[str]) -> Optional[Dict[str, Any]]:
    """Load Altruos API config from JSON file. Returns None if config_path is empty."""
    if not config_path or not str(config_path).strip():
        return None
    path = Path(config_path).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        config = json.load(f)
    base_url = (config.get("baseUrl") or "").strip().rstrip("/")
    if not base_url:
        raise ValueError("Config must contain 'baseUrl'")
    return {**config, "baseUrl": base_url + "/"}


def _altruos_auth_and_headers(config: Dict[str, Any]) -> Tuple[Dict[str, str], Optional[Any]]:
    """Build headers and optional AWS4Auth for Altruos API (same as test_altruos_search.py)."""
    headers = {
        "Content-Type": "application/json",
        "x-api-key": str(config.get("apiKey") or ""),
    }
    auth = None
    if _HAS_AWS4AUTH:
        access_key = (config.get("accessKey") or "").strip()
        secret_key = (config.get("secretKey") or "").strip()
        if access_key and secret_key:
            region = (config.get("region") or "ca-central-1").strip()
            service = (config.get("service") or "execute-api").strip()
            auth = AWS4Auth(access_key, secret_key, region, service)
    return headers, auth


def _altruos_post(
    config: Dict[str, Any],
    path: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """POST to Altruos API; path is relative to baseUrl (e.g. 'shipments/search')."""
    url = (config.get("baseUrl") or "").rstrip("/") + "/" + path.lstrip("/")
    headers, auth = _altruos_auth_and_headers(config)
    response = requests.post(url, headers=headers, json=payload, auth=auth, timeout=30)
    response.raise_for_status()
    return response.json()


def _altruos_post_with_debug(
    config: Dict[str, Any],
    path: str,
    payload: Dict[str, Any],
    debug_list: List[Dict[str, Any]],
) -> Optional[Any]:
    """
    POST to Altruos API and append request/response to debug_list.
    Returns response data (dict or list) on success, None on failure. Debug entry includes
    endpoint, url, request_json, response_json, response_status_code, and error (if any).
    """
    url = (config.get("baseUrl") or "").rstrip("/") + "/" + path.lstrip("/")
    request_json = json.loads(json.dumps(payload))
    entry: Dict[str, Any] = {
        "endpoint": path,
        "url": url,
        "request_json": request_json,
        "response_json": None,
        "response_status_code": None,
        "error": None,
    }
    try:
        headers, auth = _altruos_auth_and_headers(config)
        response = requests.post(
            url, headers=headers, json=payload, auth=auth, timeout=30
        )
        raw_text = response.text
        entry["response_status_code"] = response.status_code
        try:
            response_data = response.json()
        except Exception:
            response_data = raw_text
        entry["response_json"] = response_data
        response.raise_for_status()
        debug_list.append(entry)
        return response_data
    except Exception as e:
        entry["error"] = str(e)
        debug_list.append(entry)
        return None


def _altruos_shipment_id_by_po(
    config: Dict[str, Any],
    po: str,
    debug_list: Optional[List[Dict[str, Any]]] = None,
) -> Optional[str]:
    """Look up Altruos Shipment ID by PO number. Returns first shipment id or None.
    If debug_list is provided, appends one entry with request/response for this call."""
    if not (po or str(po).strip()):
        return None
    po_str = str(po).strip()
    payload = {"purchase_order": po_str}
    if debug_list is not None:
        data = _altruos_post_with_debug(config, "shipments/search", payload, debug_list)
        if data is None:
            return None
    else:
        try:
            data = _altruos_post(config, "shipments/search", payload)
        except Exception:
            return None
    # Use only "shipment_id" label from response; if not found leave blank
    if isinstance(data, list) and len(data) > 0:
        first = data[0]
        sid = first.get("shipment_id")
        return str(sid) if sid is not None else None
    if isinstance(data, dict):
        sid = data.get("shipment_id")
        if sid is not None:
            return str(sid)
        items = data.get("shipments") or data.get("data") or []
        if items and isinstance(items, list):
            first = items[0]
            sid = first.get("shipment_id")
            return str(sid) if sid is not None else None
    return None


def _altruos_load_id_by_shipment_id(
    config: Dict[str, Any],
    shipment_id: str,
    debug_list: Optional[List[Dict[str, Any]]] = None,
) -> Optional[str]:
    """Look up Altruos Load ID by Shipment ID. Returns first load id or None.
    If debug_list is provided, appends one entry with request/response for this call."""
    if not (shipment_id or str(shipment_id).strip()):
        return None
    try:
        sid_int = int(shipment_id)
    except (TypeError, ValueError):
        return None
    payload = {"movement": {"shipment_ids": [sid_int]}}
    if debug_list is not None:
        data = _altruos_post_with_debug(
            config, "movements/search/shipments", payload, debug_list
        )
        if data is None:
            return None
    else:
        try:
            data = _altruos_post(
                config,
                "movements/search/shipments",
                payload,
            )
        except Exception:
            return None
    # Use only "load_id" label from response (result_list[].movement.header.load_id); if not found leave blank
    if isinstance(data, list) and len(data) > 0:
        first = data[0]
        lid = _extract_load_id_from_movement_item(first)
        return str(lid) if lid is not None else None
    if isinstance(data, dict):
        result_list = data.get("result_list") or []
        if result_list and isinstance(result_list, list):
            first = result_list[0]
            lid = _extract_load_id_from_movement_item(first)
            return str(lid) if lid is not None else None
    return None


def _extract_load_id_from_movement_item(item: Dict[str, Any]) -> Optional[str]:
    """Extract only 'load_id' from result_list[].movement.header; if not present return None."""
    if not isinstance(item, dict):
        return None
    movement = item.get("movement")
    if not isinstance(movement, dict):
        return None
    header = movement.get("header")
    if not isinstance(header, dict):
        return None
    load_id = header.get("load_id")
    return str(load_id) if load_id is not None else None


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


# ---- Sobeys subject ----
# Full: "Sobeys - C3 Response for NUMBER on DATE for Consignee" or "Sobeys - C3 Response: NUMBER on DATE for Consignee"
# Notification-only: "Sobeys - No Show Appointment notification" / "Sobeys - Missing/Incorrect paperwork notification" / "Sobeys - Signed paperwork notification"
SOBEYS_SUBJECT_RE = re.compile(
    r"^(.+?)\s+-\s+(.+?)\s*(?:for\s+|:\s*)(\d+)\s+on\s+(.+?)\s+for\s+(.+)$",
    re.IGNORECASE | re.DOTALL,
)
SOBEYS_SUBJECT_NOTIFICATION_RE = re.compile(
    r"^Sobeys\s+-\s+(.+)$",
    re.IGNORECASE,
)


def _parse_sobeys_subject(subject: str) -> Dict[str, Optional[str]]:
    """
    Parse Sobeys email subject (full format with appt/date/consignee, or notification-only).
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
    if m:
        client, c3_response, appt_no, date_time, consignee = m.groups()
        out["client"] = (client or "").strip()
        out["c3_response"] = (c3_response or "").strip()
        out["appointment_number"] = (appt_no or "").strip()
        out["appointment_date_time"] = (date_time or "").strip()
        out["consignee"] = (consignee or "").strip()
        return out
    # Fallback: notification-only subject (e.g. "Sobeys - No Show Appointment notification")
    m2 = SOBEYS_SUBJECT_NOTIFICATION_RE.match(subject)
    if m2:
        out["client"] = "Sobeys"
        out["c3_response"] = (m2.group(1) or "").strip()
    return out


# ---- Loblaw subject ----
# Standard: "C3 Response - from Loblaw Appointing: ..." or "C3 Response from Loblaw Appointing: ..." (with/without hyphen, optional brackets/suffix)
# Examples: "Appointment Confirmation Approved - from Loblaw Appointing: ...", "Appointment Rejection from Loblaw Appointing - ref# ...", "No Show Notification from Loblaw Appointing: ..."
LOBLAW_C3_CLIENT_RE = re.compile(
    r"^(.+?)(?:\s*-\s*from|\s+from)\s+(.+?)(?:\s+Appointing|\s*:|\s*$)",
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


def _extract_loblaw_reference_number_from_body(body: str) -> Optional[str]:
    """Extract Reference # from Loblaw email body (Updates section). Uses _strip_html like _parse_loblaw_body."""
    if not body or not isinstance(body, str):
        return None
    text = _strip_html(body).replace("\r\n", "\n")
    ref_m = re.search(r"Reference\s*#\s*:\s*(\d+)", text, re.IGNORECASE)
    return ref_m.group(1).strip() if ref_m else None


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
    _set(COL_ALTRUOS_SHIPMENT_ID, extracted.get("shipment_ids") or [])
    _set(COL_ALTRUOS_LOAD_ID, extracted.get("load_ids") or [])

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
    """
    Fetch all items where name contains 'Loblaw' and created within the last N weeks
    (Update section). Paginates through every page so we can compare Reference # across
    all such rows for New vs Update. Fallback: all Loblaw by name (no date filter).
    """
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
        # Same rules as user query: name contains Loblaw + pulse_log within last 2 weeks (CREATED_AT).
        # order_by __creation_log__ so items come oldest first (oldest row = New, newer duplicates = Update).
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
            "order_by": [{"column_id": "__creation_log__"}],
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
        # Paginate through all pages: keep requesting while cursor is present
        while cursor:
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


def _get_loblaw_oldest_item_id_per_reference(
    api_token: str,
    board_id: int,
    weeks: int = 2,
) -> Dict[str, str]:
    """
    For all Loblaw rows in the past N weeks (ordered oldest first), extract Reference #
    from each item's updates. Returns ref_number -> item_id of the OLDEST item with that ref.
    That item gets Row Type "New"; all other items with the same ref get "Update".
    """
    loblaw_items = _fetch_loblaw_items_past_weeks(api_token, board_id, weeks)
    if not loblaw_items:
        return {}
    item_ids = [str(i["id"]) for i in loblaw_items]
    updates_by_item: Dict[str, List[Dict[str, Any]]] = {}
    for i in range(0, len(item_ids), 50):
        updates_by_item.update(_fetch_updates_for_items(api_token, item_ids[i : i + 50]))
    # Items are in creation order (oldest first). First item_id we see for each ref # is the oldest.
    # Use _strip_html so we match "Reference #:" in HTML email bodies (same as _parse_loblaw_body).
    ref_to_oldest_item_id: Dict[str, str] = {}
    for iid in item_ids:
        for upd in updates_by_item.get(iid) or []:
            body = (upd.get("body") or "").strip()
            if not body:
                continue
            text = _strip_html(body).replace("\r\n", "\n")
            ref_m = re.search(r"Reference\s*#\s*:\s*(\d+)", text, re.IGNORECASE)
            if ref_m:
                ref = ref_m.group(1).strip()
                if ref not in ref_to_oldest_item_id:
                    ref_to_oldest_item_id[ref] = iid
                break  # one ref per item (use first update that has it)
    return ref_to_oldest_item_id


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
    config_path: Optional[str] = None,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    """
    Fetches items from the C3 board where Row Type is empty (first N rows in dev),
    and extracts appointment_number, client, consignee, appointment_date_time, c3_response,
    po_numbers. When config_path is provided, also looks up Altruos Shipment ID and Load ID
    via API (PO -> shipments/search, Shipment ID -> movements/search/shipments).
    Supports Sobeys and Loblaw email formats.
    If progress_callback is provided, it is called with {"items_total": n, "items_processed": i}.
    """
    try:
        token = _get_api_token(api_token)
        bid = board_id if board_id is not None else DEFAULT_BOARD_ID
        group_filter = (group_name or DEFAULT_GROUP_NAME).strip()
        max_items = max(1, min(limit, 50))
        altruos_config = _load_altruos_config(config_path)

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

        items_total = len(items_to_process)
        if progress_callback:
            progress_callback({"items_total": items_total, "items_processed": 0})

        item_ids = [str(i["id"]) for i in items_to_process]
        updates_by_item = {}
        for i in range(0, len(item_ids), 50):
            batch = item_ids[i : i + 50]
            updates_by_item.update(_fetch_updates_for_items(token, batch))

        # Precompute for Loblaw: ref # -> oldest item_id (past 2 weeks, creation order). That item = New, others = Update.
        loblaw_oldest_item_id_per_ref = _get_loblaw_oldest_item_id_per_reference(token, bid, weeks=2)

        # First-appended row (bottom of board) gets "New"; others get "Update".
        # Process bottom-to-top: items_to_process[0]=top (newest), [-1]=bottom (oldest). So reversed = bottom first.
        ref_to_oldest_in_batch: Dict[str, str] = {}
        for item in reversed(items_to_process):
            name = (item.get("name") or "").strip()
            if "Loblaw" not in name:
                continue
            iid = str(item.get("id", ""))
            for upd in updates_by_item.get(iid) or []:
                body = (upd.get("body") or "").strip()
                ref = _extract_loblaw_reference_number_from_body(body) if body else None
                if ref and ref not in ref_to_oldest_in_batch:
                    ref_to_oldest_in_batch[ref] = iid
                if ref:
                    break
        # Prefer batch order: for refs in this batch, "New" = first in bottom-to-top order (bottom row)
        for ref, iid in ref_to_oldest_in_batch.items():
            loblaw_oldest_item_id_per_ref[ref] = iid

        results: List[Dict[str, Any]] = []
        for item in reversed(items_to_process):
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

            # Determine Row Type: New (oldest occurrence) or Update (newer duplicates)
            row_type_value = ROW_TYPE_NEW
            appt = (appointment_number or "").strip()
            if "Sobeys" in name or (name and name.strip().lower().startswith("sobeys")):
                count = _count_items_with_name_containing(token, bid, appt) if appt else 0
                row_type_value = ROW_TYPE_NEW if count <= 1 else ROW_TYPE_UPDATE
            else:
                # Loblaw: only the OLDEST item (by creation) with this Reference # gets New; rest get Update
                oldest_item_id = loblaw_oldest_item_id_per_ref.get(appt) if appt else None
                if not appt:
                    row_type_value = ROW_TYPE_NEW
                elif oldest_item_id is None:
                    row_type_value = ROW_TYPE_NEW  # ref # not seen in past 2 weeks → single occurrence
                else:
                    row_type_value = ROW_TYPE_NEW if oldest_item_id == iid else ROW_TYPE_UPDATE

            shipment_ids: List[str] = []
            load_ids: List[str] = []
            po_details: List[Dict[str, str]] = []
            altruos_api_debug: List[Dict[str, Any]] = []

            if altruos_config and po_numbers:
                for po in po_numbers:
                    po_str = str(po).strip()
                    shipment_id = _altruos_shipment_id_by_po(
                        altruos_config, po_str, debug_list=altruos_api_debug
                    )
                    load_id: Optional[str] = None
                    if shipment_id:
                        shipment_ids.append(shipment_id)
                        load_id = _altruos_load_id_by_shipment_id(
                            altruos_config, shipment_id, debug_list=altruos_api_debug
                        )
                        if load_id:
                            load_ids.append(load_id)
                    po_details.append({
                        "po": po_str,
                        "shipment_id": shipment_id or "",
                        "load_id": load_id or "",
                    })

            extracted_row = {
                "appointment_number": appointment_number,
                "client": client,
                "consignee": consignee,
                "appointment_date_time": appointment_date_time,
                "c3_response": c3_response,
                "po_numbers": po_numbers,
                "shipment_ids": shipment_ids,
                "load_ids": load_ids,
            }

            # Populate all columns on Monday: Appointment #, Client, Consignee, Appointment Date,
            # Requested Delivery Appointment, PO, Altruos Shipment ID, Altruos Load ID, Row Type
            try:
                _update_item_with_extracted_columns(
                    token, bid, int(iid), column_by_title, extracted_row, row_type_value
                )
            except Exception as update_err:
                row_type_value = f"{row_type_value} (update failed: {update_err})"

            result_item: Dict[str, Any] = {
                "item_id": iid,
                "appointment_number": appointment_number,
                "client": client,
                "consignee": consignee,
                "appointment_date_time": appointment_date_time,
                "c3_response": c3_response,
                "po_numbers": po_numbers,
                "row_type": row_type_value,
            }
            if po_details:
                result_item["po_details"] = po_details
            if altruos_api_debug:
                result_item["altruos_api_debug"] = altruos_api_debug
            results.append(result_item)
            if progress_callback:
                progress_callback({"items_total": items_total, "items_processed": len(results)})

        # Save items to Downloads/new_records.csv (one row per PO, headers as column names)
        saved_path = ""
        if results:
            try:
                saved_path = _save_items_as_csv_to_downloads(results, NEW_RECORDS_FILENAME)
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


def _run_background_extract_job(args: Dict[str, Any]) -> None:
    """
    Internal capability: runs in a background subprocess. Updates job status via JobManager
    (file-based) as it progresses. Called with capability _run_background_extract_job.
    """
    job_id = args.get("job_id")
    original_args = args.get("original_args", {})
    if not job_id:
        return
    try:
        _job_manager.update_job_status(job_id, JobManager.STATUS_RUNNING)

        def progress_cb(progress: Dict[str, Any]) -> None:
            _job_manager.update_job_status(job_id, JobManager.STATUS_RUNNING, progress=progress)

        result = extract_c3_appointment_details(
            api_token=original_args.get("api_token"),
            board_id=original_args.get("board_id"),
            group_name=original_args.get("group_name"),
            limit=int(original_args.get("limit") or DEV_LIMIT_ROWS),
            config_path=original_args.get("config_path"),
            progress_callback=progress_cb,
        )
        if "error" in result:
            _job_manager.update_job_status(
                job_id, JobManager.STATUS_FAILED, error=result["error"]
            )
        else:
            _job_manager.update_job_status(
                job_id,
                JobManager.STATUS_COMPLETED,
                result=result.get("result"),
                progress={
                    "items_total": result.get("result", {}).get("count", 0),
                    "items_processed": result.get("result", {}).get("count", 0),
                },
            )
    except Exception as e:
        _job_manager.update_job_status(
            job_id, JobManager.STATUS_FAILED, error=str(e)
        )


def start_extract_c3_appointment(args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Starts C3 appointment extraction in a background subprocess and returns immediately with job_id.
    Use check_job_status with job_id to poll for completion (e.g. every 10–30 seconds).
    """
    try:
        if not args.get("api_token"):
            return {
                "error": "Missing required parameter: api_token",
                "capability": CAPABILITY_START_APPOINTMENT,
            }
        job_id = str(uuid.uuid4())[:8]
        _job_manager.create_job(job_id, args)

        script_path = os.path.abspath(__file__)
        background_input = json.dumps({
            "capability": CAPABILITY_RUN_BACKGROUND_JOB,
            "args": {"job_id": job_id, "original_args": args},
        })

        if sys.platform == "win32":
            DETACHED_PROCESS = 0x00000008
            CREATE_NEW_PROCESS_GROUP = 0x00000200
            CREATE_NO_WINDOW = 0x08000000
            process = subprocess.Popen(
                [sys.executable, script_path],
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW,
                close_fds=True,
            )
            process.stdin.write(background_input.encode())
            process.stdin.close()
        else:
            process = subprocess.Popen(
                [sys.executable, script_path],
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
                close_fds=True,
            )
            process.stdin.write(background_input.encode())
            process.stdin.close()

        return {
            "result": {
                "job_id": job_id,
                "status": "started",
                "message": "Extract job started in background. Use check_job_status to monitor progress.",
            },
            "capability": CAPABILITY_START_APPOINTMENT,
        }
    except Exception as e:
        return {
            "error": f"Failed to start job: {str(e)}",
            "capability": CAPABILITY_START_APPOINTMENT,
        }


def check_job_status(args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Check the status of a background extract job. Returns progress and, when completed, final_result.
    Poll every 10–30 seconds until status is "completed" or "failed".
    """
    try:
        job_id = args.get("job_id")
        if not job_id:
            return {
                "error": "Missing required parameter: job_id",
                "capability": CAPABILITY_CHECK_JOB,
            }
        job_id = str(job_id).strip()
        job_data = _job_manager.get_job(job_id)
        if job_data is None:
            return {
                "error": f"Job not found: {job_id}",
                "capability": CAPABILITY_CHECK_JOB,
            }
        response = {
            "result": {
                "job_id": job_id,
                "status": job_data["status"],
                "created_at": job_data["created_at"],
                "updated_at": job_data["updated_at"],
                "progress": job_data.get("progress") or {"items_total": 0, "items_processed": 0},
            },
            "capability": CAPABILITY_CHECK_JOB,
        }
        if job_data["status"] == JobManager.STATUS_COMPLETED and job_data.get("result"):
            response["result"]["final_result"] = job_data["result"]
        if job_data["status"] == JobManager.STATUS_FAILED and job_data.get("error"):
            response["result"]["error"] = job_data["error"]
        return response
    except Exception as e:
        return {
            "error": f"Error checking job status: {str(e)}",
            "capability": CAPABILITY_CHECK_JOB,
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
                config_path=args.get("config_path"),
            )
            print(json.dumps(result, indent=2))
        elif capability == CAPABILITY_START_APPOINTMENT:
            result = start_extract_c3_appointment(args)
            print(json.dumps(result, indent=2))
        elif capability == CAPABILITY_CHECK_JOB:
            result = check_job_status(args)
            print(json.dumps(result, indent=2))
        elif capability == CAPABILITY_RUN_BACKGROUND_JOB:
            _run_background_extract_job(args)
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
