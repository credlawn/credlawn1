import frappe
import requests
from frappe import _
from frappe.utils import getdate, date_diff, now_datetime

LOCK_KEY = "credlawn:ipa_sync:lock"
PB_PER_PAGE = 500
BATCH_SIZE = 500

SYNC_FIELDS = [
    "name", "creation", "modified", "modified_by", "owner", "docstatus",
    "pb_id", "pb_created", "pb_updated",
    "customer_name", "mobile_no", "employee_name", "employee_code",
    "ip_status", "arn_no", "login_date", "date_of_birth",
    "arn_date", "arn_month", "unique",
    "data_code", "custom_code", "old_arn_no", "old_decision_date", "gap",
]

UPDATE_EXCLUDE = {"name", "creation", "owner", "pb_id", "pb_created"}
UPDATE_PARTS = ", ".join(
    f"`{f}`=VALUES(`{f}`)" for f in SYNC_FIELDS if f not in UPDATE_EXCLUDE
)

@frappe.whitelist()
def pull_ipa_data():
    if frappe.cache().get_value(LOCK_KEY):
        frappe.msgprint(_("IPA Sync is already running."))
        return
    frappe.cache().set_value(LOCK_KEY, 1, expires_in_sec=3600)
    frappe.enqueue(
        "credlawn.credlawn.doctype.ipa_records.pull_ipa_code.run_smart_sync",
        queue="long",
        timeout=3600,
    )
    return _("Smart IPA Sync initiated. Monitoring progress...")


def _fetch_pb_records(api_url, headers, pb_filter, page, per_page, return_raw=False):
    try:
        resp = requests.get(
            api_url, headers=headers,
            params={"filter": pb_filter, "page": page, "perPage": per_page, "sort": "updated"},
            timeout=30,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data if return_raw else data.get("items", [])
        return {} if return_raw else []
    except Exception:
        return {} if return_raw else []


def _fetch_all_items(api_url, headers, pb_filter):
    initial = _fetch_pb_records(api_url, headers, pb_filter, 1, 1, return_raw=True)
    total = initial.get("totalItems", 0) if initial else 0
    if not total:
        return []

    all_items = []
    page = 1
    while True:
        items = _fetch_pb_records(api_url, headers, pb_filter, page, PB_PER_PAGE)
        if not items:
            break
        all_items.extend(items)
        if page * PB_PER_PAGE >= total:
            break
        page += 1

    return all_items


def _clean_dt(val):
    if not val:
        return None
    return str(val).split("T")[0].split(" ")[0]


def _process_item(item, enrichment_map):
    pb_id = item.get("id")
    mobile = item.get("mobile_number")
    arn_no = item.get("arn_no", "")
    enriched = enrichment_map.get(mobile, {})

    arn_date_val, arn_month_val, parsed, _ = parse_arn_full(arn_no)

    old_arn = enriched.get("old_arn_no")
    old_decision_date = enriched.get("old_decision_date")

    if not old_decision_date and old_arn:
        parsed_old_date, _, _, _ = parse_arn_full(old_arn)
        old_decision_date = parsed_old_date

    gap_days = None
    try:
        if arn_date_val and old_decision_date:
            gap_days = date_diff(getdate(arn_date_val), getdate(old_decision_date))
    except Exception:
        pass

    return pb_id, {
        "pb_id": pb_id,
        "pb_created": item.get("created"),
        "pb_updated": item.get("updated"),
        "customer_name": item.get("customer_name"),
        "mobile_no": mobile,
        "employee_name": item.get("employee_name"),
        "employee_code": item.get("employee_code"),
        "ip_status": item.get("lead_status"),
        "arn_no": arn_no,
        "login_date": _clean_dt(item.get("arn_date")),
        "date_of_birth": _clean_dt(item.get("date_of_birth")),
        "arn_date": arn_date_val,
        "arn_month": arn_month_val,
        "unique": "Yes" if item.get("login_type") == "Unique" else "No",
        "data_code": enriched.get("data_code"),
        "custom_code": enriched.get("custom_code"),
        "old_arn_no": old_arn,
        "old_decision_date": old_decision_date,
        "gap": gap_days or 0,
    }


def _upsert_batch(records):
    if not records:
        return

    now_val = now_datetime()
    user = frappe.session.user
    cols = ", ".join(f"`{f}`" for f in SYNC_FIELDS)
    placeholders = "(" + ", ".join(["%s"] * len(SYNC_FIELDS)) + ")"
    all_placeholders = ", ".join([placeholders] * len(records))

    sql = f"""INSERT INTO `tabIPA Records` ({cols}) VALUES {all_placeholders}
ON DUPLICATE KEY UPDATE {UPDATE_PARTS}"""

    values = []
    for name_val, r in records:
        values.extend((
            name_val, now_val, now_val, user, user, 0,
            r["pb_id"], r["pb_created"], r["pb_updated"],
            r["customer_name"], r["mobile_no"], r["employee_name"], r["employee_code"],
            r["ip_status"], r["arn_no"], r["login_date"], r["date_of_birth"],
            r["arn_date"], r["arn_month"], r["unique"],
            r["data_code"], r["custom_code"], r["old_arn_no"], r["old_decision_date"],
            r["gap"],
        ))

    try:
        frappe.db.sql(sql, values)
    except Exception:
        for name_val, r in records:
            try:
                single_placeholders = "(" + ", ".join(["%s"] * len(SYNC_FIELDS)) + ")"
                single_sql = f"""INSERT INTO `tabIPA Records` ({cols}) VALUES {single_placeholders}
ON DUPLICATE KEY UPDATE {UPDATE_PARTS}"""
                frappe.db.sql(single_sql, (
                    name_val, now_val, now_val, user, user, 0,
                    r["pb_id"], r["pb_created"], r["pb_updated"],
                    r["customer_name"], r["mobile_no"], r["employee_name"], r["employee_code"],
                    r["ip_status"], r["arn_no"], r["login_date"], r["date_of_birth"],
                    r["arn_date"], r["arn_month"], r["unique"],
                    r["data_code"], r["custom_code"], r["old_arn_no"], r["old_decision_date"],
                    r["gap"],
                ))
            except Exception:
                frappe.log_error(frappe.get_traceback(), f"IPA Sync: Upsert Error pb_id={r['pb_id']}")


def run_smart_sync():
    try:
        last_updated = frappe.db.sql("SELECT MAX(pb_updated) FROM `tabIPA Records`")[0][0]

        pb_url = frappe.conf.get("pocketbase_url")
        pb_token = frappe.conf.get("pocketbase_auth_token")
        if not pb_url or not pb_token:
            raise Exception("Pocketbase configuration missing.")

        api_url = f"{pb_url.rstrip('/')}/api/collections/case_login/records"
        headers = {"Authorization": f"Bearer {pb_token}"}

        pb_filter = '(lead_status="IP Approved")'
        if last_updated:
            pb_filter += f" && (updated > \"{last_updated}\")"

        existing_map = {
            d.pb_id: d.name
            for d in frappe.get_all("IPA Records", fields=["name", "pb_id"])
            if d.pb_id
        }

        publish_progress(10, "Fetching records from PocketBase...")
        all_items = _fetch_all_items(api_url, headers, pb_filter)

        if not all_items:
            publish_progress(100, "Local data is already up-to-date.")
            return

        total = len(all_items)

        publish_progress(20, f"Fetched {total} records. Enriching data...")

        all_mobiles = list({i.get("mobile_number") for i in all_items if i.get("mobile_number")})
        enrichment_map = {}
        if all_mobiles:
            active_data = frappe.db.get_list(
                "Active Database",
                filters={"mobile_no": ["in", all_mobiles]},
                fields=["mobile_no", "data_code", "custom_code", "old_arn_no", "old_decision_date"],
            )
            enrichment_map = {d.mobile_no: d for d in active_data}

        to_upsert = []
        malformed_arns = []

        for idx, item in enumerate(all_items):
            try:
                pb_id, doc_data = _process_item(item, enrichment_map)
                arn_no = item.get("arn_no", "")
                if arn_no:
                    _, _, parsed, reason = parse_arn_full(arn_no)
                    if not parsed:
                        malformed_arns.append(f"{arn_no} → {reason}")
                name_val = existing_map.get(pb_id, pb_id)
                to_upsert.append((name_val, doc_data))
                if (idx + 1) % 500 == 0:
                    publish_progress(
                        30 + int((idx + 1) / total * 25),
                        f"Processed {idx + 1}/{total}...",
                    )
            except Exception:
                frappe.log_error(
                    frappe.get_traceback(),
                    f"IPA Sync: Process Error pb_id={item.get('id', 'unknown')}",
                )

        if to_upsert:
            publish_progress(55, f"Writing {len(to_upsert)} records...")
            for i in range(0, len(to_upsert), BATCH_SIZE):
                batch = to_upsert[i:i + BATCH_SIZE]
                _upsert_batch(batch)
                frappe.db.commit()
                if (i + BATCH_SIZE) % 1000 == 0:
                    publish_progress(
                        55 + int((i + BATCH_SIZE) / len(to_upsert) * 40),
                        f"Written {min(i + BATCH_SIZE, len(to_upsert))}/{len(to_upsert)}...",
                    )

        if malformed_arns:
            unique = list(set(malformed_arns))
            summary = f"Sync found {len(unique)} malformed ARN(s):\n"
            summary += "\n".join(unique[:50])
            if len(unique) > 50:
                summary += f"\n... and {len(unique) - 50} more"
            frappe.log_error(summary, "IPA Sync: Malformed ARNs List")

        publish_progress(100, f"Sync Finished: {len(to_upsert)} records processed.")

    except Exception:
        frappe.log_error(frappe.get_traceback(), "IPA Smart Sync Failure")
        publish_progress(0, "Sync Error", failed=True)
    finally:
        frappe.cache().delete_value(LOCK_KEY)


def parse_arn_full(arn_no):
    if not arn_no or len(arn_no) < 6:
        return None, None, False, "ARN too short or empty (< 6 chars)"

    month_map = {
        "A": ("01", "Jan"), "B": ("02", "Feb"), "C": ("03", "Mar"),
        "D": ("04", "Apr"), "E": ("05", "May"), "F": ("06", "Jun"),
        "G": ("07", "Jul"), "H": ("08", "Aug"), "I": ("09", "Sep"),
        "J": ("10", "Oct"), "K": ("11", "Nov"), "L": ("12", "Dec"),
    }

    yy = arn_no[1:3]
    m_char = arn_no[3].upper()
    dd_str = arn_no[4:6]

    if not yy.isdigit():
        return None, None, False, f"Year digits not numeric at positions 1-2: '{yy}'"
    if m_char not in month_map:
        return None, None, False, f"Month code '{m_char}' not in A-L at position 3"
    if not dd_str.isdigit():
        return None, None, False, f"Day digits not numeric at positions 4-5: '{dd_str}'"

    try:
        mm_num, mmm_name = month_map[m_char]
        day_int = int(dd_str)
        if day_int < 1 or day_int > 31:
            return None, None, False, f"Day {day_int} out of range (must be 1-31)"
        return f"20{yy}-{mm_num}-{dd_str}", f"{mmm_name}-{yy}", True, None
    except Exception:
        return None, None, False, f"Unexpected parse error for ARN"


def publish_progress(percentage, message, failed=False):
    frappe.publish_realtime(
        "ipa_sync_progress",
        {"percentage": percentage, "message": message, "failed": failed},
    )
