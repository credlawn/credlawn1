import frappe
import requests
from frappe import _
from frappe.utils import getdate, date_diff
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOCK_KEY = "credlawn:ipa_sync:lock"
PB_PER_PAGE = 500
BATCH_SIZE = 500

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


def _get_session():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))
    return session


def _fetch_all_items(session, api_url, headers, pb_filter):
    first_resp = session.get(
        api_url, headers=headers,
        params={"filter": pb_filter, "perPage": 1}, timeout=30,
    )
    first_resp.raise_for_status()
    total = first_resp.json().get("totalItems", 0)
    if not total:
        return []

    all_items = []
    page = 1
    while True:
        resp = session.get(
            api_url, headers=headers,
            params={"filter": pb_filter, "page": page, "perPage": PB_PER_PAGE, "sort": "updated"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])
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

    arn_date_val, arn_month_val, parsed = parse_arn_full(arn_no)

    old_arn = enriched.get("old_arn_no")
    old_decision_date = enriched.get("old_decision_date")

    if not old_decision_date and old_arn:
        parsed_old_date, _, _ = parse_arn_full(old_arn)
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
        "gap": gap_days,
    }


INSERT_FIELDS = [
    "pb_id", "pb_created", "pb_updated",
    "customer_name", "mobile_no", "employee_name", "employee_code",
    "ip_status", "arn_no", "login_date", "date_of_birth",
    "arn_date", "arn_month", "unique",
    "data_code", "custom_code", "old_arn_no", "old_decision_date", "gap",
    "update_error",
]

UPDATE_SQL = """UPDATE `tabIPA Records` SET
    pb_created = %s, pb_updated = %s,
    customer_name = %s, mobile_no = %s, employee_name = %s, employee_code = %s,
    ip_status = %s, arn_no = %s, login_date = %s, date_of_birth = %s,
    arn_date = %s, arn_month = %s, `unique` = %s,
    data_code = %s, custom_code = %s, old_arn_no = %s, old_decision_date = %s,
    gap = %s
WHERE name = %s"""


def _bulk_insert_records(records):
    num_fields = len(INSERT_FIELDS)
    cols = ", ".join(f"`{f}`" for f in INSERT_FIELDS)

    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        flat_values = []
        for r in batch:
            flat_values.extend((
                r["pb_id"], r["pb_created"], r["pb_updated"],
                r["customer_name"], r["mobile_no"], r["employee_name"], r["employee_code"],
                r["ip_status"], r["arn_no"], r["login_date"], r["date_of_birth"],
                r["arn_date"], r["arn_month"], r["unique"],
                r["data_code"], r["custom_code"], r["old_arn_no"], r["old_decision_date"], r["gap"],
                0,
            ))

        row_placeholders = ", ".join(
            ["(" + ", ".join(["%s"] * num_fields) + ")"] * len(batch)
        )
        sql = f"INSERT INTO `tabIPA Records` ({cols}) VALUES {row_placeholders}"

        try:
            frappe.db.sql(sql, flat_values)
        except Exception:
            fallback_sql = f"INSERT INTO `tabIPA Records` ({cols}) VALUES ({', '.join(['%s'] * num_fields)})"
            for r in batch:
                try:
                    frappe.db.sql(fallback_sql, (
                        r["pb_id"], r["pb_created"], r["pb_updated"],
                        r["customer_name"], r["mobile_no"], r["employee_name"], r["employee_code"],
                        r["ip_status"], r["arn_no"], r["login_date"], r["date_of_birth"],
                        r["arn_date"], r["arn_month"], r["unique"],
                        r["data_code"], r["custom_code"], r["old_arn_no"], r["old_decision_date"], r["gap"],
                        0,
                    ))
                except Exception:
                    frappe.log_error(frappe.get_traceback(), f"IPA Sync: Insert Error pb_id={r['pb_id']}")


def _bulk_update_records(records):
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        for doc_name, r in batch:
            try:
                frappe.db.sql(UPDATE_SQL, (
                    r["pb_created"], r["pb_updated"],
                    r["customer_name"], r["mobile_no"], r["employee_name"], r["employee_code"],
                    r["ip_status"], r["arn_no"], r["login_date"], r["date_of_birth"],
                    r["arn_date"], r["arn_month"], r["unique"],
                    r["data_code"], r["custom_code"], r["old_arn_no"], r["old_decision_date"],
                    r["gap"],
                    doc_name,
                ))
            except Exception:
                frappe.log_error(frappe.get_traceback(), f"IPA Sync: Update Error {doc_name}")


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

        session = _get_session()

        publish_progress(10, "Fetching records from PocketBase...")
        all_items = _fetch_all_items(session, api_url, headers, pb_filter)

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

        to_insert = []
        to_update = []
        malformed_arns = []

        for idx, item in enumerate(all_items):
            try:
                pb_id, doc_data = _process_item(item, enrichment_map)
                arn_no = item.get("arn_no", "")
                if arn_no and not parse_arn_full(arn_no)[2]:
                    malformed_arns.append(arn_no)
                if pb_id in existing_map:
                    to_update.append((existing_map[pb_id], doc_data))
                else:
                    to_insert.append(doc_data)
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

        if to_insert:
            publish_progress(55, f"Inserting {len(to_insert)} records...")
            _bulk_insert_records(to_insert)
        if to_update:
            publish_progress(70, f"Updating {len(to_update)} records...")
            _bulk_update_records(to_update)

        if malformed_arns:
            summary = f"Sync found {len(malformed_arns)} malformed ARNs:\n"
            summary += ", ".join(list(set(malformed_arns[:50])))
            if len(set(malformed_arns)) > 50:
                summary += f"\n... and {len(set(malformed_arns)) - 50} more"
            frappe.log_error(summary, "IPA Sync: Malformed ARNs List")

        publish_progress(
            100,
            f"Sync Finished: {len(to_insert)} created, {len(to_update)} updated.",
        )

    except Exception:
        frappe.log_error(frappe.get_traceback(), "IPA Smart Sync Failure")
        publish_progress(0, "Sync Error", failed=True)
    finally:
        frappe.cache().delete_value(LOCK_KEY)


def parse_arn_full(arn_no):
    if not arn_no or len(arn_no) < 6:
        return None, None, False

    month_map = {
        "A": ("01", "Jan"), "B": ("02", "Feb"), "C": ("03", "Mar"),
        "D": ("04", "Apr"), "E": ("05", "May"), "F": ("06", "Jun"),
        "G": ("07", "Jul"), "H": ("08", "Aug"), "I": ("09", "Sep"),
        "J": ("10", "Oct"), "K": ("11", "Nov"), "L": ("12", "Dec"),
    }

    try:
        yy = arn_no[1:3]
        m_char = arn_no[3].upper()
        dd_str = arn_no[4:6]
        m_data = month_map.get(m_char)

        if m_data and yy.isdigit() and dd_str.isdigit():
            mm_num, mmm_name = m_data
            day_int = int(dd_str)
            if 1 <= day_int <= 31:
                return f"20{yy}-{mm_num}-{dd_str}", f"{mmm_name}-{yy}", True
    except Exception:
        pass
    return None, None, False


def publish_progress(percentage, message, failed=False):
    frappe.publish_realtime(
        "ipa_sync_progress",
        {"percentage": percentage, "message": message, "failed": failed},
    )
