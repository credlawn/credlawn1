import frappe
from frappe import _
import requests
from datetime import datetime
import pytz
from frappe.utils import now_datetime

@frappe.whitelist()
def execute_sync():
    """Main entry for Active Leads sync - Enqueues background job"""
    try:
        frappe.enqueue(
            method='credlawn.credlawn.doctype.active_leads.sync_active_leads.sync_job',
            queue='long',
            timeout=7200,
            is_async=True,
            job_name='active_leads_sync'
        )
        return _("High-Speed Active Leads Sync started in the background.")
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Active Leads Sync Enqueue Error")
        return _("Failed to start sync: {0}").format(str(e))

def sync_job():
    try:
        pb_url = frappe.conf.get("pocketbase_url")
        pb_token = frappe.conf.get("pocketbase_auth_token")

        if not pb_url or not pb_token:
            return

        # 1. Automatic Checkpoint - Find the latest pb_updated in Frappe
        latest_pb_updated = frappe.db.get_value("Active Leads", 
            filters={"pb_id": ["is", "set"]}, 
            fieldname="pb_updated", 
            order_by="pb_updated desc"
        )

        pb_filter = ""
        if latest_pb_updated:
            pb_filter = f"updated > '{latest_pb_updated}'"

        page = 1
        per_page = 500
        collection_name = "leads"
        
        total_fetched = 0
        inserted_count = 0
        updated_count = 0
        failed_count = 0

        # Initial call to get active incremental items
        initial_res = fetch_pb_records(pb_url, pb_token, collection_name, 1, 1, pb_filter, return_raw=True)
        total_items = initial_res.get('totalItems', 0) if initial_res else 0

        # We need a fast lookup for ONLY the IDs we fetch, not the entire database
        # This prevents loading huge dicts into memory.
        
        while True:
            records = fetch_pb_records(pb_url, pb_token, collection_name, page, per_page, pb_filter)
            if not records:
                break

            fetched_ids = [r.get('id') for r in records if r.get('id')]
            if not fetched_ids:
                break
            
            # See which ones already exist in Frappe
            existing_db = frappe.db.sql(
                "SELECT pb_id, name FROM `tabActive Leads` WHERE pb_id IN %s", 
                (tuple(fetched_ids),), as_dict=True
            )
            existing_map = {r.pb_id: r.name for r in existing_db}

            to_insert = []
            to_update = []

            for record in records:
                pb_id = record.get('id')
                if not pb_id: continue

                if pb_id in existing_map:
                    record['frappe_name'] = existing_map[pb_id]
                    to_update.append(record)
                else:
                    record['frappe_name'] = f"AL-{pb_id}"
                    to_insert.append(record)

            total_fetched += len(records)

            try:
                if to_insert:
                    perform_batch_insert(to_insert)
                    inserted_count += len(to_insert)
                if to_update:
                    perform_batch_update(to_update)
                    updated_count += len(to_update)
            except Exception as e:
                frappe.log_error(f"Batch SQL Error: {str(e)}\n\n{frappe.get_traceback()}", "Active Leads Batch Error")
                failed_count += len(records)

            if total_items > 0:
                progress = int((total_fetched / total_items) * 100)
                if progress > 100: progress = 100
                publish_progress(progress, f"Incremental Sync: {total_fetched} of {total_items}...")

            if len(records) < per_page:
                break
            page += 1
            frappe.db.commit()

        frappe.db.commit()
        summary = f"Incremental Sync Complete! Inserted: {inserted_count}, Updated: {updated_count}, Failed: {failed_count}"
        if total_items == 0:
            summary = "Everything is up-to-date. No new changes found."
            
        publish_progress(100, summary)

    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Active Leads Sync Fatal Error")
        publish_progress(0, "Sync Failed. Check Error Log.", failed=True)

def perform_batch_insert(records):
    fields = [
        'name', 'creation', 'modified', 'owner', 'modified_by', 'docstatus',
        'employee_name', 'employee_code', 'customer_name', 'mobile_no', 'city',
        'lead_status', 'lead_status_date', 'lead_status_time', 'data_status',
        'total_calls', 'connected_calls', 'total_duration', 'arn_no', 'segment',
        'employer', 'product', 'remarks', 'decline_reason', 
        'assigned_date', 'assigned_time', 'followup_date', 'followup_time', 'date_of_birth',
        'data_code', 'data_sub_code', 'custom_code', 
        'pb_id', 'pb_created', 'pb_updated', 'clean_from_pb', 'deleted_from_pb'
    ]

    now_val = now_datetime()
    values = []

    for pb in records:
        lead_date, lead_time = split_dt(pb.get('lead_status_date'), both=True)
        assign_date, assign_time = split_dt(pb.get('assigned_date'), both=True)
        follow_date, follow_time = split_dt(pb.get('followup_time'), both=True)
        dob = split_dt(pb.get('date_of_birth'), both=False)
        
        name = pb.get('frappe_name')

        values.append((
            name, now_val, now_val, 'Administrator', 'Administrator', 0,
            pb.get('employee_name'), pb.get('employee_code'), pb.get('customer_name'), pb.get('mobile_no'), pb.get('city'),
            pb.get('lead_status'), lead_date, lead_time, pb.get('data_status'),
            pb.get('total_calls') or 0, pb.get('connected_calls') or 0, pb.get('total_duration') or 0, pb.get('arn_no'), pb.get('segment'),
            pb.get('employer'), pb.get('product'), pb.get('remarks'), pb.get('decline_reason'),
            assign_date, assign_time, follow_date, follow_time, dob,
            pb.get('data_code'), pb.get('data_sub_code'), pb.get('custom_code'),
            pb.get('id'), pb.get('created'), pb.get('updated'), 0, 0
        ))

    placeholders = "(" + ", ".join(["%s"] * len(fields)) + ")"
    placeholders_list = ", ".join([placeholders] * len(values))
    flattened_values = [val for row in values for val in row]

    query = f"""
        INSERT IGNORE INTO `tabActive Leads` (
            {', '.join(['`' + f + '`' for f in fields])}
        ) VALUES {placeholders_list}
    """
    frappe.db.sql(query, flattened_values)

def perform_batch_update(records):
    now_val = now_datetime()
    for pb in records:
        lead_date, lead_time = split_dt(pb.get('lead_status_date'), both=True)
        assign_date, assign_time = split_dt(pb.get('assigned_date'), both=True)
        follow_date, follow_time = split_dt(pb.get('followup_time'), both=True)
        dob = split_dt(pb.get('date_of_birth'), both=False)

        frappe.db.sql("""
            UPDATE `tabActive Leads` SET
                modified=%s, employee_name=%s, employee_code=%s, customer_name=%s, mobile_no=%s, city=%s,
                lead_status=%s, lead_status_date=%s, lead_status_time=%s, data_status=%s,
                total_calls=%s, connected_calls=%s, total_duration=%s, arn_no=%s, segment=%s,
                employer=%s, product=%s, remarks=%s, decline_reason=%s,
                assigned_date=%s, assigned_time=%s, followup_date=%s, followup_time=%s, date_of_birth=%s,
                data_code=%s, data_sub_code=%s, custom_code=%s,
                pb_updated=%s
            WHERE pb_id=%s
        """, (
            now_val, pb.get('employee_name'), pb.get('employee_code'), pb.get('customer_name'), pb.get('mobile_no'), pb.get('city'),
            pb.get('lead_status'), lead_date, lead_time, pb.get('data_status'),
            pb.get('total_calls') or 0, pb.get('connected_calls') or 0, pb.get('total_duration') or 0, pb.get('arn_no'), pb.get('segment'),
            pb.get('employer'), pb.get('product'), pb.get('remarks'), pb.get('decline_reason'),
            assign_date, assign_time, follow_date, follow_time, dob,
            pb.get('data_code'), pb.get('data_sub_code'), pb.get('custom_code'),
            pb.get('updated'), pb.get('id')
        ))

def split_dt(utc_str, both=False):
    if not utc_str:
        return (None, None) if both else None
    try:
        utc_str = str(utc_str).replace(' ', 'T')
        if utc_str.endswith('Z'): utc_str = utc_str[:-1]
        dt = datetime.fromisoformat(utc_str)
        ist = dt.replace(tzinfo=pytz.UTC).astimezone(pytz.timezone('Asia/Kolkata'))
        return (ist.date(), ist.strftime("%H:%M:%S")) if both else ist.date()
    except Exception:
        return (None, None) if both else None

def fetch_pb_records(pb_url, pb_token, collection, page, per_page, pb_filter="", return_raw=False):
    try:
        url = f"{pb_url.rstrip('/')}/api/collections/{collection}/records"
        headers = {"Authorization": f"Bearer {pb_token}"}
        params = {"page": page, "perPage": per_page, "sort": "+updated"} # Oldest updated first for smooth checkpoints
        if pb_filter:
            params["filter"] = f"({pb_filter})"
            
        res = requests.get(url, headers=headers, params=params, timeout=30)
        if res.status_code == 200:
            data = res.json()
            return data if return_raw else data.get('items', [])
        return {} if return_raw else []
    except Exception:
        return {} if return_raw else []

def publish_progress(percentage, message, failed=False):
    if percentage > 100: percentage = 100
    frappe.publish_realtime("active_leads_sync_progress", {"percentage": percentage, "message": message, "failed": failed})
