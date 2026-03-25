import frappe
import requests
import datetime
from frappe import _
from frappe.utils import now_datetime

# Optional: set record_count > 0 for testing small batches
record_count = 0 

@frappe.whitelist()
def pull_ipa_data(full_sync=False):
    """Entry point to start the background sync job."""
    # Check if a sync is already running
    if frappe.db.get_single_value("Sync Settings", "is_ipa_sync_running"):
        frappe.msgprint(_("IPA Sync is already running in the background."))
        return
    
    # Start background job
    frappe.enqueue(
        "credlawn.credlawn.doctype.ipa_records.pull_ipa_code.sync_job",
        queue="long",
        timeout=3600,
        full_sync=frappe.parse_json(full_sync)
    )
    
    return _("IPA Sync started in the background. Check progress bar.")

def sync_job(full_sync=False):
    """Main background job for syncing data from Pocketbase."""
    # Capture job start time for the next sync checkpoint (UTC)
    job_start_time = datetime.datetime.utcnow()
    
    try:
        # Mark sync as running
        frappe.db.set_value("Sync Settings", "Sync Settings", "is_ipa_sync_running", 1)
        frappe.db.commit()

        pb_url = frappe.conf.get("pocketbase_url")
        pb_token = frappe.conf.get("pocketbase_auth_token")

        if not pb_url or not pb_token:
            raise Exception("Pocketbase configuration missing in site_config.json")

        api_url = f"{pb_url.rstrip('/')}/api/collections/case_login/records"
        headers = {"Authorization": f"Bearer {pb_token}"}
        
        # Get last sync time for incremental sync
        last_sync = None if full_sync else frappe.db.get_single_value("Sync Settings", "ipa_last_sync_time")
        
        # Build filter
        pb_filter = '(lead_status="IP Approved")'
        if last_sync:
            # Pocketbase prefers ISO 8601 format with UTC 'Z' suffix (e.g. 2023-01-01 10:00:00.000Z)
            last_sync_safe = last_sync - datetime.timedelta(minutes=10)
            last_sync_str = last_sync_safe.strftime('%Y-%m-%d %H:%M:%S.000Z')
            pb_filter += f' && (updated > "{last_sync_str}")'

        # Pre-fetch existing records for lookup efficiency
        existing_records = {d.arn_no.upper(): d.name for d in frappe.get_all("IPA Records", fields=["name", "arn_no"]) if d.arn_no}
        
        success_count = 0
        error_count = 0
        total_processed = 0
        
        page = 1
        per_page = 200
        
        # Initial call to get total count for progress bar
        initial_params = {"filter": pb_filter, "page": 1, "perPage": 1}
        initial_res = requests.get(api_url, headers=headers, params=initial_params, timeout=30)
        initial_res.raise_for_status()
        total_items_to_sync = initial_res.json().get("totalItems", 0)
        
        if total_items_to_sync == 0:
            publish_progress(100, "No new records to sync.")
            finish_job(0, 0, 0, job_start_time)
            return

        while True:
            params = {
                "filter": pb_filter,
                "page": page,
                "perPage": per_page,
                "sort": "updated" # Process oldest modified first
            }
            
            response = requests.get(api_url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            items = data.get("items", [])
            
            if not items:
                break

            for item in items:
                if record_count > 0 and total_processed >= record_count:
                    break
                
                try:
                    raw_arn = item.get("arn_no")
                    if not raw_arn:
                        continue
                    
                    arn_no = raw_arn.upper()
                    
                    # Data Mapping
                    arn_date = item.get("arn_date").split(' ')[0] if item.get("arn_date") else None
                    dob = item.get("date_of_birth")
                    if dob:
                        dob = dob.split('T')[0] if 'T' in dob else dob.split(' ')[0]

                    login_type = item.get("login_type")
                    unique_val = "Yes" if login_type == "Unique" else "No" if login_type == "Duplicate" else ""

                    doc_data = {
                        "doctype": "IPA Records",
                        "customer_name": item.get("customer_name"),
                        "mobile_no": item.get("mobile_number"),
                        "employee_name": item.get("employee_name"),
                        "employee_code": item.get("employee_code"),
                        "ip_status": item.get("lead_status"),
                        "arn_no": arn_no,
                        "arn_date": arn_date,
                        "date_of_birth": dob,
                        "unique": unique_val
                    }

                    if arn_no in existing_records:
                        doc_name = existing_records[arn_no]
                        doc = frappe.get_doc("IPA Records", doc_name)
                        doc.update(doc_data)
                        doc.save(ignore_permissions=True)
                    else:
                        doc = frappe.get_doc(doc_data)
                        doc.insert(ignore_permissions=True)
                        existing_records[arn_no] = doc.name
                    
                    success_count += 1
                except Exception as e:
                    error_count += 1
                    frappe.log_error(f"Sync Error (ARN: {item.get('arn_no')}): {str(e)}", "IPA Sync Job")
                
                total_processed += 1
                
                # Publish progress every 10 records or at the end of a page
                if total_processed % 10 == 0 or total_processed == total_items_to_sync:
                    progress = int((total_processed / total_items_to_sync) * 100)
                    msg = _("Syncing {0} of {1} records...").format(total_processed, total_items_to_sync)
                    publish_progress(progress, msg)

            if record_count > 0 and total_processed >= record_count:
                break
            
            if page * per_page >= total_items_to_sync:
                break
                
            page += 1
            frappe.db.commit()

        # Update last sync time and mark as finished
        finish_job(success_count, error_count, total_processed, job_start_time)

    except Exception as e:
        frappe.db.set_value("Sync Settings", "Sync Settings", "is_ipa_sync_running", 0)
        frappe.db.commit()
        frappe.log_error(f"Global Sync Job Error: {str(e)}", "IPA Sync Job")
        publish_progress(0, _("Sync failed. Check Error Logs."), failed=True)

def publish_progress(percentage, message, failed=False):
    """Publish real-time progress via Socket.io."""
    frappe.publish_realtime("ipa_sync_progress", {
        "percentage": percentage,
        "message": message,
        "failed": failed
    })

def finish_job(success, error, total, start_time):
    """Mark job as finished and update settings."""
    frappe.db.set_value("Sync Settings", "Sync Settings", {
        "is_ipa_sync_running": 0,
        "ipa_last_sync_time": start_time
    })
    frappe.db.commit()
    
    summary = f"IPA Sync Summary: {success} Success, {error} Failed. Total: {total}"
    frappe.log_error(summary, "IPA Sync Completed")
    publish_progress(100, summary)
