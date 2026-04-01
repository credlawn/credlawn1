import frappe
from frappe import _
import requests

@frappe.whitelist()
def execute_archive(password):
    """Main entry for Call Log archiving with password verification."""
    # Verify password
    from frappe.auth import LoginManager
    login_manager = LoginManager()
    try:
        login_manager.check_password(frappe.session.user, password)
    except frappe.AuthenticationError:
        frappe.throw(_("Invalid password. Archive cancelled."))

    # Enqueue background job
    frappe.enqueue(
        "credlawn.credlawn.doctype.call_log.archive_call_logs.archive_job",
        queue="long",
        timeout=3600,
        job_name="call_log_archive_pb"
    )
    
    return _("Call Log archiving started in the background.")

def archive_job():
    """Background job to batch delete from PB and update Frappe."""
    try:
        pb_url = frappe.conf.get("pocketbase_url")
        pb_token = frappe.conf.get("pocketbase_auth_token")

        if not pb_url or not pb_token:
            return

        # Fetch records marked to archive
        records_to_archive = frappe.get_all(
            "Call Log", 
            filters={"clean_from_pb": 1, "pb_id": ["is", "set"]}, 
            fields=["name", "pb_id"]
        )

        if not records_to_archive:
            publish_progress(100, "No records found to archive.")
            return

        total_records = len(records_to_archive)
        batch_size = 50
        processed_count = 0
        success_count = 0

        api_url = f"{pb_url.rstrip('/')}/api/batch"
        headers = {"Authorization": f"Bearer {pb_token}", "Content-Type": "application/json"}

        for i in range(0, total_records, batch_size):
            batch = records_to_archive[i:i + batch_size]
            batch_requests = [{"method": "DELETE", "url": f"/api/collections/call_logs/records/{r.pb_id}"} for r in batch]
            
            try:
                response = requests.post(api_url, headers=headers, json={"requests": batch_requests}, timeout=60)
                
                if response.status_code == 200:
                    success_count += len(batch)
                    
                    # Optimized record update in Frappe
                    batch_names = [r.name for r in batch]
                    frappe.db.sql("""
                        UPDATE `tabCall Log` 
                        SET 
                            clean_from_pb = 0, 
                            deleted_from_pb = 1, 
                            pb_id = '' 
                        WHERE name IN %s
                    """, (tuple(batch_names),))
                else:
                    frappe.log_error(f"PB Archive Error: {response.text}", "Call Log Archive")

            except Exception as e:
                frappe.log_error(f"Archive Batch Request Failed: {str(e)}", "Call Log Archive")

            processed_count += len(batch)
            progress = int((processed_count / total_records) * 100)
            publish_progress(progress, f"Archived {processed_count} of {total_records}...")
            frappe.db.commit()

        summary = f"Call Log Archive Complete! Success: {success_count}"
        publish_progress(100, summary)

    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Call Log Archive Job Fatal Error")
        publish_progress(0, "Archive Failed. Check Error Logs.", failed=True)

def publish_progress(percentage, message, failed=False):
    frappe.publish_realtime("call_log_archive_progress", {"percentage": percentage, "message": message, "failed": failed})
