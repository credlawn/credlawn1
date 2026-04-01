import frappe
from frappe import _
import requests

@frappe.whitelist()
def execute_archive(password):
    """Main entry for Active Leads archiving with password verification."""
    # Verify password
    from frappe.auth import LoginManager
    login_manager = LoginManager()
    try:
        login_manager.check_password(frappe.session.user, password)
    except frappe.AuthenticationError:
        frappe.throw(_("Invalid password. Archive cancelled."))

    # Enqueue background job
    frappe.enqueue(
        "credlawn.credlawn.doctype.active_leads.archive_active_leads.archive_job",
        queue="long",
        timeout=3600,
        job_name="active_leads_archive_pb"
    )
    
    return _("Active Leads archiving started in the background (Silent).")

def archive_job():
    """Silent background job to batch delete from PB and update Frappe."""
    try:
        pb_url = frappe.conf.get("pocketbase_url")
        pb_token = frappe.conf.get("pocketbase_auth_token")

        if not pb_url or not pb_token:
            return

        # Fetch records marked to archive
        records_to_archive = frappe.get_all(
            "Active Leads", 
            filters={"clean_from_pb": 1, "pb_id": ["is", "set"]}, 
            fields=["name", "pb_id"]
        )

        if not records_to_archive:
            return

        total_records = len(records_to_archive)
        batch_size = 50
        success_count = 0

        api_url = f"{pb_url.rstrip('/')}/api/batch"
        headers = {"Authorization": f"Bearer {pb_token}", "Content-Type": "application/json"}

        for i in range(0, total_records, batch_size):
            batch = records_to_archive[i:i + batch_size]
            batch_requests = [{"method": "DELETE", "url": f"/api/collections/leads/records/{r.pb_id}"} for r in batch]
            
            try:
                response = requests.post(api_url, headers=headers, json={"requests": batch_requests}, timeout=60)
                
                if response.status_code == 200:
                    success_count += len(batch)
                    
                    # Optimized record update in Frappe
                    batch_names = [r.name for r in batch]
                    frappe.db.sql("""
                        UPDATE `tabActive Leads` 
                        SET 
                            clean_from_pb = 0, 
                            deleted_from_pb = 1, 
                            pb_id = '' 
                        WHERE name IN %s
                    """, (tuple(batch_names),))
                else:
                    frappe.log_error(f"Active Leads PB Archive Error: {response.text}", "Active Leads Archive Error")

            except Exception as e:
                frappe.log_error(f"Active Leads Archive Batch Request Failed: {str(e)}", "Active Leads Archive Fatal Batch Error")

            frappe.db.commit()

        summary = f"Successfully deleted {success_count} Active Leads from Pocketbase"
        frappe.log_error(summary, "Active Leads Archive Completed")

    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Active Leads Archive Job Fatal Error")
