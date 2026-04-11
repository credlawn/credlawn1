import frappe
import requests
import datetime
import json
from frappe import _
from frappe.utils import getdate, date_diff

@frappe.whitelist()
def pull_ipa_data():
    """Entry point for the Smart Incremental Sync."""
    if frappe.cache().get_value("ipa_sync_active"):
        frappe.msgprint(_("IPA Sync is already running."))
        return
    
    frappe.enqueue(
        "credlawn.credlawn.doctype.ipa_records.pull_ipa_code.run_smart_sync",
        queue="long",
        timeout=3600
    )
    return _("Smart IPA Sync initiated. Monitoring progress...")

def run_smart_sync():
    """Fully decentralized sync logic with Advanced Enrichment and GAP Analysis."""
    try:
        frappe.cache().set_value("ipa_sync_active", 1, expires_in_sec=3600)
        
        last_updated = frappe.db.sql("SELECT MAX(pb_updated) FROM `tabIPA Records`")[0][0]
        
        pb_url = frappe.conf.get("pocketbase_url")
        pb_token = frappe.conf.get("pocketbase_auth_token")

        if not pb_url or not pb_token:
            raise Exception("Pocketbase configuration missing.")

        api_url = f"{pb_url.rstrip('/')}/api/collections/case_login/records"
        headers = {"Authorization": f"Bearer {pb_token}"}
        
        pb_filter = '(lead_status="IP Approved")'
        if last_updated:
            pb_filter += f' && (updated > "{last_updated}")'

        existing_map = {d.pb_id: d.name for d in frappe.get_all("IPA Records", fields=["name", "pb_id"]) if d.pb_id}
        
        page = 1
        per_page = 200
        success_count = 0
        malformed_arns = []
        
        first_resp = requests.get(api_url, headers=headers, params={"filter": pb_filter, "perPage": 1}, timeout=30)
        total_to_sync = first_resp.json().get("totalItems", 0)

        if total_to_sync == 0:
            publish_progress(100, "Local data is already up-to-date.")
            return

        while True:
            res = requests.get(api_url, headers=headers, params={"filter": pb_filter, "page": page, "perPage": per_page, "sort": "updated"}, timeout=30).json()
            items = res.get("items", [])
            if not items: break

            mobile_list = [i.get('mobile_number') for i in items if i.get('mobile_number')]
            enrichment_map = {}
            if mobile_list:
                # Optimized Bulk Query including Old ARN and Old Decision Date
                active_data = frappe.db.get_list("Active Database", 
                    filters={"mobile_no": ["in", mobile_list]},
                    fields=["mobile_no", "data_code", "custom_code", "old_arn_no", "old_decision_date"]
                )
                enrichment_map = {d.mobile_no: d for d in active_data}

            for item in items:
                pb_id = item.get("id")
                mobile = item.get("mobile_number")
                arn_no = item.get("arn_no", "")
                enriched = enrichment_map.get(mobile, {})

                # 1. PARSE CURRENT ARN
                arn_date_val, arn_month_val, parsed = parse_arn_full(arn_no)
                if not parsed and arn_no:
                    malformed_arns.append(arn_no)

                # 2. ENRICHMENT & GAP LOGIC
                old_arn = enriched.get("old_arn_no")
                old_decision_date = enriched.get("old_decision_date")
                
                # If old decision date is missing, try to parse it from old_arn_no
                if not old_decision_date and old_arn:
                    parsed_old_date, _, _ = parse_arn_full(old_arn)
                    old_decision_date = parsed_old_date

                # 3. CALCULATE GAP (Days between old_decision_date and current arn_date)
                gap_days = None
                try:
                    if arn_date_val and old_decision_date:
                        gap_days = date_diff(getdate(arn_date_val), getdate(old_decision_date))
                except:
                    pass

                # Clean strings function
                def clean_dt(val):
                    if not val: return None
                    return str(val).split('T')[0].split(' ')[0]

                doc_data = {
                    "doctype": "IPA Records",
                    "pb_id": pb_id,
                    "pb_created": item.get("created"),
                    "pb_updated": item.get("updated"),
                    "customer_name": item.get("customer_name"),
                    "mobile_no": mobile,
                    "employee_name": item.get("employee_name"),
                    "employee_code": item.get("employee_code"),
                    "ip_status": item.get("lead_status"),
                    "arn_no": arn_no,
                    "login_date": clean_dt(item.get("arn_date")),
                    "date_of_birth": clean_dt(item.get("date_of_birth")),
                    "arn_date": arn_date_val,
                    "arn_month": arn_month_val,
                    "unique": "Yes" if item.get("login_type") == "Unique" else "No",
                    # Enrichment Fields
                    "data_code": enriched.get("data_code"),
                    "custom_code": enriched.get("custom_code"),
                    "old_arn_no": old_arn,
                    "old_decision_date": old_decision_date,
                    "gap": gap_days
                }

                if pb_id in existing_map:
                    doc = frappe.get_doc("IPA Records", existing_map[pb_id])
                    doc.flags.ignore_pb_sync = True
                    doc.update(doc_data)
                    doc.save(ignore_permissions=True)
                else:
                    doc = frappe.get_doc(doc_data)
                    doc.flags.ignore_pb_sync = True
                    doc.insert(ignore_permissions=True)
                    existing_map[pb_id] = doc.name
                
                success_count += 1
                if success_count % 10 == 0:
                    publish_progress(int((success_count/total_to_sync)*100), f"Synced {success_count}/{total_to_sync}")

            frappe.db.commit()
            if page * per_page >= total_to_sync: break
            page += 1

        if malformed_arns:
            summary_msg = f"Sync found {len(malformed_arns)} malformed ARNs:\n" + ", ".join(list(set(malformed_arns)))
            frappe.log_error(summary_msg, "IPA Sync: Malformed ARNs List")

        publish_progress(100, f"Sync Finished: {success_count} records.")

    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "IPA Smart Sync Failure")
        publish_progress(0, "Sync Error", failed=True)
    finally:
        frappe.cache().delete_value("ipa_sync_active")

def parse_arn_full(arn_no):
    """Helper to extract Date, Month Name, and Success Flag from ARN."""
    if not arn_no or len(arn_no) < 6:
        return None, None, False
    
    month_map = { 'A': ('01', 'Jan'), 'B': ('02', 'Feb'), 'C': ('03', 'Mar'), 'D': ('04', 'Apr'), 'E': ('05', 'May'), 'F': ('06', 'Jun'), 'G': ('07', 'Jul'), 'H': ('08', 'Aug'), 'I': ('09', 'Sep'), 'J': ('10', 'Oct'), 'K': ('11', 'Nov'), 'L': ('12', 'Dec') }
    
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
    except:
        pass
    return None, None, False

@frappe.whitelist()
def one_time_legacy_sync():
    """Heals existing records by populating PB and Active Database values without Truncate."""
    try:
        # 1. Fetch All IP Approved records from PocketBase in one go (Memory Map)
        pb_url = frappe.conf.get("pocketbase_url")
        pb_token = frappe.conf.get("pocketbase_auth_token")
        if not pb_url or not pb_token: return "Pocketbase Config Missing"
        
        api_url = f"{pb_url.rstrip('/')}/api/collections/case_login/records"
        headers = {"Authorization": f"Bearer {pb_token}"}
        
        # Get all approved items (assuming volume is manageable for one-time fetch)
        res = requests.get(api_url, headers=headers, params={"filter": '(lead_status="IP Approved")', "perPage": 5000}, timeout=60).json()
        pb_items = res.get("items", [])
        
        # Build ARN -> PB Item Map
        pb_map = {item.get("arn_no").upper(): item for item in pb_items if item.get("arn_no")}
        
        # 2. Get local records that need healing
        targets = frappe.get_all("IPA Records", filters={"pb_id": ["is", "not set"]}, fields=["name", "arn_no", "mobile_no"])
        
        count = 0
        for doc in targets:
            arn = doc.arn_no.upper() if doc.arn_no else ""
            pb_item = pb_map.get(arn)
            
            if pb_item:
                # Get existing doc and trigger validate (which handles Gap/Enrichment)
                obj = frappe.get_doc("IPA Records", doc.name)
                obj.pb_id = pb_item.get("id")
                obj.pb_created = pb_item.get("created")
                obj.pb_updated = pb_item.get("updated")
                
                # Flag to ignore PB push during healing
                obj.flags.ignore_pb_sync = True
                obj.save(ignore_permissions=True)
                count += 1
            
            if count % 50 == 0:
                frappe.db.commit()

        return f"Successfully healed {count} records."

    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Legacy Sync Error")
        return f"Error: {str(e)}"

def publish_progress(percentage, message, failed=False):
    frappe.publish_realtime("ipa_sync_progress", {"percentage": percentage, "message": message, "failed": failed})
