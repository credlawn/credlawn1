import frappe
import requests
from frappe.model.document import Document

class IPARecords(Document):
    def validate(self):
        """Standard validations and field updates."""
        if self.arn_no:
            self.set_arn_details_from_arn()
        
        # New: Auto-Enrich data and calculate GAP on every save/validate
        self.enrich_from_active_database()

    def enrich_from_active_database(self):
        """Fetches latest codes and calculates GAP from Active Database."""
        from frappe.utils import getdate, date_diff

        if not self.mobile_no:
            return

        # Fetch latest enrichment data from local DB
        enriched = frappe.db.get_value("Active Database", 
            {"mobile_no": self.mobile_no}, 
            ["data_code", "custom_code", "old_arn_no", "old_decision_date"], 
            as_dict=True
        )

        if enriched:
            # Update fields
            self.data_code = enriched.get("data_code")
            self.custom_code = enriched.get("custom_code")
            self.old_arn_no = enriched.get("old_arn_no")
            
            # Smart Date Selection for Gap
            target_old_date = enriched.get("old_decision_date")
            if not target_old_date and self.old_arn_no:
                parsed_date, _, _ = self.parse_any_arn(self.old_arn_no)
                target_old_date = parsed_date
            
            self.old_decision_date = target_old_date

            # Calculate Final GAP
            try:
                if self.arn_date and self.old_decision_date:
                    self.gap = date_diff(getdate(self.arn_date), getdate(self.old_decision_date))
            except:
                self.gap = 0
        else:
            # Fallback for gap if no historical record
            self.gap = 0

    def parse_any_arn(self, arn):
        """Standalone helper for parsing ANY arn string provided."""
        if not arn or len(arn) < 6: return None, None, False
        month_map = { 'A': ('01', 'Jan'), 'B': ('02', 'Feb'), 'C': ('03', 'Mar'), 'D': ('04', 'Apr'), 'E': ('05', 'May'), 'F': ('06', 'Jun'), 'G': ('07', 'Jul'), 'H': ('08', 'Aug'), 'I': ('09', 'Sep'), 'J': ('10', 'Oct'), 'K': ('11', 'Nov'), 'L': ('12', 'Dec') }
        try:
            yy, m_char, dd_str = arn[1:3], arn[3].upper(), arn[4:6]
            m_data = month_map.get(m_char)
            if m_data and yy.isdigit() and dd_str.isdigit():
                mm_num, mmm_name = m_data
                if 1 <= int(dd_str) <= 31:
                    return f"20{yy}-{mm_num}-{dd_str}", f"{mmm_name}-{yy}", True
        except: pass
        return None, None, False

    def on_update(self):
        """Two-Way Sync: Manual changes to arn_no push back to PocketBase in BACKGROUND."""
        if hasattr(self.flags, 'ignore_pb_sync') and self.flags.ignore_pb_sync:
            return

        if self.pb_id and self.has_value_changed('arn_no'):
            # Offload to background worker
            frappe.enqueue(
                'credlawn.credlawn.doctype.ipa_records.ipa_records.push_arn_update_to_pb',
                doc_name=self.name,
                pb_id=self.pb_id,
                new_arn=self.arn_no,
                now=frappe.flags.in_test or frappe.flags.in_import
            )

    def set_arn_details_from_arn(self):
        """
        Parses arn_no (e.g., D26D10...) with strict validation.
        """
        if self.arn_no and len(self.arn_no) >= 6:
            month_map = { 
                'A': ('01', 'Jan'), 'B': ('02', 'Feb'), 'C': ('03', 'Mar'), 'D': ('04', 'Apr'), 
                'E': ('05', 'May'), 'F': ('06', 'Jun'), 'G': ('07', 'Jul'), 'H': ('08', 'Aug'), 
                'I': ('09', 'Sep'), 'J': ('10', 'Oct'), 'K': ('11', 'Nov'), 'L': ('12', 'Dec') 
            }
            try:
                yy = self.arn_no[1:3]
                m_char = self.arn_no[3].upper()
                dd_str = self.arn_no[4:6]
                m_data = month_map.get(m_char)
                
                # Validation: Year and Day must be numeric
                if m_data and yy.isdigit() and dd_str.isdigit():
                    mm_num, mmm_name = m_data
                    day_int = int(dd_str)
                    if 1 <= day_int <= 31:
                        self.arn_date = f"20{yy}-{mm_num}-{dd_str}"
                        self.arn_month = f"{mmm_name}-{yy}"
            except Exception:
                pass

@frappe.whitelist()
def push_arn_update_to_pb(doc_name, pb_id, new_arn):
    """Background logic with Update Error tracking."""
    import time
    pb_url = frappe.conf.get("pocketbase_url")
    pb_token = frappe.conf.get("pocketbase_auth_token")

    if not pb_url or not pb_token or not pb_id:
        return

    api_url = f"{pb_url.rstrip('/')}/api/collections/case_login/records/{pb_id}"
    headers = { "Authorization": f"Bearer {pb_token}", "Content-Type": "application/json" }
    payload = {"arn_no": new_arn}

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.patch(api_url, headers=headers, json=payload, timeout=10)
            if response.status_code == 200:
                # SUCCESS: Clear the error flag if set
                frappe.db.set_value("IPA Records", doc_name, "update_error", 0)
                frappe.db.commit()
                return 
            
            if attempt == max_retries - 1:
                # FINAL FAILURE: Set the error flag
                frappe.db.set_value("IPA Records", doc_name, "update_error", 1)
                frappe.db.commit()
                frappe.log_error(f"Sync Failed for {doc_name}: {response.text}", "PB Sync Error")
        
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt == max_retries - 1:
                frappe.db.set_value("IPA Records", doc_name, "update_error", 1)
                frappe.db.commit()
                frappe.log_error(f"Sync Connection Error for {doc_name}: {str(e)}", "PB Sync Fatal")
            else:
                time.sleep(1)
        except Exception as e:
            frappe.db.set_value("IPA Records", doc_name, "update_error", 1)
            frappe.db.commit()
            frappe.log_error(f"Unexpected Sync Error for {doc_name}: {str(e)}", "PB Sync Fatal")
            break
