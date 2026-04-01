import frappe
from frappe import _

@frappe.whitelist()
def execute_mark():
    """High-speed batch marking of Active Leads for archival using direct SQL."""
    try:
        # Smart Logic for Retention:
        # 'Called' / 'New' -> Skip completely
        # 'IP Approved' -> 31 days
        # 'Follow Up' -> 5 days
        # Others -> 2 days (Based on lead_status_date)
        
        query = """
            UPDATE `tabActive Leads`
            SET clean_from_pb = 1
            WHERE clean_from_pb = 0 
              AND deleted_from_pb = 0
              AND pb_id IS NOT NULL
              AND pb_id != ''
              AND lead_status NOT IN ('Called', 'New')
              AND lead_status_date IS NOT NULL
              AND (
                  (lead_status = 'IP Approved' AND lead_status_date < DATE_SUB(CURDATE(), INTERVAL 31 DAY))
                  OR
                  (lead_status = 'Follow Up' AND lead_status_date < DATE_SUB(CURDATE(), INTERVAL 5 DAY))
                  OR
                  (lead_status NOT IN ('IP Approved', 'Follow Up') AND lead_status_date < DATE_SUB(CURDATE(), INTERVAL 2 DAY))
              )
        """
        
        frappe.db.sql(query)
        marked_count = frappe.db.sql("SELECT ROW_COUNT()")[0][0]
        frappe.db.commit()

        return _(f"Successfully marked {marked_count} Active Leads for archival.")
        
    except Exception as e:
        frappe.db.rollback()
        frappe.log_error(frappe.get_traceback(), "Mark Active Leads Error")
        frappe.throw(_("Failed to mark Active Leads. Check Error Log."))
