import frappe
from frappe.utils import add_days, today
from frappe import _

@frappe.whitelist()
def execute_mark():
    """Marks Call Log records older than 10 days for archiving."""
    try:
        # 1. Calculate the cutoff date (records older than 10 days)
        cutoff_date = add_days(today(), -10)
        
        # 2. Count records that meet the criteria
        count_to_mark = frappe.db.count('Call Log', {
            'call_date': ('<', cutoff_date),
            'pb_id': ('is', 'set'),
            'clean_from_pb': 0,
            'deleted_from_pb': 0
        })
        
        if count_to_mark == 0:
            return _("No Call Log records found older than 10 days.")

        # 3. Bulk mark records
        frappe.db.sql("""
            UPDATE `tabCall Log`
            SET clean_from_pb = 1
            WHERE 
                call_date < %s 
                AND pb_id IS NOT NULL 
                AND pb_id != '' 
                AND clean_from_pb = 0 
                AND deleted_from_pb = 0
        """, (cutoff_date,))
        
        frappe.db.commit()
        
        return _("Marked {0} Call Logs for archiving (older than {1}).").format(count_to_mark, cutoff_date)

    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Mark Call Logs Error")
        frappe.throw(_("Failed to mark records: {0}").format(str(e)))
