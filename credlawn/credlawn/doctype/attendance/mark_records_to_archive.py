import frappe
from frappe.utils import add_days, today
from frappe import _

@frappe.whitelist()
def execute_mark():
    """Marks records older than 31 days for archiving from Pocketbase."""
    try:
        # 1. Calculate the cutoff date (records older than 31 days)
        cutoff_date = add_days(today(), -31)
        
        # 2. Count records that meet the criteria before updating
        count_to_mark = frappe.db.count('Attendance', {
            'attendance_date': ('<', cutoff_date),
            'pb_id': ('is', 'set'),
            'clean_from_pb': 0,
            'deleted_from_pb': 0
        })
        
        if count_to_mark == 0:
            return _("No records found older than 31 days that need marking.")

        # 3. Use high-performance direct SQL to mark records
        frappe.db.sql("""
            UPDATE `tabAttendance`
            SET clean_from_pb = 1
            WHERE 
                attendance_date < %s 
                AND pb_id IS NOT NULL 
                AND pb_id != '' 
                AND clean_from_pb = 0 
                AND deleted_from_pb = 0
        """, (cutoff_date,))
        
        frappe.db.commit()
        
        return _("Successfully marked {0} records for archiving (attendance date before {1}).").format(count_to_mark, cutoff_date)

    except Exception as e:
        frappe.log_error(frappe.get_traceback(), _("Mark to Archive Error"))
        frappe.throw(_("Failed to mark records: {0}").format(str(e)))
