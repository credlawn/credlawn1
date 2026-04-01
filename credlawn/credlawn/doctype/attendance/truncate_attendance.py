import frappe
from frappe import _

@frappe.whitelist()
def truncate_attendance(password):
    # Verify current user's password for safety
    from frappe.auth import LoginManager
    login_manager = LoginManager()
    
    try:
        login_manager.check_password(frappe.session.user, password)
    except frappe.AuthenticationError:
        frappe.throw(_("Invalid password. Truncate action cancelled."))

    # Note: Using TRUNCATE or DELETE FROM depends on requirement. 
    # For Frappe, DELETE FROM `tabAttendance` is standard for deleting records via script.
    frappe.db.sql("DELETE FROM `tabAttendance`")
    frappe.db.commit()
    return _("All Attendance Records have been cleared.")
