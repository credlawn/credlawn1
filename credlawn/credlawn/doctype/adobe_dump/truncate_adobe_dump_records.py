import frappe
from frappe import _

@frappe.whitelist()
def truncate_adobe_dump_records(password):
    # Verify current user's password for safety
    from frappe.auth import LoginManager
    login_manager = LoginManager()
    
    try:
        login_manager.check_password(frappe.session.user, password)
    except frappe.AuthenticationError:
        frappe.throw(_("Invalid password. Truncate action cancelled."))

    frappe.db.sql("DELETE FROM `tabAdobe Dump`")
    frappe.db.commit()
    return _("All Adobe Dump Records have been cleared.")
