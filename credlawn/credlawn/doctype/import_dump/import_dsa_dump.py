import frappe
from frappe import _

@frappe.whitelist()
def run_import_sample():
    return _("DSA Import Logic: Script Called Successfully (Sample)")
