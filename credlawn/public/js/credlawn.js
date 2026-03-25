
const CREDLAWN_DOCTYPES = [
    'Leads',
    // ... apne doctypes
];

CREDLAWN_DOCTYPES.forEach(function(doctype) {
    frappe.listview_settings[doctype] = frappe.listview_settings[doctype] || {};
    frappe.listview_settings[doctype].hide_name_column = true;
    frappe.listview_settings[doctype].hide_name_filter = true;
});
