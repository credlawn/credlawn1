// Copyright (c) 2026, Credlawn India and contributors
// For license information, please see license.txt

frappe.listview_settings['Leads'] = {
    onload: function (listview) {
        // Pull Sync Button
        listview.page.add_inner_button(__('Pull Sync'), function () {
            frappe.call({
                method: 'credlawn.credlawn.doctype.leads.pull_sync.execute_pull_sync',
                callback: function (r) {
                    if (r.message) {
                        frappe.msgprint(__('Pull Sync completed successfully'));
                        listview.refresh();
                    }
                }
            });
        });

        // Push Sync Button
        listview.page.add_inner_button(__('Push Sync'), function () {
            frappe.call({
                method: 'credlawn.credlawn.doctype.leads.push_sync.execute_push_sync',
                callback: function (r) {
                    if (r.message) {
                        frappe.msgprint(__('Push Sync completed successfully'));
                        listview.refresh();
                    }
                }
            });
        });
    }
};
