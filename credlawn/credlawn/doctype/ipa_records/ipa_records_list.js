frappe.listview_settings['IPA Records'] = {
    hide_name_column: true,
    hide_name_filter: true,

    onload: function (listview) {
        // Listen for real-time sync progress
        frappe.realtime.on('ipa_sync_progress', (data) => {
            if (data.failed) {
                frappe.show_progress(__('Sync Failed'), 100, 100, data.message);
                setTimeout(() => frappe.hide_progress(), 5000);
                return;
            }

            frappe.show_progress(__('Syncing IPA Records'), data.percentage, 100, data.message);
            
            if (data.percentage >= 100) {
                setTimeout(() => {
                    frappe.hide_progress();
                    listview.refresh();
                }, 3000);
            }
        });

        listview.page.add_inner_button(__('Pull IPA'), function () {
            frappe.confirm(
                __('Start Incremental IPA Sync? (Only new/modified records will be pulled)'),
                function () {
                    frappe.call({
                        method: 'credlawn.credlawn.doctype.ipa_records.pull_ipa_code.pull_ipa_data',
                        callback: function (r) {
                            if (r.message) {
                                frappe.show_alert({
                                    message: r.message,
                                    indicator: 'green'
                                });
                            }
                        }
                    });
                }
            );
        });

        listview.page.add_inner_button(__('Re-map Employee'), function () {
            frappe.show_progress(__('Re-mapping Data'), 0, 100, __('Fetching matches from Adobe and DSA Dumps...'));
            frappe.call({
                method: 'credlawn.credlawn.doctype.ipa_records.remap_employee.remap_all_data',
                callback: function (r) {
                    frappe.hide_progress();
                    if (r.message) {
                        frappe.msgprint(r.message);
                        listview.refresh();
                    }
                }
            });
        }, __('Task'));

        listview.page.add_inner_button(__('Full Sync'), function () {
            frappe.confirm(
                __('Are you sure you want to perform a Full Sync? This will re-check all records from Pocketbase regardless of the last sync time.'),
                function () {
                    frappe.call({
                        method: 'credlawn.credlawn.doctype.ipa_records.pull_ipa_code.pull_ipa_data',
                        args: { full_sync: true },
                        callback: function (r) {
                            if (r.message) {
                                frappe.show_alert({ message: r.message, indicator: 'green' });
                            }
                        }
                    });
                }
            );
        }, __('Task'));

        listview.page.add_inner_button(__('Truncate'), function () {
            frappe.prompt([
                {
                    label: __('Enter your Login Password to confirm'),
                    fieldname: 'password',
                    fieldtype: 'Password',
                    reqd: 1
                }
            ], (values) => {
                frappe.call({
                    method: 'credlawn.credlawn.doctype.ipa_records.truncate_ipa_records.truncate_ipa_records',
                    args: {
                        password: values.password
                    },
                    callback: function (r) {
                        if (r.message) {
                            frappe.show_alert({
                                message: r.message,
                                indicator: 'green'
                            });
                            listview.refresh();
                        }
                    }
                });
            }, __('Sensitive Action: Truncate All Records'), __('Confirm'));
        }, __('Task'));

    }
};
