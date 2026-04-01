frappe.listview_settings['Call Log'] = {
    hide_name_column: true,
    hide_name_filter: true,

    onload: function (listview) {
        // Listen for real-time progress events
        frappe.realtime.on('call_log_sync_progress', (data) => {
            if (data.failed) {
                frappe.show_progress(__('Sync Failed'), 100, 100, data.message);
                setTimeout(() => frappe.hide_progress(), 5000);
            } else {
                frappe.show_progress(__('Syncing Call Logs'), data.percentage, 100, data.message);
                if (data.percentage >= 100) {
                    setTimeout(() => { frappe.hide_progress(); listview.refresh(); }, 3000);
                }
            }
        });

        frappe.realtime.on('call_log_archive_progress', (data) => {
            if (data.failed) {
                frappe.show_progress(__('Archive Failed'), 100, 100, data.message);
                setTimeout(() => frappe.hide_progress(), 5000);
            } else {
                frappe.show_progress(__('Archiving Call Logs'), data.percentage, 100, data.message);
                if (data.percentage >= 100) {
                    setTimeout(() => { frappe.hide_progress(); listview.refresh(); }, 3000);
                }
            }
        });

        // Sync Call Logs Button (Main Area)
        listview.page.add_inner_button(__('Sync Call Logs'), function () {
            frappe.confirm(
                __('Are you sure you want to sync Call Logs? This uses High-Speed Batch SQL (New Records Only).'),
                function () {
                    frappe.call({
                        method: 'credlawn.credlawn.doctype.call_log.sync_call_log.execute_sync',
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

        // Administrative Tasks (Task Menu)
        listview.page.add_inner_button(__('Mark to Archive'), function () {
            frappe.confirm(
                __('Are you sure you want to mark Call Logs older than 10 days for archiving?'),
                function () {
                    frappe.call({
                        method: 'credlawn.credlawn.doctype.call_log.mark_call_logs_to_archive.execute_mark',
                        callback: function (r) {
                            if (r.message) {
                                frappe.msgprint({
                                    message: r.message,
                                    indicator: 'green',
                                    title: __('Archive Marking Complete')
                                });
                                listview.refresh();
                            }
                        }
                    });
                }
            );
        }, __('Task'));

        listview.page.add_inner_button(__('Archive PB'), function () {
            frappe.prompt([
                {
                    label: __('Enter Login Password to confirm archive'),
                    fieldname: 'password',
                    fieldtype: 'Password',
                    reqd: 1
                }
            ], (values) => {
                frappe.call({
                    method: 'credlawn.credlawn.doctype.call_log.archive_call_logs.execute_archive',
                    args: {
                        password: values.password
                    },
                    callback: function (r) {
                        if (r.message) {
                            frappe.show_alert({
                                message: r.message,
                                indicator: 'green'
                            });
                        }
                    }
                });
            }, __('Sensitive Action: Permanently Delete Records from Pocketbase'), __('Confirm'));
        }, __('Task'));

        listview.page.add_inner_button(__('Truncate'), function () {
            frappe.prompt([
                {
                    label: __('Enter Login Password to confirm truncation'),
                    fieldname: 'password',
                    fieldtype: 'Password',
                    reqd: 1
                }
            ], (values) => {
                frappe.call({
                    method: 'credlawn.credlawn.doctype.call_log.truncate_call_logs.truncate_call_logs',
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
            }, __('Sensitive Action: Delete All LOCAL Call Log Records'), __('Confirm'));
        }, __('Task'));
    }
};
