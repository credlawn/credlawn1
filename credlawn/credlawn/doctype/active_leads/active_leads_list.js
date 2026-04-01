frappe.listview_settings['Active Leads'] = {
    hide_name_column: true,
    hide_name_filter: true,

    onload: function (listview) {
        // Real-time synchronization events (Progress Bar)
        frappe.realtime.on('active_leads_sync_progress', (data) => {
            if (data.failed) {
                frappe.show_progress(__('Leads Sync Failed'), 100, 100, data.message);
                setTimeout(() => frappe.hide_progress(), 5000);
            } else {
                frappe.show_progress(__('Syncing Leads'), data.percentage, 100, data.message);
                if (data.percentage >= 100) {
                    setTimeout(() => { frappe.hide_progress(); listview.refresh(); }, 3000);
                }
            }
        });

        // Sync Leads Button (Main Area)
        listview.page.add_inner_button(__('Sync Leads'), function () {
            frappe.confirm(
                __('Are you sure you want to sync Active Leads from Pocketbase? This will run in the background.'),
                function () {
                    frappe.call({
                        method: 'credlawn.credlawn.doctype.active_leads.sync_active_leads.execute_sync',
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
                __('Are you sure you want to mark older Active Leads for archiving? This process will accurately flag leads that have crossed their retention period.'),
                function () {
                    frappe.call({
                        method: 'credlawn.credlawn.doctype.active_leads.mark_active_leads_to_archive.execute_mark',
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
                    method: 'credlawn.credlawn.doctype.active_leads.archive_active_leads.execute_archive',
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
            }, __('Sensitive Action: Silent Archive marked Leads in Pocketbase'), __('Confirm'));
        }, __('Task'));

        listview.page.add_inner_button(__('Truncate'), function () {
            frappe.prompt([
                {
                    label: __('Enter password to confirm truncation'),
                    fieldname: 'password',
                    fieldtype: 'Password',
                    reqd: 1
                }
            ], (values) => {
                frappe.call({
                    method: 'credlawn.credlawn.doctype.active_leads.truncate_active_leads.truncate_active_leads',
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
            }, __('Sensitive Action: Delete All LOCAL Leads Records'), __('Confirm'));
        }, __('Task'));
    }
};
