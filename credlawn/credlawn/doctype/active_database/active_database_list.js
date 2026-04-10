frappe.listview_settings['Active Database'] = {
    hide_name_column: true,
    hide_name_filter: true,

    onload: function (listview) {
        // Real-time synchronization events (Progress Bar)
        frappe.realtime.on('active_database_sync_progress', (data) => {
            if (data.failed) {
                frappe.show_progress(__('Database Sync Failed'), 100, 100, data.message);
                setTimeout(() => frappe.hide_progress(), 5000);
            } else {
                frappe.show_progress(__('Syncing Active Database'), data.percentage, 100, data.message);
                if (data.percentage >= 100) {
                    setTimeout(() => { 
                        frappe.hide_progress(); 
                        listview.refresh(); 
                    }, 3000);
                }
            }
        });

        // Real-time transfer events
        frappe.realtime.on('active_database_transfer_progress', (data) => {
            if (data.failed) {
                frappe.show_progress(__('Transfer Failed'), 100, 100, data.message);
                setTimeout(() => frappe.hide_progress(), 5000);
            } else {
                frappe.show_progress(__('Transferring to Master'), data.percentage, 100, data.message);
                if (data.percentage >= 100) {
                    setTimeout(() => { 
                        frappe.hide_progress(); 
                        listview.refresh(); 
                    }, 3000);
                }
            }
        });

        // Sync Database Button
        listview.page.add_inner_button(__('Sync Database'), function () {
            frappe.confirm(
                __('Are you sure you want to sync Active Database from Pocketbase? This will update existing records and add new ones.'),
                function () {
                    frappe.call({
                        method: 'credlawn.credlawn.doctype.active_database.sync_active_database.execute_sync',
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

        // Task Menu Buttons
        listview.page.add_inner_button(__('Mark to Archive'), function () {
            frappe.confirm(
                __('Are you sure you want to mark records for archiving based on retention logic?'),
                function () {
                    frappe.call({
                        method: 'credlawn.credlawn.doctype.active_database.mark_active_database_to_archive.execute_mark',
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
                    label: __('Enter Login Password to confirm'),
                    fieldname: 'password',
                    fieldtype: 'Password',
                    reqd: 1
                }
            ], (values) => {
                frappe.call({
                    method: 'credlawn.credlawn.doctype.active_database.archive_active_database.execute_archive',
                    args: { password: values.password },
                    callback: function (r) {
                        if (r.message) {
                            frappe.show_alert({ message: r.message, indicator: 'green' });
                        }
                    }
                });
            }, __('Sensitive Action: Silent Archive marked records in Pocketbase'), __('Confirm'));
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
                    method: 'credlawn.credlawn.doctype.active_database.truncate_active_database.execute_truncate',
                    args: { password: values.password },
                    callback: function (r) {
                        if (r.message) {
                            frappe.show_alert({ message: r.message, indicator: 'green' });
                            listview.refresh();
                        }
                    }
                });
            }, __('Sensitive Action: Delete All LOCAL Database Records'), __('Confirm'));
        }, __('Task'));

        listview.page.add_inner_button(__('Transfer to Master'), function () {
            frappe.confirm(
                __('Are you sure you want to transfer marked records to Master Database? Records will be DELETED from here after transfer.'),
                function () {
                    frappe.call({
                        method: 'credlawn.credlawn.doctype.active_database.transfer_to_master.execute_transfer',
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
        }, __('Task'));
    }
};
