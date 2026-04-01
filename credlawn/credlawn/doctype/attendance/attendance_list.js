frappe.listview_settings['Attendance'] = {
    hide_name_column: true,
    hide_name_filter: true,

    onload: function (listview) {
        // Listen for real-time sync progress
        frappe.realtime.on('attendance_sync_progress', (data) => {
            if (data.failed) {
                frappe.show_progress(__('Sync Failed'), 100, 100, data.message);
                setTimeout(() => frappe.hide_progress(), 5000);
                return;
            }

            frappe.show_progress(__('Syncing Attendance'), data.percentage, 100, data.message);
            
            if (data.percentage >= 100) {
                setTimeout(() => {
                    frappe.hide_progress();
                    listview.refresh();
                }, 3000);
            }
        });

        // Listen for real-time archive progress
        frappe.realtime.on('attendance_archive_progress', (data) => {
            if (data.failed) {
                frappe.show_progress(__('Archive Failed'), 100, 100, data.message);
                setTimeout(() => frappe.hide_progress(), 5000);
                return;
            }

            frappe.show_progress(__('Archiving Attendance'), data.percentage, 100, data.message);
            
            if (data.percentage >= 100) {
                setTimeout(() => {
                    frappe.hide_progress();
                    listview.refresh();
                }, 3000);
            }
        });

        // Sync Atn Button with Confirmation
        listview.page.add_inner_button(__('Sync Atn'), function () {
            frappe.confirm(
                __('Are you sure you want to sync Attendance from Pocketbase? This will run in the background.'),
                function () {
                    frappe.call({
                        method: 'credlawn.credlawn.doctype.attendance.sync_atn.execute_sync',
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

        // Archive PB Records Button
        listview.page.add_inner_button(__('Archive PB'), function () {
            frappe.prompt([
                {
                    label: __('Enter your Login Password to confirm'),
                    fieldname: 'password',
                    fieldtype: 'Password',
                    reqd: 1
                }
            ], (values) => {
                frappe.call({
                    method: 'credlawn.credlawn.doctype.attendance.archive_atn.execute_archive',
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
            }, __('Sensitive Action: Archive All records in Pocketbase (Only records with "Clean From PB" checked will be archived)'), __('Confirm'));
        }, __('Task'));

        // Truncate All Attendance Records Button (Frappe Only)
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
                    method: 'credlawn.credlawn.doctype.attendance.truncate_attendance.truncate_attendance',
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
            }, __('Sensitive Action: Truncate All Attendance Records'), __('Confirm'));
        }, __('Task'));
    }
};
