frappe.listview_settings['Adobe Dump'] = {
    hide_name_column: true,
    hide_name_filter: true,

    onload: function (listview) {
        // Real-time synchronization events (Progress Bar)
        frappe.realtime.on('adobe_dump_push_progress', (data) => {
            if (data.failed) {
                frappe.show_progress(__('Push Failed'), 100, 100, data.message);
                setTimeout(() => frappe.hide_progress(), 5000);
            } else {
                frappe.show_progress(__('Pushing Data to Pocketbase'), data.percentage, 100, data.message);
                if (data.percentage >= 100) {
                    setTimeout(() => { 
                        frappe.hide_progress(); 
                        listview.refresh(); 
                    }, 3000);
                }
            }
        });

        // Push Data Button
        listview.page.add_inner_button(__('Push Data'), function () {
            frappe.confirm(
                __('Are you sure you want to push data to Pocketbase? This will create/update VKYC records sequentially.'),
                function () {
                    frappe.call({
                        method: 'credlawn.credlawn.doctype.adobe_dump.push_adobe_dump_data.execute_push',
                        callback: function (r) {
                            if (r.message && r.message.status === 'success') {
                                frappe.show_alert({
                                    message: r.message.message,
                                    indicator: 'green'
                                });
                            }
                        }
                    });
                }
            );
        });

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
                    method: 'credlawn.credlawn.doctype.adobe_dump.truncate_adobe_dump_records.truncate_adobe_dump_records',
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

