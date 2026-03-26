frappe.listview_settings['DSA Dump'] = {
    onload: function (listview) {
        // Add a Truncate button to the list view for bulk deletion with security verification
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
                    method: 'credlawn.credlawn.doctype.dsa_dump.truncate_dsa_dump.truncate_dsa_dump',
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
            }, __('Sensitive Action: Truncate DSA Dump'), __('Confirm'));
        }, __('Task'));
    }
};
