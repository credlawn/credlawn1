frappe.listview_settings['BPA Records'] = {
    onload: function (listview) {
        // Add a Truncate button to the list view for bulk deletion with security verification
        listview.page.add_inner_button(__('Map Employee'), function () {
            frappe.call({
                method: 'credlawn.credlawn.doctype.bpa_records.map_employee.map_employee',
                callback: function (r) {
                    if (r.message) {
                        frappe.msgprint(r.message);
                        listview.refresh();
                    }
                }
            });
        });

        listview.page.add_inner_button(__('Calculate Payout'), function () {
            frappe.call({
                method: 'credlawn.credlawn.doctype.bpa_records.calculate_payout.get_decision_months',
                callback: function(r) {
                    let months = r.message || [];
                    frappe.prompt([
                        {
                            label: __('Select Decision Month'),
                            fieldname: 'decision_month',
                            fieldtype: 'Select',
                            options: months,
                            reqd: 1
                        }
                    ], (values) => {
                        frappe.call({
                            method: 'credlawn.credlawn.doctype.bpa_records.calculate_payout.execute_calculation',
                            args: {
                                decision_month: values.decision_month
                            },
                            callback: function (resp) {
                                if (resp.message) {
                                    frappe.msgprint(resp.message);
                                    listview.refresh();
                                }
                            }
                        });
                    }, __('Calculate Payout'), __('Calculate'));
                }
            });
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
                    method: 'credlawn.credlawn.doctype.bpa_records.truncate_bpa_records.truncate_bpa_records',
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
            }, __('Sensitive Action: Truncate BPA Records'), __('Confirm'));
        }, __('Task'));
    }
};
