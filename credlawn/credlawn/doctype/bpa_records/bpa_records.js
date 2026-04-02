frappe.ui.form.on("BPA Records", {
    refresh(frm) {
        // Add Button: 'Calculate Payout'
        frm.add_custom_button(__("Calculate Payout"), () => {
            frappe.call({
                method: "credlawn.credlawn.doctype.bpa_records.calculate_payout.execute_calculation",
                args: {
                    docname: frm.doc.name
                },
                callback: (r) => {
                    if (r.message) {
                        frappe.msgprint(r.message);
                    }
                }
            });
        });
    },
});
