// Copyright (c) 2026, Credlawn India and contributors
// For license information, please see license.txt

frappe.ui.form.on('IPA Records', {
    refresh: function(frm) {
        // Only show retry button if there was a previous sync error
        if (frm.doc.update_error) {
            frm.add_custom_button(__('Retry PB Update'), function() {
                frappe.call({
                    method: 'credlawn.credlawn.doctype.ipa_records.ipa_records.push_arn_update_to_pb',
                    args: {
                        doc_name: frm.doc.name,
                        pb_id: frm.doc.pb_id,
                        new_arn: frm.doc.arn_no
                    },
                    callback: function(r) {
                        frappe.show_alert({ 
                            message: __('Manual retry initiated. Please wait 5-10 seconds.'), 
                            indicator: 'blue' 
                        });
                        
                        // Small delay to allow background worker to finish before refreshing UI
                        setTimeout(() => {
                            frm.reload_doc();
                        }, 5000);
                    }
                });
            }).addClass('btn-danger'); // Red highlighting to indicate critical action
        }
    }
});
