frappe.ui.form.on("Import Dump", {
	refresh(frm) {
		// Real-time progress listener
		frappe.realtime.on('adobe_import_progress', (data) => {
			if (data.failed) {
				frappe.show_progress(__('Import Error'), 100, 100, data.message);
				setTimeout(() => frappe.hide_progress(), 5000);
			} else {
				frappe.show_progress(__('Importing Adobe Dump'), data.percentage, 100, data.message);
				if (data.percentage >= 100) {
					setTimeout(() => { frappe.hide_progress(); frm.reload_doc(); }, 5000);
				}
			}
		});

		frm.add_custom_button(__('Import Dump'), () => {
			if (!frm.doc.attach_dump) return frappe.msgprint(__('Please attach an Excel file first.'));
			
			if (frm.doc.dump_type === 'Adobe') {
				run_adobe_import_flow(frm);
			} else if (frm.doc.dump_type === 'DSA') {
				frappe.msgprint(__('DSA Import logic is pending implementation.'));
			} else {
				frappe.msgprint(__('Invalid Dump Type selected.'));
			}
		});
	}
});

function run_adobe_import_flow(frm) {
	// Starting validation with show_progress for consistency
	frappe.show_progress(__('Validating Excel'), 0, 100, __('Counting records and checking fields...'));

	frappe.call({
		method: 'credlawn.credlawn.doctype.import_dump.import_adobe_dump.validate_import_file',
		callback: (r) => {
			frappe.hide_progress();
			
			if (r.message) {
				let res = r.message;
				let msg = `<b>Total records discovered:</b> ${res.total_rows}<br><br>`;
				
				if (res.missing_headers && res.missing_headers.length > 0) {
					msg += `<span style="color: red;"><b>Notice:</b> Found ${res.missing_headers.length} missing fields in Excel.</span><br>`;
					msg += `<small>${res.missing_headers.join(', ')}</small><br><br>`;
				} else {
					msg += `All fields mapped successfully.<br><br>`;
				}
				
				msg += `Do you want to proceed with the import?`;

				frappe.confirm(msg, () => {
					// User confirmed: Show progress bar immediately and then start background job
					frappe.show_progress(__('Importing Adobe Dump'), 1, 100, __('Initializing import...'));

					frappe.call({
						method: 'credlawn.credlawn.doctype.import_dump.import_adobe_dump.run_import_sample',
						callback: function (r) {
							if (r.message && r.message.status === 'success') {
								// Background job is now enqueued
							}
						}
					});
				});
			}
		},
		error: () => d.hide()
	});
}
