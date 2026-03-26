frappe.ui.form.on("Import Dump", {
	refresh(frm) {
		// Set progress bar color to blue for brand consistency
		frappe.dom.set_style(`
			.progress-bar, .frappe-progress .progress-bar, .progress-bar-inner { 
				background-color: #2490ef !important; 
				background: #2490ef !important;
			}
		`);

		// Adobe Real-time progress listener
		frappe.realtime.on('adobe_import_progress', (data) => {
			handle_import_progress(frm, data, __('Importing Adobe Dump'));
		});

		// DSA Real-time progress listener
		frappe.realtime.on('dsa_import_progress', (data) => {
			handle_import_progress(frm, data, __('Importing DSA Dump'));
		});

		frm.add_custom_button(__('Import Dump'), () => {
			if (!frm.doc.attach_dump) return frappe.msgprint(__('Please attach an Excel file first.'));

			if (frm.doc.dump_type === 'Adobe') {
				run_import_flow(frm, 'adobe_dump');
			} else if (frm.doc.dump_type === 'DSA') {
				run_import_flow(frm, 'dsa_dump');
			} else {
				frappe.msgprint(__('Invalid Dump Type selected.'));
			}
		});

		frm.add_custom_button(__('Clear'), () => {
			frappe.confirm(__('Are you sure you want to clear the form and delete the attached file?'), () => {
				frappe.call({
					method: 'credlawn.credlawn.doctype.import_dump.import_adobe_dump.clear_import_fields',
					callback: (r) => {
						if (r.message && r.message.status === 'success') {
							frappe.show_alert({ message: r.message.message, indicator: 'green' });
							frm.reload_doc();
						}
					}
				});
			});
		});
	}
});

/**
 * Handles real-time progress updates for both Adobe and DSA imports
 */
function handle_import_progress(frm, data, title) {
	if (data.failed) {
		frappe.show_progress(__('Import Error'), 100, 100, data.message);
		setTimeout(() => frappe.hide_progress(), 5000);
	} else {
		frappe.show_progress(title, data.percentage, 100, data.message);
		if (data.percentage >= 100) {
			setTimeout(() => { 
				frappe.hide_progress(); 
				frm.reload_doc(); 
			}, 5000);
		}
	}
}

/**
 * General function to handle the import flow for different dump types
 */
function run_import_flow(frm, module_name) {
	const python_base = `credlawn.credlawn.doctype.import_dump.import_${module_name}`;
	const label_title = module_name.includes('adobe') ? __('Adobe Dump') : __('DSA Dump');

	frappe.show_progress(__('Validating Excel'), 0, 100, __('Checking fields and filtration rules...'));

	frappe.call({
		method: `${python_base}.validate_import_file`,
		callback: (r) => {
			frappe.hide_progress();

			if (r.message) {
				let res = r.message;
				let msg = `<b>Eligible records discovered:</b> ${res.total_rows}<br><br>`;

				if (res.missing_headers && res.missing_headers.length > 0) {
					msg += `<span style="color: red;"><b>Notice:</b> Found ${res.missing_headers.length} missing fields in Excel.</span><br>`;
					msg += `<small>${res.missing_headers.join(', ')}</small><br><br>`;
				} else {
					msg += `All fields mapped successfully.<br><br>`;
				}

				msg += `Do you want to proceed with the ${label_title} import?`;

				frappe.confirm(msg, () => {
					// User confirmed: Show progress immediately
					frappe.show_progress(__('Importing ' + label_title), 1, 100, __('Initializing background job...'));

					frappe.call({
						method: `${python_base}.run_import_sample`,
						callback: function (r) {
							if (r.message && r.message.status === 'success') {
								// Background job is now enqueued
							}
						}
					});
				});
			}
		}
	});
}
