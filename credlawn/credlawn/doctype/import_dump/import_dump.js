frappe.ui.form.on("Import Dump", {
	refresh(frm) {
		// Professional CSS for Progress Bar and Modal refinement
		frappe.dom.set_style(`
			.progress-bar { background-color: #2490ef !important; transition: width 0.4s ease; }
			.frappe-progress .progress-title { font-weight: 600; color: #1f2937; margin-bottom: 8px; }
			.import-summary-modal .modal-body { padding: 10px 20px !important; }
		`);

		// Real-time progress listeners
		setup_realtime_listeners(frm);

		// 1. Unified Import Button
		frm.add_custom_button(__('Import Dump'), () => {
			if (!frm.doc.attach_dump) {
				return frappe.msgprint({
					title: __('Selection Required'),
					message: __('Please attach an Excel file first.'),
					indicator: 'orange'
				});
			}

			const module_map = {
				'Adobe': 'adobe_dump',
				'DSA': 'dsa_dump',
				'Payout': 'payout_dump'
			};

			const module_name = module_map[frm.doc.dump_type];
			if (module_name) {
				start_professional_import_flow(frm, module_name);
			} else if (frm.doc.dump_type === 'Deduction') {
				frappe.msgprint(__('Deduction Import is coming in Phase 2. Please use Payout for now.'));
			} else {
				frappe.msgprint(__('Invalid Dump Type selected.'));
			}
		});

		// 2. Form Cleanup Button
		frm.add_custom_button(__('Clear'), () => {
			frappe.confirm(__('Are you sure you want to clear all fields and the attachment?'), () => {
				frappe.call({
					method: 'credlawn.credlawn.doctype.import_dump.import_adobe_dump.clear_import_fields',
					callback: (r) => {
						if (r.message?.status === 'success') {
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
 * Setup real-time listeners for all import types
 */
function setup_realtime_listeners(frm) {
	const events = ['adobe_import_progress', 'dsa_import_progress', 'payout_import_progress'];
	events.forEach(event => {
		frappe.realtime.off(event); // Cleanup previous listeners
		frappe.realtime.on(event, (data) => {
			if (data.failed) {
				frappe.show_progress(__('Import Failed'), 100, 100, data.message);
				setTimeout(() => frappe.hide_progress(), 5000);
			} else {
				// Re-calculating the title consistently
				const type = event.split('_')[0].toUpperCase();
				const title = type + ' ' + __('Import');
				
				frappe.show_progress(title, data.percentage, 100, data.message);
				
				if (data.percentage >= 100) {
					setTimeout(() => { 
						frappe.hide_progress(); 
						frm.reload_doc();
						frappe.show_alert({ message: __('Import Completed Successfully'), indicator: 'green' });
					}, 2000);
				}
			}
		});
	});
}

/**
 * Main Flow: Validation -> Summary Dialog -> Execution
 */
function start_professional_import_flow(frm, module_name) {
	const python_base = `credlawn.credlawn.doctype.import_dump.import_${module_name}`;
	
	// Better feedback during server call
	frappe.dom.freeze(__('Validating Dataset...'));

	frappe.call({
		method: `${python_base}.validate_import_file`,
		callback: (r) => {
			if (r.message) {
				const res = r.message;
				let status_html = `
					<div style="padding: 10px;">
						<p><b>Records discovered in Excel:</b> <span class="text-primary" style="font-size: 1.1em; font-weight: bold;">${res.total_rows}</span></p>
				`;

				if (res.missing_headers && res.missing_headers.length > 0) {
					status_html += `
						<div class="alert alert-warning" style="margin-top: 15px; border-left: 4px solid #fbbd08;">
							<p><i class="fa fa-info-circle"></i> <b>Optional Fields Missing:</b></p>
							<small style="color: #6b7280;">${res.missing_headers.join(', ')}</small>
						</div>
					`;
				} else {
					status_html += `<p class="text-success" style="margin-top: 10px;"><i class="fa fa-check-circle"></i> All required headers verified.</p>`;
				}
				
				status_html += `</div>`;

				// Step 2: Confirmation Dialog
				const d = new frappe.ui.Dialog({
					title: __('Ready to Import: {0}', [frm.doc.dump_type]),
					fields: [
						{ fieldtype: 'HTML', fieldname: 'summary', options: status_html }
					],
					primary_action_label: __('Confirm & Start Import'),
					primary_action() {
						d.hide();
						execute_background_import(frm, module_name, python_base);
					}
				});
				d.show();
			} else {
				frappe.msgprint(__('Validation failed. Please check the Excel format.'));
			}
		},
		always: () => {
			frappe.dom.unfreeze();
		}
	});
}

/**
 * Step 3: Trigger background job
 * Improvement: Instant progress bar matching realtime title.
 */
function execute_background_import(frm, module_name, python_base) {
	// Construct title consistently (e.g., ADOBE Import / DSA Import / PAYOUT Import)
	const type_key = module_name.split('_')[0].toUpperCase();
	const title = type_key + ' ' + __('Import');

	// ACTION: Instant visual feedback with the EXACT same progress bar title
	frappe.show_progress(title, 2, 100, __('Initializing background job...'));

	frappe.call({
		method: `${python_base}.run_import_sample`,
		callback: (r) => {
			if (r.message && r.message.status === 'success') {
				// The realtime listener will now update THIS SAME progress bar
				frappe.show_alert({ message: __('Job enqueued.'), indicator: 'blue' });
			}
		}
	});
}
