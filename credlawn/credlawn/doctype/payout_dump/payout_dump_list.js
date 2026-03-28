frappe.listview_settings['Payout Dump'] = {
	onload: function(listview) {
		// 1. Download Sample Button
		listview.page.add_inner_button(__('Download Sample'), function() {
			window.open(frappe.urllib.get_full_url(
				'/api/method/credlawn.credlawn.doctype.payout_dump.download_sample.download_sample_payout_excel'
			));
		}, __('Task'));

		// 2. Truncate All Records Button (Secure)
		listview.page.add_inner_button(__('Truncate'), function() {
			frappe.prompt([
				{
					label: __('Enter your Login Password to confirm'),
					fieldname: 'password',
					fieldtype: 'Password',
					reqd: 1
				}
			], (values) => {
				frappe.call({
					method: 'credlawn.credlawn.doctype.payout_dump.truncate_payout_dump.truncate_payout_dump',
					args: {
						password: values.password
					},
					callback: function(r) {
						if (r.message) {
							frappe.show_alert({
								message: r.message,
								indicator: 'green'
							});
							listview.refresh();
						}
					}
				});
			}, __('Sensitive Action: Truncate All Payout Dump Records'), __('Confirm'));
		}, __('Task'));
	}
};
