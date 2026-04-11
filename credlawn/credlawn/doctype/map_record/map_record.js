frappe.ui.form.on('Map Record', {
    refresh: function(frm) {
        // Step 1: Filter Target DocType
        frm.set_query('target_doctype', function() {
            return { filters: { module: 'Credlawn' } };
        });

        // Step 2: Set Initial Visibility (Pro Wizard Feel)
        toggle_pro_visibility(frm);

        // Load options if data already exists
        if (frm.doc.source_file) update_excel_headers(frm);
        if (frm.doc.target_doctype) update_doctype_fields(frm);

        // Action Button
        frm.add_custom_button(__('Run Mapping Engine'), function() {
            // ... (keep existing validation and call)
            if (!frm.doc.source_file) {
                frappe.msgprint(__('Please attach a Source file.'));
                return;
            }
            if (!frm.doc.target_doctype || !frm.doc.excel_lookup_column || !frm.doc.doctype_lookup_field) {
                frappe.msgprint(__('Please select Target DocType, Excel Column and Lookup Field.'));
                return;
            }

            frappe.call({
                method: 'credlawn.credlawn.doctype.map_record.map_record.start_mapping',
                args: {
                    target_dt: frm.doc.target_doctype,
                    excel_col: frm.doc.excel_lookup_column,
                    dt_field: frm.doc.doctype_lookup_field,
                    extract_fields: JSON.stringify(frm.doc.fields_to_extract ? frm.doc.fields_to_extract.split(',').map(f => f.trim()).filter(f => f !== "") : [])
                },
                callback: function(r) {
                    if (r.message) frappe.show_alert({ message: r.message, indicator: 'blue' });
                }
            });
        }).addClass('btn-primary');

        // Help select fields easily
        frm.set_df_property('fields_to_extract', 'description', 
            '<button class="btn btn-xs btn-default" id="btn-pick-fields" style="margin-top: 5px;">Pick Fields from List</button>');

        $(frm.wrapper).off('click', '#btn-pick-fields').on('click', '#btn-pick-fields', () => {
            if (!frm.doc.target_doctype) {
                frappe.msgprint(__('Please select Target DocType first.'));
                return;
            }
            open_field_picker(frm);
        });

        // Download link
        if (frm.doc.mapped_file) {
            frm.add_custom_button(__('Download Result'), function() {
                window.open(frm.doc.mapped_file);
            }, __('Actions'));
        }

        // Cleanup Button
        frm.add_custom_button(__('Clear All Data'), function() {
            frappe.confirm(
                __('Are you sure you want to CLEAR all fields and PERMANENTLY DELETE attached files? This cannot be undone.'),
                function() {
                    frappe.call({
                        method: 'credlawn.credlawn.doctype.map_record.map_record.clear_mapping_data',
                        callback: function(r) {
                            if (r.message) {
                                frappe.show_alert({ message: r.message, indicator: 'green' });
                                frm.reload_doc();
                            }
                        }
                    });
                }
            );
        }, __('Actions'));
    },

    source_file: function(frm) {
        if (frm.doc.source_file) {
            update_excel_headers(frm);
        } else {
            frm.set_df_property('excel_lookup_column', 'options', []);
        }
        toggle_pro_visibility(frm);
    },

    target_doctype: function(frm) {
        if (frm.doc.target_doctype) {
            update_doctype_fields(frm);
        } else {
            frm.set_df_property('doctype_lookup_field', 'options', []);
        }
        toggle_pro_visibility(frm);
    }
});

/**
 * Professional Visibility Control (Wizard-style)
 */
function toggle_pro_visibility(frm) {
    // Show/Hide Section
    let has_initial_setup = frm.doc.source_file || frm.doc.target_doctype;
    frm.toggle_display('section_break_config', has_initial_setup);

    // Show excel lookup only if file exists
    frm.toggle_display('excel_lookup_column', frm.doc.source_file);

    // Show doctype fields only if DT is selected
    frm.toggle_display('doctype_lookup_field', frm.doc.target_doctype);
    frm.toggle_display('fields_to_extract', frm.doc.target_doctype);
}

/**
 * Fetches headers from the attached Excel and updates the Select dropdown.
 */
function update_excel_headers(frm) {
    frappe.call({
        method: 'credlawn.credlawn.doctype.map_record.map_record.get_excel_headers',
        callback: function(r) {
            let headers = r.message || [];
            frm.set_df_property('excel_lookup_column', 'options', headers);
            console.log("Excel Headers loaded:", headers.length);
        }
    });
}

/**
 * Fetches fields from selected DocType and updates the Select dropdown.
 */
function update_doctype_fields(frm) {
    frappe.model.with_doctype(frm.doc.target_doctype, () => {
        let fields = frappe.get_meta(frm.doc.target_doctype).fields
            .filter(df => !frappe.model.no_value_type.includes(df.fieldtype))
            .map(df => df.fieldname);
        
        fields.unshift('name');
        fields = [...new Set(fields)].sort();

        frm.set_df_property('doctype_lookup_field', 'options', fields);
        console.log("DocType Fields loaded:", fields.length);
    });
}

/**
 * Opens a Multi-Select Dialog to pick fields for extraction.
 */
function open_field_picker(frm) {
    frappe.model.with_doctype(frm.doc.target_doctype, () => {
        let fields = frappe.get_meta(frm.doc.target_doctype).fields
            .filter(df => !frappe.model.no_value_type.includes(df.fieldtype))
            .map(df => ({ label: df.label || df.fieldname, value: df.fieldname }));
        
        fields.unshift({ label: 'ID', value: 'name' });
        fields.sort((a, b) => a.label.localeCompare(b.label));

        let current_values = frm.doc.fields_to_extract ? frm.doc.fields_to_extract.split(',').map(v => v.trim()) : [];

        let d = new frappe.ui.Dialog({
            title: __('Select Fields to Extract'),
            fields: [
                {
                    label: __('Choose Fields'),
                    fieldname: 'selected_fields',
                    fieldtype: 'MultiSelect',
                    options: fields,
                    default: current_values.join(', ')
                }
            ],
            primary_action_label: __('Apply'),
            primary_action(values) {
                if (values.selected_fields) {
                    // Clean all spaces to ensure a clean comma-separated list
                    let clean_val = values.selected_fields.split(',')
                        .map(v => v.trim())
                        .filter(v => v !== "")
                        .join(',');
                    
                    frm.set_value('fields_to_extract', clean_val);
                }
                d.hide();
            }
        });

        d.show();
    });
}

// Global listener for background completion
frappe.realtime.on('map_record_progress', (data) => {
    if (data.status === 'success') {
        frappe.show_alert({ message: __('Mapping Engine finished successfully!'), indicator: 'green' });
        if (cur_frm && cur_frm.doctype === 'Map Record') {
            cur_frm.reload_doc();
        }
    } else if (data.status === 'failed') {
        frappe.msgprint({ title: __('Mapping Failed'), indicator: 'red', message: data.message });
    }
});
