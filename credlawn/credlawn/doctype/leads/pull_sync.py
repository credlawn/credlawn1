import frappe
from frappe import _
import requests
from datetime import datetime
import pytz


FIELD_MAPPING = {
    'employee_name': 'employee_name',
    'employee_code': 'employee_code',
    'customer_name': 'customer_name',
    'mobile_no': 'mobile_no',
    'city': 'city',
    'remarks': 'remarks',
    'lead_status': 'status',
    'segment': 'segment',
    'employer': 'employer',
    'decline_reason': 'decline_reason',
    'product': 'product',
    'data_code': 'data_code',
    'data_sub_code': 'data_sub_code',
    'custom_code': 'custom_code',
    'arn_no': 'arn_no',
    'total_calls': 'total_calls',
    'connected_calls': 'connected_calls',
    'total_duration': 'total_duration',
}

DATETIME_FIELDS = {
    'assigned_date': 'assigned_date',
    'followup_time': 'follow_up_time',
}

DATE_FIELDS = {
    'date_of_birth': 'date_of_birth',
}

BATCH_SIZE = 500
PER_PAGE = 500


@frappe.whitelist()
def execute_pull_sync():
    """Main entry point for Pull Sync - Enqueues background job"""
    try:
        frappe.enqueue(
            method='credlawn.credlawn.doctype.leads.pull_sync.process_pull_sync_job',
            queue='default',
            timeout=7200,
            is_async=True,
            job_name='leads_pull_sync'
        )
        
        return _("Pull Sync job started successfully! Records will be synced in background.")
        
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), _("Pull Sync Enqueue Error"))
        frappe.throw(_("Failed to start Pull Sync: {0}").format(str(e)))


def process_pull_sync_job():
    """Background job worker - Processes the actual sync"""
    try:
        pb_url = frappe.conf.get("pb_url")
        pb_token = frappe.conf.get("pb_token")
        
        if not pb_url or not pb_token:
            frappe.throw(_("Pocketbase credentials not found in site config"))
        
        total_fetched = 0
        created_count = 0
        updated_count = 0
        failed_count = 0
        failed_records = []
        
        page = 1
        
        while True:
            records = fetch_pocketbase_records(pb_url, pb_token, page=page, per_page=PER_PAGE)
            
            if not records:
                break
            
            total_fetched += len(records)
            
            batch_created, batch_updated, batch_failed = process_batch(records, failed_records)
            created_count += batch_created
            updated_count += batch_updated
            failed_count += batch_failed
            
            frappe.db.commit()
            
            if len(records) < PER_PAGE:
                break
            
            page += 1
        
        frappe.db.commit()
        
        message = (
            f"Pull Sync Completed!\n"
            f"Total Fetched: {total_fetched}\n"
            f"Created: {created_count}\n"
            f"Updated: {updated_count}\n"
            f"Failed: {failed_count}"
        )
        
        if failed_records:
            frappe.log_error(
                frappe.as_json(failed_records, indent=2),
                _("Pull Sync Failed Records")
            )
        
        frappe.logger().info(message)
        return message
        
    except Exception as e:
        frappe.db.rollback()
        frappe.log_error(frappe.get_traceback(), _("Pull Sync Job Error"))
        raise


def process_batch(records, failed_records):
    """Process a batch of records with bulk operations"""
    created_count = 0
    updated_count = 0
    failed_count = 0
    
    mobile_nos = [r.get('mobile_no') for r in records if r.get('mobile_no')]
    pb_ids = [r.get('id') for r in records if r.get('id')]
    
    existing_by_pb_id = {}
    existing_by_mobile = {}
    
    if pb_ids:
        existing_pb = frappe.get_all('Leads', filters={'pb_id': ['in', pb_ids]}, fields=['name', 'pb_id'])
        existing_by_pb_id = {lead['pb_id']: lead['name'] for lead in existing_pb}
    
    if mobile_nos:
        existing_mobile = frappe.get_all('Leads', filters={'mobile_no': ['in', mobile_nos]}, fields=['name', 'mobile_no'])
        existing_by_mobile = {lead['mobile_no']: lead['name'] for lead in existing_mobile}
    
    pb_url = frappe.conf.get("pb_url")
    pb_token = frappe.conf.get("pb_token")
    database_codes = fetch_database_codes(pb_url, pb_token, mobile_nos)
    
    for record in records:
        try:
            pb_id = record.get('id')
            mobile_no = record.get('mobile_no')
            
            if not pb_id or not mobile_no:
                failed_count += 1
                failed_records.append({
                    'pb_id': pb_id,
                    'mobile_no': mobile_no,
                    'error': 'Missing pb_id or mobile_no'
                })
                continue
            
            existing_lead = existing_by_pb_id.get(pb_id) or existing_by_mobile.get(mobile_no)
            
            if existing_lead:
                lead = frappe.get_doc('Leads', existing_lead)
                update_lead_fields(lead, record, database_codes.get(mobile_no))
                lead.save(ignore_permissions=True)
                updated_count += 1
            else:
                lead = frappe.new_doc('Leads')
                update_lead_fields(lead, record, database_codes.get(mobile_no))
                lead.insert(ignore_permissions=True)
                created_count += 1
                
        except Exception as e:
            failed_count += 1
            failed_records.append({
                'pb_id': record.get('id'),
                'mobile_no': record.get('mobile_no'),
                'error': str(e)
            })
            frappe.log_error(
                f"Failed to sync record: {record.get('id')}\n{frappe.get_traceback()}",
                _("Pull Sync Record Error")
            )
    
    return created_count, updated_count, failed_count


def fetch_pocketbase_records(pb_url, pb_token, page=1, per_page=500):
    """Fetch records from Pocketbase with pagination"""
    try:
        api_url = f"{pb_url}/api/collections/leads/records"
        
        headers = {
            "Authorization": f"Bearer {pb_token}",
            "Content-Type": "application/json"
        }
        
        params = {
            "perPage": per_page,
            "page": page
        }
        
        response = requests.get(api_url, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            return data.get('items', [])
        else:
            frappe.throw(
                _(f"Failed to fetch from Pocketbase. Status: {response.status_code}, Response: {response.text}")
            )
    
    except requests.exceptions.RequestException as e:
        frappe.throw(_("Network error while connecting to Pocketbase: {0}").format(str(e)))
    except Exception as e:
        frappe.throw(_("Error fetching Pocketbase records: {0}").format(str(e)))


CHUNK_SIZE = 50  # Max mobile numbers per API request to avoid URL length limit


def fetch_database_codes(pb_url, pb_token, mobile_nos):
    """Fetch data_code, data_sub_code, custom_code from Pocketbase database collection.
    Splits mobile_nos into chunks to avoid URL length limit (400 error).
    """
    if not mobile_nos:
        return {}
    
    database_map = {}
    
    try:
        api_url = f"{pb_url}/api/collections/database/records"
        
        headers = {
            "Authorization": f"Bearer {pb_token}",
            "Content-Type": "application/json"
        }
        
        # Split into chunks of 50 to avoid URL length limit
        for i in range(0, len(mobile_nos), CHUNK_SIZE):
            chunk = mobile_nos[i:i + CHUNK_SIZE]
            
            filter_conditions = " || ".join([f"mobile_no='{mobile}'" for mobile in chunk])
            
            params = {
                "filter": f"({filter_conditions})",
                "perPage": CHUNK_SIZE
            }
            
            response = requests.get(api_url, headers=headers, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                items = data.get('items', [])
                
                for item in items:
                    mobile = item.get('mobile_no')
                    if mobile:
                        database_map[mobile] = {
                            'data_code': item.get('data_code'),
                            'data_sub_code': item.get('data_sub_code'),
                            'custom_code': item.get('custom_code')
                        }
            else:
                frappe.log_error(
                    f"Failed to fetch database codes. Status: {response.status_code}, Response: {response.text}",
                    _("Database Collection Fetch Error")
                )
        
        return database_map
    
    except Exception as e:
        frappe.log_error(
            f"Error fetching database codes: {str(e)}\n{frappe.get_traceback()}",
            _("Database Collection Error")
        )
        return database_map  # Return whatever was fetched so far


def update_lead_fields(lead_doc, pb_record, database_codes=None):
    """Update lead document fields from Pocketbase record"""
    for pb_field, frappe_field in FIELD_MAPPING.items():
        if pb_field in pb_record:
            value = pb_record.get(pb_field)
            if value is not None and value != '':
                lead_doc.set(frappe_field, value)
    
    if pb_record.get('id'):
        lead_doc.set('pb_id', pb_record.get('id'))
    
    if pb_record.get('created'):
        lead_doc.set('pb_created', convert_to_ist(pb_record.get('created')))
    
    if pb_record.get('updated'):
        lead_doc.set('pb_updated', convert_to_ist(pb_record.get('updated')))
    
    if pb_record.get('lead_status_date'):
        ist_datetime = convert_to_ist(pb_record.get('lead_status_date'))
        if ist_datetime:
            lead_doc.set('lead_status_date', ist_datetime.date())
            lead_doc.set('lead_status_time', ist_datetime.time())
    
    for pb_field, frappe_field in DATETIME_FIELDS.items():
        if pb_field in pb_record and pb_record.get(pb_field):
            ist_datetime = convert_to_ist(pb_record.get(pb_field))
            if ist_datetime:
                lead_doc.set(frappe_field, ist_datetime)
    
    for pb_field, frappe_field in DATE_FIELDS.items():
        if pb_field in pb_record and pb_record.get(pb_field):
            ist_datetime = convert_to_ist(pb_record.get(pb_field))
            if ist_datetime:
                lead_doc.set(frappe_field, ist_datetime.date())
    
    if database_codes:
        if database_codes.get('data_code'):
            lead_doc.set('data_code', database_codes.get('data_code'))
        if database_codes.get('data_sub_code'):
            lead_doc.set('data_sub_code', database_codes.get('data_sub_code'))
        if database_codes.get('custom_code'):
            lead_doc.set('custom_code', database_codes.get('custom_code'))


def convert_to_ist(utc_datetime_str):
    """Convert UTC datetime string to IST datetime"""
    if not utc_datetime_str:
        return None
    
    try:
        utc_datetime_str = str(utc_datetime_str).replace(' ', 'T')
        
        if utc_datetime_str.endswith('Z'):
            utc_datetime_str = utc_datetime_str[:-1]
        
        utc_dt = datetime.fromisoformat(utc_datetime_str)
        
        utc_tz = pytz.UTC
        if utc_dt.tzinfo is None:
            utc_dt = utc_tz.localize(utc_dt)
        
        ist_tz = pytz.timezone('Asia/Kolkata')
        ist_dt = utc_dt.astimezone(ist_tz)
        
        return ist_dt.replace(tzinfo=None)
        
    except Exception as e:
        frappe.log_error(
            f"Error converting datetime: {utc_datetime_str}\n{frappe.get_traceback()}",
            _("DateTime Conversion Error")
        )
        return None



