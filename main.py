import sqlite3
import requests
import json
from datetime import datetime
import os

# Configuration
AIRTABLE_PERSONAL_TOKEN = os.getenv('AIRTABLE_PERSONAL_TOKEN')
BASE_ID = os.getenv('PO_BASE_ID')
TABLE_NAME = os.getenv('PO_TABLE_NAME')
AIRTABLE_URL = f'https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}'
HEADERS = {
    'Authorization': f'Bearer {AIRTABLE_PERSONAL_TOKEN}',
    'Content-Type': 'application/json'
}


# Function to fetch all records from Airtable with pagination
def fetch_all_records(url):
    records = []
    params = {}

    while True:
        response = requests.get(url, headers=HEADERS, params=params)
        response_json = response.json()
        records.extend(response_json.get('records', []))

        # Check for 'offset' in response for pagination
        if 'offset' in response_json:
            params['offset'] = response_json['offset']
        else:
            break

    return records


# Connect to SQLite database
conn = sqlite3.connect('/home/hanklokyaw/materials_requisition_form/app.db')
cursor = conn.cursor()

# Fetch data from SQLite database
cursor.execute("""
SELECT 
po_detail.`id` AS 'id',
po.`po_number` AS 'PO #', 
        po.`date` AS 'Date', 
        requester.`name` AS 'Requester', 
        requester.`dept` AS 'Department', 
        vendor.`id` AS 'Vendor ID', 
        vendor.`name` AS 'Vendor Name', 
        vendor.`city` AS 'Vendor City', 
        vendor.`phone` AS 'Vendor Phone', 
        vendor.`email` AS 'Vendor Email', 
        vendor.`comments` AS 'Vendor Notes',
        item.`item_name` AS 'New SKU',
        item.`alt_sku` AS 'Old SKU',
        item.`purchase_price` AS 'Unit Cost',
        po_detail.`quantity` AS 'Quantity',
        item.`purchase_price` * po_detail.`quantity` AS 'Cost',
        item.`url` AS 'Item Link'
FROM po_detail
LEFT JOIN po
ON po.`id` = po_detail.`po_id`
LEFT JOIN vendor
ON po.`vendor_id` = vendor.`id`
LEFT JOIN requester
ON po.`requester_id` = requester.`id`
LEFT JOIN item
ON po_detail.`item_id` = item.`id`;
""")
rows = cursor.fetchall()

# Column names (adjust according to your table structure)
columns = [description[0] for description in cursor.description]

# Fetch existing records from Airtable
airtable_records = fetch_all_records(AIRTABLE_URL)

# Map Airtable records by SQLite ID
airtable_map = {record['fields'].get('id'): record['id'] for record in airtable_records if 'id' in record['fields']}

# Track the IDs in the database
db_ids = set()

# Sync new data to Airtable
for row in rows:
    record = dict(zip(columns, row))
    db_id = record['id']
    db_ids.add(db_id)

    if db_id not in airtable_map:
        # Create new record
        response = requests.post(AIRTABLE_URL, headers=HEADERS, data=json.dumps({'fields': record}))
        print(f'Added new record to Airtable: {record}')

        if response.status_code not in [200, 201]:
            print(f'Failed to sync record: {record}')
            print(f'Response status code: {response.status_code}')
            print(f'Response: {response.json()}')
    else:
        print(f'Skipping existing record with ID: {db_id}')

print(f'Updated on {datetime.now()}.')

# Close the SQLite connection
conn.close()
