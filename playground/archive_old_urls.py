#!/usr/bin/env python3
"""
Archive old URLs from YSWS Project Mentions table.

Reads record IDs from needs_archiving.txt, fetches URL from each record,
archives it using archive.hackclub.com, and updates the Archive URL field.

Environment variables required:
- AIRTABLE_PERSONAL_ACCESS_TOKEN: Your Airtable personal access token
- ARCHIVE_HACKCLUB_COM_API_KEY: Your archive.hackclub.com API key
"""

import os
import time
import requests
from typing import Optional
from dotenv import load_dotenv
from pyairtable import Api


def archive_url(url: str, archive_api_key: str) -> Optional[str]:
    """Archive a URL using archive.hackclub.com and return the archived URL."""
    if not archive_api_key:
        print(f"⚠️ No archive API key - skipping archival of {url}")
        return None
    
    try:
        response = requests.post(
            "https://archive.hackclub.com/api/v1/archive",
            headers={
                "Authorization": f"Bearer {archive_api_key}",
                "Content-Type": "application/json"
            },
            json={"url": url},
            timeout=30
        )
        
        if response.status_code == 200:
            archive_data = response.json()
            archive_url = archive_data.get("url")
            print(f"✅ Archived {url} -> {archive_url}")
            return archive_url
        else:
            print(f"❌ Archive failed for {url}: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Archive error for {url}: {e}")
        return None


def process_record(record_id: str, airtable_token: str, archive_api_key: str):
    """Process a single record: fetch URL, archive it, update Archive URL field."""
    try:
        # Initialize Airtable API
        api = Api(airtable_token)
        base_id = "app3A5kJwYqxMLOgh"
        table_id = "tbl5SU7OcPeZMaDOs"  # YSWS Project Mentions table
        table = api.table(base_id, table_id)
        
        # Fetch the record
        record = table.get(record_id)
        fields = record.get('fields', {})
        
        url = fields.get('URL', '')
        if not url:
            print(f"⚠️ No URL found in record {record_id}")
            return
        
        # Check if already archived
        existing_archive_url = fields.get('Archive URL', '')
        if existing_archive_url:
            print(f"ℹ️ Record {record_id} already has archive URL: {existing_archive_url}")
            return
        
        print(f"📦 Processing {record_id}: {url}")
        
        # Archive the URL
        archived_url = archive_url(url, archive_api_key)
        
        if archived_url:
            # Update the record with the archived URL
            table.update(record_id, {"Archive URL": archived_url})
            print(f"✅ Updated record {record_id} with archived URL")
        
    except Exception as e:
        print(f"❌ Error processing record {record_id}: {e}")


def main():
    """Main function to process all records."""
    load_dotenv()
    
    airtable_token = os.getenv("AIRTABLE_PERSONAL_ACCESS_TOKEN")
    archive_api_key = os.getenv("ARCHIVE_HACKCLUB_COM_API_KEY")
    
    if not airtable_token:
        raise SystemExit("❌ AIRTABLE_PERSONAL_ACCESS_TOKEN not found in .env")
    
    if not archive_api_key:
        raise SystemExit("❌ ARCHIVE_HACKCLUB_COM_API_KEY not found in .env")
    
    # Read record IDs from file
    try:
        with open("needs_archiving.txt", "r") as f:
            record_ids = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        raise SystemExit("❌ needs_archiving.txt not found")
    
    print(f"🗄️ Starting archival process for {len(record_ids)} records...")
    
    total_processed = 0
    total_archived = 0
    total_skipped = 0
    
    for i, record_id in enumerate(record_ids, 1):
        if i % 50 == 0:
            print(f"\n📋 Progress: {i}/{len(record_ids)} records processed")
        elif i % 10 == 0:
            print(f"📊 {i}/{len(record_ids)}")
        
        # Only show individual record details for first 10
        if i <= 10:
            print(f"\n📋 Processing {i}/{len(record_ids)}: {record_id}")
        
        try:
            process_record(record_id, airtable_token, archive_api_key)
            total_processed += 1
            
            # Small delay to be respectful to both APIs
            time.sleep(0.2)
            
        except Exception as e:
            print(f"❌ Failed to process {record_id}: {e}")
            total_skipped += 1
    
    print(f"\n🎉 Archival complete!")
    print(f"📊 Summary:")
    print(f"   - Total records: {len(record_ids)}")
    print(f"   - Processed: {total_processed}")
    print(f"   - Skipped/Failed: {total_skipped}")


if __name__ == "__main__":
    main()
