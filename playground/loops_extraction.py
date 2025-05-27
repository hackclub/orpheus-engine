#!/usr/bin/env python3
"""
Simple Loops Email Data Enrichment Script
"""

import requests
import json
import time
import os
from datetime import datetime

# Configuration - Get session token from environment variable
SESSION_TOKEN = os.getenv('LOOPS_SESSION_TOKEN')
if not SESSION_TOKEN:
    raise ValueError("LOOPS_SESSION_TOKEN environment variable is required")
OUTPUT_FILE = "enriched_emails.json"

def build_headers():
    """Build request headers"""
    return {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'content-type': 'application/json',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36'
    }

def build_cookies(session_token):
    """Build cookies string"""
    return {
        '__Secure-next-auth.session-token': session_token
    }

def get_email_titles(headers, cookies):
    """Fetch all email titles"""
    url = "https://app.loops.so/api/trpc/teams.getTitlesOfEmails"
    params = {"input": json.dumps({"json": {}})}
    
    response = requests.get(url, headers=headers, cookies=cookies, params=params)
    response.raise_for_status()
    return response.json()

def get_email_details(email_id, headers, cookies):
    """Fetch email details"""
    url = "https://app.loops.so/api/trpc/emailMessages.getEmailMessageById"
    params = {"input": json.dumps({"json": {"id": email_id}})}
    
    response = requests.get(url, headers=headers, cookies=cookies, params=params)
    response.raise_for_status()
    return response.json()

def get_email_metrics(email_id, headers, cookies):
    """Fetch email metrics"""
    url = "https://app.loops.so/api/trpc/emailMessages.calculateEmailMessageMetrics"
    params = {
        "input": json.dumps({
            "json": {
                "emailMessageId": email_id,
                "filters": None,
                "page": 0,
                "pageSize": 20,
                "sortOrder": "desc",
                "sortBy": "emailCreatedAt"
            }
        })
    }
    
    response = requests.get(url, headers=headers, cookies=cookies, params=params)
    response.raise_for_status()
    return response.json()

def extract_text_preview(content):
    """Extract text preview from email content"""
    text_parts = []
    
    def extract_recursive(node):
        if isinstance(node, dict):
            if node.get('type') == 'text' and 'text' in node:
                text_parts.append(node['text'])
            if 'children' in node:
                for child in node['children']:
                    extract_recursive(child)
            if 'root' in node:
                extract_recursive(node['root'])
    
    extract_recursive(content)
    preview = ' '.join(text_parts)[:300]
    return preview + '...' if len(preview) == 300 else preview

def process_email(email_info, email_type, headers, cookies):
    """Process a single email"""
    email_id = email_info.get('id')
    print(f"Processing {email_type}: {email_info.get('name', 'Unnamed')} ({email_id})")
    
    enriched = {
        'id': email_id,
        'type': email_type,
        'name': email_info.get('name', ''),
        'emoji': email_info.get('emoji', ''),
        'details': {},
        'metrics': {},
        'engagement': {}
    }
    
    try:
        # Get details
        details_response = get_email_details(email_id, headers, cookies)
        details = details_response.get('result', {}).get('data', {}).get('json', {})
        
        if details:
            enriched['details'] = {
                'subject': details.get('subject', ''),
                'fromEmail': details.get('fromEmail', ''),
                'fromName': details.get('fromName', ''),
                'createdAt': details.get('createdAt', ''),
                'updatedAt': details.get('updatedAt', ''),
                'contentPreview': extract_text_preview(details.get('emailContent', {}))
            }
        
        # Get metrics
        time.sleep(0.5)  # Rate limiting
        metrics_response = get_email_metrics(email_id, headers, cookies)
        metrics_data = metrics_response.get('result', {}).get('data', {}).get('json', {})
        
        if metrics_data:
            email_metrics = metrics_data.get('emailMessageMetrics', {})
            full_metrics = metrics_data.get('emailMetrics', {}).get('metrics', {})
            
            enriched['metrics'] = {
                'sentCount': email_metrics.get('sentCount', 0),
                'opens': email_metrics.get('opens', 0),
                'clicks': email_metrics.get('clicks', 0),
                'delivers': full_metrics.get('delivers', 0),
                'unsubscribes': full_metrics.get('unsubscribes', 0),
                'campaignStatus': metrics_data.get('campaignStatus', '')
            }
            
            # Calculate engagement rates
            sent = enriched['metrics']['sentCount']
            if sent > 0:
                enriched['engagement'] = {
                    'openRate': round((enriched['metrics']['opens'] / sent) * 100, 2),
                    'clickRate': round((enriched['metrics']['clicks'] / sent) * 100, 2),
                    'deliveryRate': round((enriched['metrics']['delivers'] / sent) * 100, 2)
                }
        
    except Exception as e:
        enriched['error'] = str(e)
        print(f"  Error: {e}")
    
    time.sleep(0.5)  # Rate limiting
    return enriched

def main():
    headers = build_headers()
    cookies = build_cookies(SESSION_TOKEN)
    print(cookies)
    
    print("Fetching email titles...")
    titles_response = get_email_titles(headers, cookies)
    email_data = titles_response.get('result', {}).get('data', {}).get('json', {})
    
    all_enriched = []
    
    # Process each type of email
    for email_type in ['campaigns', 'loops', 'transactionals']:
        emails = email_data.get(email_type, [])
        print(f"\nProcessing {len(emails)} {email_type}")
        
        # Process first 3 of each type (remove [:3] to process all)
        for email in emails[:3]:
            enriched = process_email(email, email_type[:-1], headers, cookies)
            all_enriched.append(enriched)
    
    # Save results
    output = {
        'generated_at': datetime.now().isoformat(),
        'total_emails': len(all_enriched),
        'emails': all_enriched
    }
    
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f, indent=2)
    
    print(f"\n✅ Successfully enriched {len(all_enriched)} emails")
    print(f"📁 Output saved to: {OUTPUT_FILE}")
    
    # Print summary
    print("\nTop performing emails by open rate:")
    sorted_emails = sorted(
        [e for e in all_enriched if e.get('engagement', {}).get('openRate')],
        key=lambda x: x['engagement']['openRate'],
        reverse=True
    )[:5]
    
    for email in sorted_emails:
        print(f"  - {email['name']}: {email['engagement']['openRate']}% opens")

if __name__ == "__main__":
    main()