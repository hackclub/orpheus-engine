#!/usr/bin/env python3
"""
Cancels any in-flight runs from a previous deployment.
Uses GraphQL terminateRun with terminatePolicy=MARK_AS_CANCELED_IMMEDIATELY.
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error

GRAPHQL = os.getenv("DAGSTER_GRAPHQL_URL", "http://localhost:3000/graphql")

QUERY_INFLIGHT = """
query Inflight {
  runsOrError(filter: {statuses: [STARTED, STARTING, CANCELING]}, limit: 1000) {
    __typename
    ... on Runs {
      results { runId status }
    }
    ... on PythonError { message }
  }
}
"""

MUTATION_TERMINATE = """
mutation TerminateRun($runId: String!, $policy: TerminateRunPolicy) {
  terminateRun(runId: $runId, terminatePolicy: $policy) {
    __typename
    ... on TerminateRunSuccess { run { runId } }
    ... on TerminateRunFailure { message }
    ... on RunNotFoundError { runId }
    ... on PythonError { message stack }
  }
}
"""

def gql(query, variables=None, timeout=10):
    """Make a GraphQL request to the Dagster webserver."""
    payload = json.dumps({"query": query, "variables": variables or {}}).encode("utf-8")
    req = urllib.request.Request(GRAPHQL, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())

def wait_for_webserver(max_wait_seconds=30):
    """Wait for Dagster webserver to be ready."""
    print(f"Waiting for Dagster webserver at {GRAPHQL}...")
    for attempt in range(max_wait_seconds):
        try:
            # Simple query to check readiness
            gql("query { __typename }", timeout=2)
            print("Dagster webserver is ready!")
            return True
        except Exception as e:
            if attempt == 0:
                print(f"Webserver not ready yet, waiting... (attempt 1/{max_wait_seconds})")
            time.sleep(1)
    return False

def main():
    """Main function to cancel orphaned runs."""
    print("Starting orphaned run cleanup...")
    
    if not wait_for_webserver():
        print("ERROR: Dagster webserver not ready within timeout; skipping cleanup.", file=sys.stderr)
        return
    
    try:
        # Get all in-flight runs
        print("Querying for in-flight runs...")
        data = gql(QUERY_INFLIGHT)
        
        if data.get("data", {}).get("runsOrError", {}).get("__typename") != "Runs":
            print("ERROR: Could not list runs; skipping cleanup.", file=sys.stderr)
            print(f"Response: {data}", file=sys.stderr)
            return
        
        inflight = data["data"]["runsOrError"]["results"]
        
        if not inflight:
            print("✓ No in-flight runs to cancel.")
            return
        
        print(f"Found {len(inflight)} in-flight runs to cancel:")
        for run in inflight:
            print(f"  - {run['runId']} (status: {run['status']})")
        
        # Cancel each run
        cancelled, failed = 0, 0
        for run_info in inflight:
            run_id = run_info["runId"]
            try:
                print(f"Cancelling run {run_id}...")
                res = gql(MUTATION_TERMINATE, {
                    "runId": run_id, 
                    "policy": "MARK_AS_CANCELED_IMMEDIATELY"
                })
                
                ty = res.get("data", {}).get("terminateRun", {}).get("__typename")
                if ty == "TerminateRunSuccess":
                    cancelled += 1
                    print(f"  ✓ Successfully cancelled {run_id}")
                else:
                    failed += 1
                    print(f"  ✗ Failed to cancel {run_id}: {res}", file=sys.stderr)
                    
            except Exception as e:
                failed += 1
                print(f"  ✗ Exception canceling {run_id}: {e}", file=sys.stderr)
        
        print(f"\n✓ Cleanup complete! Marked canceled: {cancelled}, failures: {failed}")
        
    except Exception as e:
        print(f"ERROR: Unexpected error during cleanup: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
