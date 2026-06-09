#!/usr/bin/env python
"""
Simple test script to inject fake TNS data into VOLTTRON for testing the historian.
Uses vctl auth add to create credentials first.
# TODO make this more of an official test. 
SETUP INSTRUCTIONS:
-------------------
Before running this script, you need to:
1. Have VOLTTRON running with the TN Historian agent installed
2. Create credentials for the test agent:
   pixi run vctl auth add --identity test.injector

CUSTOMIZATION REQUIRED:
-----------------------
To use this script for your own system, modify the following:

1. TABLE NAME (Line ~35, optional):
   Change test_table_name = "test_transactive_records" to your desired table name.

2. TABLE SCHEMA (Lines ~59-66):
   Modify the table_columns list to match your data structure:
   - Keep the "id" column with primary_key for SQLAlchemy requirements
   - Add/remove/modify other columns as needed
   - DO NOT include "id" in your test data payload

4. TEST DATA (Lines ~91-97):
   Update the "data" dictionary to match your schema and desired test values.

4. TOPIC (Line ~87, optional):
   Change topic = f"TNS/{test_table_name}" if your historian subscribes to a different pattern.

5. AGENT IDENTITY (Line ~47, optional):
   Change "test.injector" if you want a different identity name.

USAGE:
------
Run with: pixi run python test_inject_simple.py

The script will:
- Connect to VOLTTRON via VIP
- Register the table schema with the historian
- Publish 4 test records
- Display success/error messages
"""

import json
import sys
import os
import gevent
from datetime import datetime
import pytz

# VOLTTRON imports
from volttron.client.vip.agent import Agent
from volttron.client.messaging import headers as headers_mod
from volttron.utils import format_timestamp


def main():
    """Main test function."""
    
    # Get VOLTTRON_HOME from environment
    volttron_home = os.environ.get("VOLTTRON_HOME")
    if not volttron_home:
        print("ERROR: VOLTTRON_HOME not set!")
        print("Run this script with: pixi run python test_inject_simple.py")
        sys.exit(1)
    
    vip_address = f"ipc://@{volttron_home}/run/vip.socket"
    print(f"Connecting to: {vip_address}")
    
    # Create test data
    test_table_name = "test_transactive_records"
    
    print("=" * 60)
    print("TNS Historian Test Data Injection")
    print("=" * 60)
    print()
    
    # Create a publishing agent using platform identity
    print("Creating VOLTTRON agent...")
    os.environ["AGENT_VIP_IDENTITY"] = "test.injector"
    
    try:
        # Simple agent creation - will look for credentials in credentials_store
        agent = Agent(address=vip_address)
        print("Starting agent...")
        gevent.spawn(agent.core.run)
        gevent.sleep(2)  # Give it time to connect
        
        print("[OK] Agent connected")
        
        # First, register the table with the historian via RPC
        print(f"\nRegistering table '{test_table_name}' with historian...")
        table_definition = {
            "name": test_table_name,
            "columns": [
                {"name": "id", "type": "Integer", "kwargs": {"primary_key": True, "autoincrement": True}},
                {"name": "device_name", "type": "String"},
                {"name": "power", "type": "Float"},
                {"name": "price", "type": "Float"},
                {"name": "timestamp", "type": "DateTime"},
                {"name": "notes", "type": "String"}
            ]
        }
        
        try:
            result = agent.vip.rpc.call(
                "tns.historian",
                "create_tables",
                [table_definition]
            ).get(timeout=5)
            print(f"[OK] Registration result: {result}")
        except Exception as e:
            print(f"[WARNING] Could not register table: {e}")
            print("Records will remain retryable until the table is registered.")
        
        # Publish test records
        print(f"\nPublishing test records to TNS/{test_table_name}...")
        topic = f"TNS/{test_table_name}"
        standard_topic = f"record/{test_table_name}"
        
        now = format_timestamp(datetime.utcnow())
        headers = {
            headers_mod.DATE: now,
            headers_mod.TIMESTAMP: now
        }
        
        for i in range(1, 5):
            test_data = {
                "table_name": test_table_name,
                "data": {
                    "device_name": f"test_device_{i:03d}",
                    "power": 100.0 + (i * 25),
                    "price": 0.10 + (i * 0.01),
                    "timestamp": datetime.now(pytz.UTC).isoformat(),
                    "notes": f"Test record {i} from injection script"
                }
            }
            
            if i == 1:
                print(f"\nSample record:")
                print(json.dumps(test_data, indent=2))
            
            agent.vip.pubsub.publish(
                "pubsub",
                topic,
                headers=headers,
                message=test_data
            ).get(timeout=5)
            
            print(f"  [OK] Record {i} published")

            standard_value = 1000.0 + (i * 10.0)
            standard_now = format_timestamp(datetime.utcnow())
            standard_headers = dict(headers)
            standard_headers[headers_mod.DATE] = standard_now
            standard_headers[headers_mod.TIMESTAMP] = standard_now
            standard_headers["units"] = "kW"
            agent.vip.pubsub.publish(
                "pubsub",
                standard_topic,
                headers=standard_headers,
                message=standard_value,
            ).get(timeout=5)

            print(f"  [OK] Standard VOLTTRON record {i} published")
            gevent.sleep(0.5)
        
        print("\n" + "=" * 60)
        print("SUCCESS! Test data injected.")
        print("=" * 60)
        print("\nCheck the historian database and VOLTTRON logs to verify storage.")
        
    except FileNotFoundError as e:
        print(f"\n[ERROR] Credentials not found: {e}")
        print("\nYou need to create credentials first. Run:")
        print(f"  pixi run vctl auth add --identity test.injector")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] Error during injection: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        # Shutdown the agent
        print("\nShutting down agent...")
        try:
            agent.core.stop()
        except:
            pass
        gevent.sleep(1)
    
    print("Done!")


if __name__ == "__main__":
    main()
