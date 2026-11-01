#!/usr/bin/env python
"""
Manual check against a running platform: register a table with the transactive-node historian and publish a few
archive envelopes and standard records. Not a pytest test (it needs VOLTTRON and a running historian).

Before running:
1. VOLTTRON is running with the historian installed and configured (see README.md). HISTORIAN_VIP below must be the
   identity it was installed under (the containers use "tns.historian"; the agent's default is "platform.historian").
2. Create credentials for this script's identity:  vctl auth add --identity test.injector
3. If create_tables is protected (README, "Protecting create_tables"), give test.injector the capability
   <historian identity>.create_tables.

Run with VOLTTRON_HOME set, e.g.:  pixi run python scripts/inject_simple.py

Envelopes go on <ARCHIVE_PREFIX>/archive/<NODE>/records; ARCHIVE_PREFIX must match the historian's
custom_topics.capture_record_data entry (case-sensitive; "tns/archive/" in this project's containers). Adjust the table name, columns and data below for your own schema; keep an "id"
primary key column and do not include it in the data payload.
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
from tent.data_manager import ARCHIVE_SCHEMA_VERSION


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
    HISTORIAN_VIP = "tns.historian"
    ARCHIVE_PREFIX = "tns"
    NODE = "test_injector"
    
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
                HISTORIAN_VIP,
                "create_tables",
                {"definitions": [table_definition], "schema_version": ARCHIVE_SCHEMA_VERSION}
            ).get(timeout=5)
            print(f"[OK] Registration result: {result}")
        except Exception as e:
            print(f"[WARNING] Could not register table: {e}")
            print("Records will remain retryable until the table is registered.")
        
        # Publish test records
        topic = f"{ARCHIVE_PREFIX}/archive/{NODE}/records"
        print(f"\nPublishing test records to {topic}...")
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
