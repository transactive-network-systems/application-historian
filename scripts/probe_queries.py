"""Call the Application Historian's standard query RPCs from inside its platform, to verify a deployment. Run as the
platform user with the venv python:
    python scripts/probe_queries.py tcp://<platform address>:22916 <historian identity> <device topic glob>
e.g. python scripts/probe_queries.py tcp://127.0.0.1:22916 platform.historian 'Campus/Building/*/Power'
It lists topics, queries the first topic matching the glob, matches the glob and reads that topic's metadata."""
import json, sys
from volttron.client.commands.connection import ControlConnection

address, identity, pattern = sys.argv[1:4]
cc = ControlConnection(address)
rpc = cc.server.vip.rpc
topics = rpc.call(identity, 'get_topic_list').get(timeout=20)
print('topics:', len(topics), 'first:', sorted(topics)[:2])
matched = rpc.call(identity, 'get_topics_by_pattern', topic_pattern=pattern).get(timeout=20)
print('by pattern:', matched)
if not matched:
    sys.exit(f'no topic matches {pattern!r}')
device_topic = sorted(matched)[0]
print('query:', json.dumps(rpc.call(identity, 'query', topic=device_topic, count=3).get(timeout=20), default=str)[:400])
print('metadata:', json.dumps(rpc.call(identity, 'get_topics_metadata', topics=[device_topic]).get(timeout=20),
                              default=str)[:200])
