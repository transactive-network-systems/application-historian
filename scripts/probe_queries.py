"""Call the TNS historian's standard query RPCs from inside its platform. Run as the platform user with the venv python:
    python scripts/probe_queries.py tcp://<platform address>:22916
It lists topics, queries a device Power topic, matches a pattern and reads metadata; used to verify a deployment."""
import json, sys
from volttron.client.commands.connection import ControlConnection
cc = ControlConnection(sys.argv[1])
rpc = cc.server.vip.rpc
topics = rpc.call('tns.historian', 'get_topic_list').get(timeout=20)
print('topics:', len(topics), 'first:', sorted(topics)[:2])
device_topic = next(t for t in sorted(topics) if '/HP' in t and t.endswith('/Power'))
print('query_historian:', json.dumps(rpc.call('tns.historian', 'query', topic=device_topic, count=3).get(timeout=20), default=str)[:400])
print('by pattern:', rpc.call('tns.historian', 'get_topics_by_pattern', topic_pattern='Campus/SMALL_OFFICE_DR/*/Power').get(timeout=20))
print('metadata:', json.dumps(rpc.call('tns.historian', 'get_topics_metadata', topics=[device_topic]).get(timeout=20), default=str)[:200])
