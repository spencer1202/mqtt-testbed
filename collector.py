#!/usr/bin/env python3
# collector script, Steve Willoughby, June 2025
#
# This is just a start, a lot more to be added here.
# Collect stats on publishers and subscribers to track what messages were sent and received, what data was sent (and not sent),
# (later) what data was transformed or mis-delivered, altered by policy rules, etc.
# and the throughput time stats for messages going through the system based on timestamps for pub/sub time per message.

from datetime import datetime
import argparse
import os, os.path
import re

ap = argparse.ArgumentParser(prog='collector', description='collect experiment simulator output and summarize')
ap.add_argument('-l', '--logdir', default='~/Downloads/mqtt-logs', help='directory containing subscriber client logs')
ap.add_argument('-p', '--publog', default='typescript', help='main simulator (publisher) log file')
opts = ap.parse_args()

class MessagePayload:
    def __init__(self, timestamp, topic):
        self.timestamp=timestamp
        self.topic=topic
        self.data=''

class ClientData:
    def __init__(self, logfilename):
        self.logfilename=logfilename
        self.id = None
        self.topic = None
        self.connected = None
        self.data = []

logdir = os.path.expanduser(opts.logdir)
publog = os.path.expanduser(opts.publog)
collect_to = None
clients = []
publish_events = {}

with open(publog) as f:
    for line in f:
        if m := re.match(r'^\s*\[(\d+:\d+:\d+)\]\s+Data published on: (\S+).*', line):
            if m.group(2) not in publish_events:
                publish_events[m.group(2)] = []
            publish_events[m.group(2)].append(datetime.strptime(m.group(1), '%H:%M:%S'))

for client in os.scandir(logdir):
    if client.name.startswith('subscriber-') and client.name.endswith('.log'):
        c = ClientData(client.path)
        clients.append(c)
        with open(client.path) as f:
            for line in f:
                if collect_to is not None:
                    collect_to.data += line
                    if line.strip() == '}':
                        collect_to = None
                    continue

                if m := re.match(r'^Client ID:\s*(\S+)', line):
                    c.id = m.group(1)
                    continue
                
                if m := re.match(r'^Topic:\s+(\S+)', line):
                    c.topic = m.group(1)
                    continue

                if m := re.match(r'^\[(\d+-\d+-\d+\s+\d+:\d+:\d+)\]\s+(.*)$', line):
                    timestamp = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
                    event = m.group(2)
                    if event.startswith('Connected'):
                        c.connected = timestamp
                        continue

                    if m := re.match(r"Received on '(.*?)' for", event):
                        collect_to = MessagePayload(timestamp, m.group(1))
                        c.data.append(collect_to)
                        continue

print(f'Published Topics: {len(publish_events)}')
for pub in sorted(publish_events.keys()):
    print(f'{pub:25} {len(publish_events[pub]):5}')
print()

print(f'Subscribers Reporting: {len(clients)}')
for client in clients:
    count={}
    blen={}
    print(f'{client.id:40} {client.topic:25} {len(client.data):5} {client.connected}') 
    for d in client.data:
        count[d.topic] = count.get(d.topic,0) + 1
        blen[d.topic] = blen.get(d.topic, 0) + len(d.data)
    for t in sorted(count.keys()):
        print(f'                                         {t:25} {count[t]:5}, {blen[t]} bytes')


