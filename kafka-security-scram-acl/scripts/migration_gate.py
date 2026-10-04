#!/usr/bin/env python3
"""실험용 client 전환 gate: 실제 전송/소비/commit 확인 전 plain 제거 보류."""
import json,time

def secure_clients_ready(events):
    transports={};producer_ready=consumer_ready=commit_ready=False
    for e in events:
        kind=e['event']
        if kind=='producer_client_recreated':transports['producer']=e['transport'];producer_ready=False
        elif kind=='consumer_client_recreated':transports['consumer']=e['transport'];consumer_ready=commit_ready=False
        elif kind=='send_ok' and transports.get('producer')=='secure':producer_ready=True
        elif kind=='consume' and transports.get('consumer')=='secure':consumer_ready=True
        elif kind=='commit_ok' and transports.get('consumer')=='secure':commit_ready=True
    return transports=={'producer':'secure','consumer':'secure'} and producer_ready and consumer_ready and commit_ready

def wait_for_secure_clients(directory,timeout=180):
    deadline=time.monotonic()+timeout
    while True:
        # writer가 마지막 행을 쓰는 중이면 다음 poll에서 재확인한다.
        events=[]
        for line in (directory/'events.jsonl').read_text().splitlines():
            try:events.append(json.loads(line))
            except json.JSONDecodeError:break
        if secure_clients_ready(events):return
        if time.monotonic()>=deadline:raise RuntimeError('보안 client 전송/소비/commit 확인 실패: plain 제거 보류')
        time.sleep(1)
