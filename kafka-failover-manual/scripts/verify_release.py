#!/usr/bin/env python3
"""실측 증거의 순차성·독립 상태와 공개 파일 링크/해시 검증."""
import datetime, hashlib, json, re
from pathlib import Path
from gates import voters_caught_up
root=Path(__file__).resolve().parents[1]; ev=root/'evidence/final'
def read(n): return json.loads((ev/n).read_text())
def require(ok,message):
    if not ok: raise SystemExit(message)
s=read('summary.json'); events=read('events.json'); snapshots=read('snapshots.json')
require(s['complete'] and s['pass_data'],'전수 데이터/정리 완료 실패')
require(s['producer_exit_code']==0 and s['client_pids_preserved'],'producer EOF/PID 보존 실패')
require([e['node'] for e in events[:3]]==[1,2,3],'순차 node1/2/3 결과 없음')
require(any(e['controller_before']==e['node_id'] and e['controller_down']!=e['node_id'] for e in events),'controller leader 종료 관측 없음')
bytime={x['time']:i for i,x in enumerate(snapshots)}
def check_pair(timestamp,count):
    i=bytime[timestamp]; require(i>0,'연속 상태 없음')
    pair=snapshots[i-1:i+1]
    require(len({(x['controller'],x['epoch']) for x in pair})==1,'연속 leader/epoch 불안정')
    for x in pair:
        require(x['clients_alive'],'측정 중 client 종료')
        require(all(len(r['replicas'])==3 and len(r['isr'])==count and r['leader'] in r['isr'] for r in x['topics']),'RF/ISR/leader 실패')
        live={v for r in x['topics'] for v in r['isr']}
        require(len(live)==count and voters_caught_up(x['replication'],live),'voter 복제 실패')
for i,e in enumerate(events):
    if i: require(events[i-1]['recovered_at']<e['stop_at'],'이전 복구 전 다음 종료')
    require(e['exit_at']<e['start_at']<e['recovered_at'],'종료/기동/복구 시각 순서 실패')
    require(datetime.datetime.fromisoformat(e['down_end'])-datetime.datetime.fromisoformat(e['down_start'])>=datetime.timedelta(seconds=45),'실제 정지 관측45초 미달')
    check_pair(e['down_start'],2); check_pair(e['down_end'],2); check_pair(e['recovered_at'],3)
    require(any(v==e['node_id'] and e['leaders_down'][p]!=v for p,v in e['leaders_before'].items()),'partition leader 전환 없음')
    start={x['partition']:x['current'] for x in e['commits_down_start']}
    require(len(start)==3 and all(x['current']>start[x['partition']] for x in e['commits_down_end']),'partition별 commit 증가 실패')
    require(all(e['consumed_down_end'][str(j)]>e['consumed_down_start'][str(j)] for j in (0,1,2)),'partition별 소비 증가 실패')
require(s['executed_script']['sha256']==hashlib.sha256((root/'scripts/live_lab.py').read_bytes()).hexdigest(),'실행본/배포본 hash 불일치')
require(read('executed-gates.json')['sha256']==hashlib.sha256((root/'scripts/gates.py').read_bytes()).hexdigest(),'실행 gate hash 불일치')
for file in root.rglob('*.md'):
    for target in re.findall(r'\]\(([^)]+)\)',file.read_text()):
        if target.startswith(('http:','https:')): continue
        require((file.parent/target.split('#')[0]).exists(),'링크 없음: '+str(file)+' '+target)
manifest=read('published-scripts-sha256.json')
for name,digest in manifest.items(): require(hashlib.sha256((root/name).read_bytes()).hexdigest()==digest,'공개 script hash 불일치: '+name)
print('PASS: 데이터/순차종료/ISR/quorum/commit/소비/controller/실행해시/링크')
