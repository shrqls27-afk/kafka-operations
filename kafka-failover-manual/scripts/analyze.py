#!/usr/bin/env python3
"""CLI 입력/소비 전수 대조. 브로커 ACK 카운터가 아님."""
import argparse, collections, datetime, hashlib, json, math, re
from pathlib import Path
p=argparse.ArgumentParser(); p.add_argument('runtime'); p.add_argument('output'); a=p.parse_args()
d=Path(a.runtime); out=Path(a.output); out.mkdir(parents=True,exist_ok=True)
def read(n): return json.loads((d/n).read_text())
def save(n,x): (out/n).write_text(json.dumps(x,indent=2,ensure_ascii=False)+'\n')
run=read('run.json'); snapshots=read('snapshots.json'); expected=[json.loads(x) for x in (d/'input.jsonl').read_text().splitlines()]
received=[]; invalid=[]
for line in (d/'received.jsonl').read_text().splitlines():
    obj=json.loads(line)
    m=re.search(r'Partition:(\d+)\s+Offset:(\d+)\s+('+re.escape(run['run_id'])+r')\|(\d+)\|(\d+)$',obj['line'])
    if not m: invalid.append(obj); continue
    received.append(dict(partition=int(m[1]),offset=int(m[2]),id=int(m[4]),input_ms=int(m[5]),received_ms=obj['time_ms']))
ec=collections.Counter(x['id'] for x in expected); rc=collections.Counter(x['id'] for x in received)
missing=sorted(set(ec)-set(rc)); unexpected=sorted(set(rc)-set(ec)); duplicate=sum(v-1 for v in rc.values() if v>1)
order_errors=[]; previous={}; partition_counts=collections.Counter()
for x in received:
    j=x['partition']; partition_counts[j]+=1
    if j in previous and (x['id']<=previous[j]['id'] or x['offset']<=previous[j]['offset']): order_errors.append(x)
    previous[j]=x
def ms(t): return int(datetime.datetime.fromisoformat(t).timestamp()*1000)
def distribution(values):
    values=sorted(values)
    return dict(count=len(values),p95_ms=values[max(0,math.ceil(len(values)*.95)-1)] if values else None,max_ms=max(values) if values else None)
windows=[]
for e in run['events']:
    begin,end=ms(e['stop_at']),ms(e['recovered_at'])
    down_begin,down_end=ms(e['down_start']),ms(e['down_end'])
    subset=[x for x in received if begin<=x['input_ms']<=end]
    windows.append(dict(node=e['node'],stop_at=e['stop_at'],recovered_at=e['recovered_at'],input_count=sum(begin<=x['time_ms']<=end for x in expected),received_ids_with_input_in_window=len(subset),input_to_observer_delay=distribution([x['received_ms']-x['input_ms'] for x in subset]),observed_down_seconds=round((datetime.datetime.fromisoformat(e['down_end'])-datetime.datetime.fromisoformat(e['down_start'])).total_seconds(),3),down_input_count=sum(down_begin<=x['time_ms']<=down_end for x in expected),down_received_count=sum(down_begin<=x['received_ms']<=down_end for x in received)))
error_summary={}
for label in ('producer','consumer'):
    text=(d/(label+'.err')).read_text()
    error_summary[label]=dict(error_lines=sum(bool(re.search(r'(?:^\s*ERROR\b|\]\s+ERROR\b)',x)) for x in text.splitlines()),exception_mentions=sum(bool(re.search(r'\b\w*Exception\b',x)) for x in text.splitlines()),warning_lines=sum(bool(re.search(r'(?:^\s*WARN\b|\]\s+WARN\b)',x)) for x in text.splitlines()),disconnect_lines=sum(bool(re.search(r'disconnect|could not be established',x,re.I)) for x in text.splitlines()))
values_ok=all(next((x['value'] for x in expected if x['id']==r['id']),None)=='%s|%08d|%d'%(run['run_id'],r['id'],r['input_ms']) for r in received)
preservation=read('preservation.json')
for side in ('health_before','health_after'): preservation[side]['cluster_id']='permanent-cluster-A'
same_clients=len({tuple(s['client_pids']) for s in snapshots})==1 and all(s['clients_alive'] for s in snapshots if s['label']!='final-drain')
if 'client_command_sha256' in snapshots[0]: same_clients= same_clients and len({tuple(s['client_command_sha256']) for s in snapshots if s['label']!='final-drain'})==1
pass_data=not (missing or unexpected or duplicate or invalid or order_errors) and values_ok and len(expected)==len(received) and set(partition_counts)=={0,1,2}
final=snapshots[-1]
summary=dict(run_id=run['run_id'],failure=run['failure'],input_count=len(expected),consumed_count=len(received),unique_consumed=len(rc),missing_ids=missing,unexpected_ids=unexpected,duplicate_count=duplicate,invalid_rows=len(invalid),partition_order_errors=len(order_errors),payload_match=values_ok,partition_counts=dict(partition_counts),producer_exit_code=run.get('producer_exit_code'),completion_basis='sync EOF/exit0 + full input/consumed set comparison; no per-message ACK log',client_pids_preserved=same_clients,client_pids=final['client_pids'],final_commits=final['commits'],errors=error_summary,all_input_to_observer_delay=distribution([x['received_ms']-x['input_ms'] for x in received]),windows=windows,pass_data=pass_data,preservation=preservation,executed_script=read('executed-script.json'))
if run['failure'] or run.get('producer_exit_code')!=0 or not pass_data or not same_clients or any(x['lag'] for x in final['commits']) or not preservation['same_pid_and_cmdline'] or not preservation['test_ports_closed'] or not preservation.get('all_owned_processes_exited') or any(x['error_lines'] for x in error_summary.values()): summary['complete']=False
else: summary['complete']=True
save('summary.json',summary); save('events.json',run['events'])
# 원본 토픽명은 격리시험에서 생성한 이름만 공개. 상시 topic metadata는 공개하지 않음.
save('snapshots.json',snapshots)
hashes={str(f.relative_to(d)):hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted(d.rglob('*')) if f.is_file()}
save('raw-evidence-sha256.json',hashes)
print(json.dumps(summary,indent=2,ensure_ascii=False))
if not summary['complete']: raise SystemExit(1)
