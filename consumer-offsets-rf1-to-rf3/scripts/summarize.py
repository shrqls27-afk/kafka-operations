#!/usr/bin/env python3
"""원본 로그를 복사하지 않고 공유 가능한 숫자/assignment만 신규 생성한다."""
import collections, hashlib, json, math, pathlib, sys

def stats(values):
    values=sorted(values)
    return {'count':len(values),'p95':values[max(0,math.ceil(len(values)*.95)-1)] if values else None,'max':max(values) if values else None}

def main():
    src,out=map(pathlib.Path,sys.argv[1:])
    out.mkdir(parents=True,exist_ok=False)
    events=[json.loads(x) for x in (src/'events.jsonl').read_text().splitlines()]
    before=json.loads((src/'plan/before.json').read_text()); after=json.loads((src/'after.json').read_text())
    summary=json.loads((src/'workload-summary.json').read_text())
    summary['version']='Apache Kafka 3.9.1 / JDK 17'
    summary['first_event_utc']=events[0]['time'];summary['last_event_utc']=events[-1]['time']
    summary['elapsed_ms']=events[-1]['elapsed_ms']
    summary['phases']={}
    for phase in sorted({e['phase'] for e in events}):
        es=[e for e in events if e['phase']==phase]
        summary['phases'][phase]={
          'first_ms':min(e['elapsed_ms'] for e in es),'last_ms':max(e['elapsed_ms'] for e in es),
          'events':dict(collections.Counter(e['event'] for e in es)),
          'send_duration_ms':stats([e['duration_ms'] for e in es if e['event']=='send_ok']),
          'commit_duration_ms':stats([e['duration_ms'] for e in es if e['event']=='commit_ok']),
          'consume_latency_ms':stats([e['latency_ms'] for e in es if e['event']=='consume' and e.get('latency_ms',-1)>=0]),
            'consume_interval_ms':stats([e['interval_ms'] for e in es if e['event']=='consume']),
          'lag':stats([e['lag'] for e in es if e['event']=='lag' and e['lag']>=0])}
    summary['errors']={k:v for k,v in collections.Counter(e['event'] for e in events if 'error' in e['event']).items()}
    summary['resume_first']=[{k:e[k] for k in ('offset','expected','elapsed_ms')} for e in events if e['event']=='resume_first']
    summary['snapshot_count']=sum(e['event']=='offsets_snapshot' for e in events)
    leaders={p['partition']:p['leader'] for p in before['partitions']}
    changes=[]
    for e in events:
        if e['event']=='offsets_snapshot':
            for p in e['snapshot']['partitions']:
                old=leaders.get(p['partition'])
                if old is not None and old!=p['leader']:changes.append({'elapsed_ms':e['elapsed_ms'],'partition':p['partition'],'from':old,'to':p['leader']})
                leaders[p['partition']]=p['leader']
    summary['sampled_leader_changes']=changes
    summary['broker_configs_before']=before['configs'];summary['broker_configs_after']=after['configs']
    summary['throttle_snapshot_configs']=json.loads((src/'throttled.json').read_text())['configs']
    summary['final_reassignments']=after['reassignments']
    summary['preflight']=json.loads((src/'preflight.json').read_text())
    summary['shutdown']=json.loads((src/'shutdown.json').read_text())
    for record in summary['shutdown']:
        if 'verified_command_contains' in record:
            record['verified_command_contains']=pathlib.Path(record['verified_command_contains']).name
    rows=[]
    for b in sorted(before['partitions'],key=lambda p:p['partition']):
        a=next(p for p in after['partitions'] if p['partition']==b['partition'])
        assert len(b['replicas'])==len(b['isr'])==1
        assert len(a['replicas'])==len(a['isr'])==3 and set(a['replicas'])==set(a['isr'])
        assert a['replicas'][:1]==b['replicas']
        rows.append({'partition':b['partition'],'before':b,'after':a})
    summary['partition_count']=len(rows)
    files={'summary.json':summary,'partitions.json':rows,
      'original.json':json.loads((src/'plan/original.json').read_text()),'target.json':json.loads((src/'plan/target.json').read_text())}
    for name,value in files.items():(out/name).write_text(json.dumps(value,indent=2,ensure_ascii=False)+'\n')
    hashes={str(p.relative_to(src)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [src/'events.jsonl',src/'client.log',src/'execute.log',src/'verify-clear.log',src/'final-topic.log',src/'final-group.log',src/'plan/before.json',src/'after.json',src/'shutdown.json']}
    (out/'raw-evidence-sha256.json').write_text(json.dumps(hashes,indent=2)+'\n')
    print(json.dumps({k:summary[k] for k in ('acked','duplicates','missing_after_drain','errors','sampled_leader_changes')},ensure_ascii=False))

if __name__=='__main__':main()
