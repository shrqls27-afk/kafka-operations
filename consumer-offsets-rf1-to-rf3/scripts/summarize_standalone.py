#!/usr/bin/env python3
"""단일 .sh 실험의 신규 원본 증거에서 작은 공유 요약을 생성한다."""
import collections
import hashlib
import json
from pathlib import Path
import sys

from summarize import stats
from reassign import validate_complete


def main():
    src, out = map(Path, sys.argv[1:])
    out.mkdir(parents=True, exist_ok=False)
    result = json.loads((src/'standalone-result.json').read_text())
    work = Path(result.pop('work_directory')); result.pop('declined_work_directory')
    before = json.loads((src/'independent-before.json').read_text())
    after = json.loads((src/'independent-after.json').read_text())
    target = json.loads((work/'plan/target.json').read_text())
    validate_complete(before,target,after)
    assert before['configs']==after['configs']
    events=[json.loads(x) for x in (src/'events.jsonl').read_text().splitlines()]
    attempts={e['id'] for e in events if e['event']=='send_attempt'}
    ack={e['id'] for e in events if e['event']=='send_ok'}
    consumed=[e['id'] for e in events if e['event']=='consume']
    assert attempts==ack==set(consumed) and len(ack)==len(consumed)
    summary=json.loads((src/'workload-summary.json').read_text())
    summary.update(result)
    summary['client_preservation']=json.loads((src/'client-preservation.json').read_text())
    summary['first_event_utc']=events[0]['time'];summary['last_event_utc']=events[-1]['time']
    summary['elapsed_ms']=events[-1]['elapsed_ms']
    summary['event_errors']=dict(collections.Counter(e['event'] for e in events if 'error' in e['event']))
    inflight=[e for e in events if e['event']=='offsets_snapshot' and e['snapshot']['reassignments'] != '{}']
    observed=[e for e in events if inflight and inflight[0]['elapsed_ms'] <= e['elapsed_ms'] <= inflight[-1]['elapsed_ms']]
    summary['inflight_observation_count']=len(inflight)
    summary['inflight_observed_first_ms']=inflight[0]['elapsed_ms'] if inflight else None
    summary['inflight_observed_last_ms']=inflight[-1]['elapsed_ms'] if inflight else None
    summary['inflight_observed_events']=dict(collections.Counter(e['event'] for e in observed))
    summary['inflight_client_progress_confirmed']=len(inflight)>=2 and all(any(e['event']==kind for e in observed) for kind in ('send_ok','consume','commit_ok'))
    summary['measurement_errors']=dict(collections.Counter(e['event'] for e in events if e['event'].endswith('_error')))
    summary['measurement_error_free']=not summary['measurement_errors']
    summary['phase_boundaries']=json.loads((src/'phase-boundaries.json').read_text())
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
    summary['stages']=json.loads((src/'stages.json').read_text())
    summary['before_configs']=before['configs'];summary['after_configs']=after['configs']
    summary['throttled_configs']=json.loads((src/'throttled.json').read_text())['configs']
    summary['preflight']=json.loads((src/'preflight.json').read_text())
    summary['shutdown']=json.loads((src/'shutdown.json').read_text())
    summary['service_preservation']=json.loads((src/'service-preservation.json').read_text())
    summary['completed_work_state_phase']=json.loads((work/'state.json').read_text())['phase']
    logs=list(work.glob('*.log'))
    summary['actual_execute_log_count']=sum('Successfully started partition reassignments' in p.read_text() for p in logs)
    assert summary['actual_execute_log_count']==1
    summary['helper_cli_exit_codes']=[{'stage':p.name.split('-',1)[1][:-4], 'exit_code':0} for p in sorted(logs)]
    summary['helper_cli_exit_code_basis']='company.run stops on every nonzero result; full execution and done resume exited 0'
    rows=[{'partition':b['partition'],'before':b,'after':next(a for a in after['partitions'] if a['partition']==b['partition'])} for b in before['partitions']]
    for name,value in [('summary.json',summary),('partitions.json',rows),('original.json',json.loads((work/'plan/original.json').read_text())),('target.json',target)]:
        (out/name).write_text(json.dumps(value,indent=2,ensure_ascii=False)+'\n')
    raw=[src/'events.jsonl',src/'standalone-no.log',src/'standalone-yes.log',src/'standalone-resume.log',src/'stages.json',src/'independent-before.json',src/'independent-after.json',src/'independent-after-no.json',src/'independent-after-resume.json',src/'shutdown.json',src/'final-group.log',src/'standalone/offsets-rf3.sh']
    hashes={str(p.relative_to(src)):hashlib.sha256(p.read_bytes()).hexdigest() for p in raw}
    assert hashes['standalone/offsets-rf3.sh']==summary['script_sha256']
    (out/'raw-evidence-sha256.json').write_text(json.dumps(hashes,indent=2)+'\n')
    print(json.dumps({k:summary[k] for k in ['acked','consumed_unique','duplicates','missing_after_drain','event_errors','full_launcher_exit_code','done_resume_exit_code']},ensure_ascii=False))


if __name__=='__main__':main()
