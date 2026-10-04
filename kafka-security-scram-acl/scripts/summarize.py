#!/usr/bin/env python3
"""비공개 원본에서 허용된 필드만 공개 요약으로 추출한다."""
import argparse, collections, datetime, hashlib, json
from pathlib import Path

def summarize(d):
    result=json.loads((d/'result.json').read_text())
    result['final_validation_completed_utc']=datetime.datetime.fromtimestamp((d/'result.json').stat().st_mtime,datetime.timezone.utc).isoformat()
    steps=json.loads((d/'steps.json').read_text())
    result['started_utc']=steps[0]['utc'];result['last_step_utc']=steps[-1]['utc']
    result['commands']=[{'step':x['event'],'utc':x['utc'],'exit_code':x['exit_code']} for x in steps if 'exit_code' in x and not x['event'].startswith('broker_')]
    result['rolling_events']=[{k:v for k,v in x.items() if k in ['utc','event','node','stage']} for x in steps if x['event'] in ['health_ready','broker_start','broker_stop','application_client_config_switch']]
    result['quorum_checkpoints']=[]
    for step in steps:
        if step['event']!='health_ready':continue
        fs=[p for p in d.glob(step['stage']+'-*.json') if p.stem.rsplit('-',1)[-1].isdigit()]
        if not fs:continue
        proof=json.loads(max(fs,key=lambda p:int(p.stem.rsplit('-',1)[-1])).read_text())
        result['quorum_checkpoints'].append({'stage':step['stage'],'utc':step['utc'],'leader':proof['quorum_leader'],'epoch':proof.get('quorum_leader_epoch'),'high_watermark':proof['quorum_high_watermark'],'max_offset_lag':proof.get('quorum_max_offset_lag')})
    if result['scenario']!='C':result['matrix']=json.loads((d/result.get('final_matrix_file','matrix.json')).read_text())
    if result.get('matrix_first_pass_failed'):result['first_matrix']=json.loads((d/'matrix.json').read_text())
    result['recheck_commands']=[{'attempt':f.parent.name,'steps':json.loads(f.read_text())} for f in d.glob('validation-*/recheck-steps.json')]
    if (d/'matrix-after-restart.json').exists():result['matrix_after_restart']=json.loads((d/'matrix-after-restart.json').read_text())
    final=sorted(d.glob('final-[0-9]*.json'),key=lambda p:int(p.stem.split('-')[-1]))[-1]
    h=json.loads(final.read_text());ps=h['partitions'];result['final_health']={'broker_count':len(h['brokers']),'partition_count':len(ps),'all_rf3_isr3':all(len(p['replicas'])==3 and len(p['isr'])==3 for p in ps),'offsets_partition_count':sum(p['topic']=='__consumer_offsets' for p in ps),'healthy':h['healthy'],'quorum_high_watermark':h['quorum_high_watermark'],'quorum_leader_epoch':h.get('quorum_leader_epoch'),'quorum_max_offset_lag':h.get('quorum_max_offset_lag')}
    result['partition_verification']=[{'topic':x['topic'],'partition':x['partition'],'rf':len(x['replicas']),'isr':len(x['isr'])} for x in ps]
    result['final_health_sha256']=hashlib.sha256(final.read_bytes()).hexdigest()
    if (d/'audit.json').exists():
        audit=json.loads((d/'audit.json').read_text());result['final_acl_count']=len(audit['acls']);result['anonymous_acl_remaining']=any('ANONYMOUS' in x for x in audit['acls'])
    if result['scenario']=='C':
        result['secure_shadow_and_negative_probes']=json.loads((d/'coexist-probe.json').read_text())
        def last_health(label):
            fs=list(d.glob(label+'-*.json'))
            return json.loads(max(fs,key=lambda p:int(p.stem.rsplit('-',1)[1])).read_text())
        secure=last_health('secure-listener-health');plain=last_health('plaintext-still-healthy')
        assignments=lambda h:{(p['topic'],p['partition']):p['replicas'] for p in h['partitions']}
        result['same_cluster_and_partition_assignments_both_listeners']=secure['cluster_id']==plain['cluster_id'] and assignments(secure)==assignments(plain)
        assert result['same_cluster_and_partition_assignments_both_listeners']
        result['committed_offset_resume']=json.loads((d/'resume-check.json').read_text())
        if (d/'lab-quorum-profile.json').exists():result['lab_quorum_profile']=json.loads((d/'lab-quorum-profile.json').read_text())
        if (d/'client-profile.json').exists():result['client_profile']=json.loads((d/'client-profile.json').read_text())
        if (d/'independent-offset-check.json').exists():result['independent_cli_offset_check']=json.loads((d/'independent-offset-check.json').read_text())
    shutdown=json.loads((d/'shutdown.json').read_text());result['all_owned_processes_exited']=all(x['exit_code'] is not None for x in shutdown)
    result['recheck_owned_processes_exited']=all(x['exit_code'] is not None for f in d.glob('validation-*/recheck-shutdown.json') for x in json.loads(f.read_text()))
    if (d/'stream-summary.json').exists():
        stream=json.loads((d/'stream-summary.json').read_text());es=[json.loads(x) for x in (d/'events.jsonl').read_text().splitlines()];counts=collections.Counter(x['event'] for x in es)
        acked={x['id'] for x in es if x['event']=='send_ok'};seen={x['id'] for x in es if x['event']=='consume'};attempts={x['id'] for x in es if x['event']=='send_attempt'}
        stream['consumed_without_success_ack']=len(seen-acked);stream['attempted_not_consumed']=len(attempts-seen)
        stream['event_counts']=dict(counts);stream['max_consume_interval_ms']=max((x['interval_ms'] for x in es if x['event']=='consume'),default=0)
        stream['max_observed_lag']=max((x['value'] for x in es if x['event']=='lag'),default=0)
        stream['max_commit_duration_ms']=max((x['duration_ms'] for x in es if x['event']=='commit_ok'),default=0)
        first_send=next((x['elapsed_ms'] for x in es if x['event']=='send_attempt'),0);first_consume=next((x['elapsed_ms'] for x in es if x['event']=='consume'),0)
        stream['first_consume_after_first_send_ms']=first_consume-first_send
        stream['max_send_duration_ms']=max((x['duration_ms'] for x in es if x['event']=='send_ok'),default=0)
        stream['error_types']=dict(collections.Counter(x.get('type','unknown') for x in es if 'error' in x['event']))
        stream['transitions']=[x for x in es if x['event'] in ['producer_client_recreated','consumer_client_recreated','consumer_client_closed_for_transition','rebalance_revoked','rebalance_assigned','drain_complete']]
        stream['per_phase']={phase:dict(collections.Counter(x['event'] for x in es if x['phase']==phase)) for phase in sorted({x['phase'] for x in es})}
        result['stream']=stream
        if result['scenario']=='C':
            result['application_api_error_count']=sum(counts[k] for k in ['send_error','commit_error','commit_error_at_transition','consume_error','producer_thread_error'])
            result['no_application_api_errors_observed']=result['application_api_error_count']==0
            result['zero_application_impact_proven']=False
            result['requires_operating_client_and_sla_validation']=True
    if (d/'post-dry-run-check.json').exists():result['post_dry_run_credential_check']=json.loads((d/'post-dry-run-check.json').read_text())
    if (d/'old-password-denied.log').exists():
        import re
        match=re.search(r'실패 종류: ([A-Za-z0-9]+);',(d/'old-password-denied.log').read_text())
        if match:result['password_rotation_old_error_type']=match.group(1)
    # 원본의 비밀/경로를 포함하지 않으며 해시로 참조한다.
    result['raw_evidence_sha256']={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in d.glob('*.json') if p.name in ['result.json','matrix.json','matrix-after-restart.json','stream-summary.json','steps.json','shutdown.json','audit.json','coexist-probe.json','resume-check.json']}
    return result

if __name__=='__main__':
    a=argparse.ArgumentParser();a.add_argument('runtime',type=Path);a.add_argument('output',type=Path);x=a.parse_args()
    if x.output.exists():raise ValueError('기존 공개 증거를 덮어쓰지 않음')
    x.output.write_text(json.dumps(summarize(x.runtime),indent=2,ensure_ascii=False)+'\n')
