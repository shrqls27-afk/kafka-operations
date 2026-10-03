#!/usr/bin/env python3
"""기본은 조회/계획만. --apply와 계획 SHA256 모두 필요하다."""
import argparse, hashlib, json, os, pathlib, subprocess, sys

ROOT = pathlib.Path(__file__).resolve().parents[1]

def run(args):
    subprocess.run(args, check=True)

def validate(before, target, current):
    assert before['cluster_id'] == current['cluster_id'], '클러스터 ID 변경'
    assert set(before['brokers']) == set(current['brokers']), 'broker 목록 변경'
    assert len(set(current['brokers'])) == 3
    original = {p['partition']: p for p in before['partitions']}
    live = {p['partition']: p for p in current['partitions']}
    entries = target['partitions']
    assert target['version'] == 1
    assert len(entries) == len(original) == len(live)
    assert {p['partition'] for p in entries} == set(original)
    assert current['reassignments'] == '{}', '다른 재할당 진행 중'
    for p in entries:
        old, now = original[p['partition']], live[p['partition']]
        assert p['topic'] == '__consumer_offsets'
        assert old['replicas'] == now['replicas'] == now['isr'], '원본 assignment/ISR 변경'
        assert len(old['replicas']) == 1 and now['leader'] in now['replicas']
        assert len(p['replicas']) == 3 and set(p['replicas']) == set(current['brokers'])
        assert p['replicas'][:1] == old['replicas'], '원본 preferred replica 보존 필요'
    for snapshot in (before, current):
        for values in snapshot['configs'].values():
            for name, entry in values.items():
                if 'throttl' in name or name == 'replica.alter.log.dirs.io.max.bytes.per.second':
                    assert entry['source'] in ('DEFAULT_CONFIG',) and entry['value'] in ('', 'null', '9223372036854775807'), '기존 throttle 발견: 별도 조정 필요'

def validate_complete(before, target, current):
    assert before['cluster_id'] == current['cluster_id'], '클러스터 ID 변경'
    assert set(before['brokers']) == set(current['brokers']) and len(set(current['brokers'])) == 3
    assert current['reassignments'] == '{}', '재할당 진행 중: throttle 해제 보류'
    original = {p['partition']: p for p in before['partitions']}
    live = {p['partition']: p for p in current['partitions']}
    entries = target['partitions']
    assert target['version'] == 1 and len(entries) == len(original) == len(live)
    assert {p['partition'] for p in entries} == set(original)
    for p in entries:
        old, now = original[p['partition']], live[p['partition']]
        assert p['topic'] == '__consumer_offsets'
        assert len(p['replicas']) == 3 and set(p['replicas']) == set(current['brokers'])
        assert p['replicas'][:1] == old['replicas'] and len(old['replicas']) == 1
        assert now['replicas'] == p['replicas'] and set(now['isr']) == set(p['replicas'])
        assert now['leader'] in now['isr']

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('action', choices=['plan', 'execute', 'verify', 'check', 'snapshot'])
    p.add_argument('--bootstrap', required=True)
    p.add_argument('--command-config', default='-', help='인증 properties 경로; 로그/저장소에 넣지 않음')
    p.add_argument('--directory', type=pathlib.Path, required=True)
    p.add_argument('--throttle', type=int, default=1048576)
    p.add_argument('--apply', action='store_true')
    p.add_argument('--reviewed-sha256')
    args = p.parse_args()
    kh = pathlib.Path(os.environ['KAFKA_HOME'])
    java = ['bash', str(ROOT/'scripts/java.sh')]
    common = [args.bootstrap, args.command_config]
    d = args.directory.resolve()
    if args.action in ('plan', 'check', 'snapshot'):
        run(java + [args.action] + common + [str(d)])
        if args.action == 'plan':
            print('검토할 계획 SHA256:', hashlib.sha256((d/'target.json').read_bytes()).hexdigest())
        return
    target = d/'target.json'
    cmd = [str(kh/'bin/kafka-reassign-partitions.sh'), '--bootstrap-server', args.bootstrap,
           '--reassignment-json-file', str(target)]
    if args.command_config != '-': cmd += ['--command-config', args.command_config]
    if args.action == 'execute':
        assert args.throttle > 0
        # 고유 파일을 사용하고 기존 증거를 덮어쓰지 않는다.
        import time
        current = d/('pre-execute-%s.json' % time.time_ns())
        run(java+['snapshot']+common+[str(current)])
        validate(json.loads((d/'before.json').read_text()), json.loads(target.read_text()), json.loads(current.read_text()))
        digest = hashlib.sha256(target.read_bytes()).hexdigest()
        cmd += ['--execute', '--throttle', str(args.throttle)]
        if not args.apply:
            print('DRY RUN: 조회/검증만 수행. 검토 후 --apply --reviewed-sha256', digest)
            print('명령:', ' '.join(cmd)); return
        assert args.reviewed_sha256 == digest, '검토한 계획 SHA256 불일치'
    else:
        cmd += ['--verify']
        if not args.apply:
            cmd += ['--preserve-throttles']
            print('조회만 수행: throttle 보존. 완료 후 --apply verify로 해제')
        else:
            import time
            current = d/('pre-clear-%s.json' % time.time_ns())
            run(java+['snapshot']+common+[str(current)])
            validate_complete(json.loads((d/'before.json').read_text()), json.loads(target.read_text()), json.loads(current.read_text()))
    run(cmd)

if __name__ == '__main__':
    if not __debug__:
        raise RuntimeError('안전 검증을 생략하는 python -O 실행은 금지')
    main()
