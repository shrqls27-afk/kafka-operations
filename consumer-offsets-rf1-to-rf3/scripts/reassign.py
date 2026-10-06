#!/usr/bin/env python3
"""기본은 조회/계획만. --apply와 계획 SHA256 모두 필요하다."""
import argparse, hashlib, json, os, pathlib, subprocess, sys

ROOT = pathlib.Path(__file__).resolve().parents[1]

def run(args):
    subprocess.run(args, check=True, timeout=90)

def validate(before, target, current):
    if not (before['cluster_id'] == current['cluster_id']): raise AssertionError('클러스터 ID 변경')
    if not (set(before['brokers']) == set(current['brokers'])): raise AssertionError('broker 목록 변경')
    if not (len(set(current['brokers'])) == 3): raise AssertionError('안전 검증 실패')
    original = {p['partition']: p for p in before['partitions']}
    live = {p['partition']: p for p in current['partitions']}
    entries = target['partitions']
    if not (target['version'] == 1): raise AssertionError('안전 검증 실패')
    if not (len(entries) == len(original) == len(live)): raise AssertionError('안전 검증 실패')
    if not ({p['partition'] for p in entries} == set(original)): raise AssertionError('안전 검증 실패')
    if not (current['reassignments'] == '{}'): raise AssertionError('다른 재할당 진행 중')
    for p in entries:
        old, now = original[p['partition']], live[p['partition']]
        if not (p['topic'] == '__consumer_offsets'): raise AssertionError('안전 검증 실패')
        if not (old['replicas'] == now['replicas'] == now['isr']): raise AssertionError('원본 assignment/ISR 변경')
        if not (len(old['replicas']) == 1 and now['leader'] in now['replicas']): raise AssertionError('안전 검증 실패')
        if not (len(p['replicas']) == 3 and set(p['replicas']) == set(current['brokers'])): raise AssertionError('안전 검증 실패')
        if not (p['replicas'][:1] == old['replicas']): raise AssertionError('원본 preferred replica 보존 필요')
    for snapshot in (before, current):
        for values in snapshot['configs'].values():
            for name, entry in values.items():
                if 'throttl' in name or name == 'replica.alter.log.dirs.io.max.bytes.per.second':
                    if not (entry['source'] in ('DEFAULT_CONFIG',) and entry['value'] in ('', 'null', '9223372036854775807')): raise AssertionError('기존 throttle 발견: 별도 조정 필요')
def validate_complete(before, target, current):
    if not (before['cluster_id'] == current['cluster_id']): raise AssertionError('클러스터 ID 변경')
    if not (set(before['brokers']) == set(current['brokers']) and len(set(current['brokers'])) == 3): raise AssertionError('안전 검증 실패')
    if not (current['reassignments'] == '{}'): raise AssertionError('재할당 진행 중: throttle 해제 보류')
    original = {p['partition']: p for p in before['partitions']}
    live = {p['partition']: p for p in current['partitions']}
    entries = target['partitions']
    if not (target['version'] == 1 and len(entries) == len(original) == len(live)): raise AssertionError('안전 검증 실패')
    if not ({p['partition'] for p in entries} == set(original)): raise AssertionError('안전 검증 실패')
    for p in entries:
        old, now = original[p['partition']], live[p['partition']]
        if not (p['topic'] == '__consumer_offsets'): raise AssertionError('안전 검증 실패')
        if not (len(p['replicas']) == 3 and set(p['replicas']) == set(current['brokers'])): raise AssertionError('안전 검증 실패')
        if not (p['replicas'][:1] == old['replicas'] and len(old['replicas']) == 1): raise AssertionError('안전 검증 실패')
        if not (now['replicas'] == p['replicas'] and set(now['isr']) == set(p['replicas'])): raise AssertionError('안전 검증 실패')
        if not (now['leader'] in now['isr']): raise AssertionError('안전 검증 실패')

def expected_configs(before, target, throttle):
    import copy
    result = copy.deepcopy(before['configs'])
    originals = {p['partition']: p['replicas'] for p in before['partitions']}
    leaders = sorted('%d:%d' % (p, b) for p, bs in originals.items() for b in bs)
    followers = sorted('%d:%d' % (p['partition'], b) for p in target['partitions']
                       for b in p['replicas'] if b not in originals[p['partition']])
    if len(result) != 4:
        raise RuntimeError('topic 및 broker 3개 설정 증거 필요')
    for resource, values in result.items():
        if 'type=TOPIC' in resource:
            for role, entries in [('leader', leaders), ('follower', followers)]:
                values[role+'.replication.throttled.replicas'] = {
                    'value': ','.join(entries), 'source': 'DYNAMIC_TOPIC_CONFIG'}
        elif 'type=BROKER' in resource:
            for role in ('leader', 'follower'):
                values[role+'.replication.throttled.rate'] = {
                    'value': str(throttle), 'source': 'DYNAMIC_BROKER_CONFIG'}
        else:
            raise RuntimeError('알 수 없는 config resource')
    return result


def canonical_config(key, entry):
    import re
    if not key.endswith('.replication.throttled.replicas'):
        return entry
    value = entry['value']
    if value in ('', 'null'):
        return entry
    entries = value.split(',')
    if any(not re.fullmatch(r'[0-9]+:[0-9]+', item) for item in entries):
        raise RuntimeError('throttle replica 와일드카드/형식 변경: 해제 보류')
    pairs = [tuple(map(int, item.split(':'))) for item in entries]
    if len(set(pairs)) != len(pairs):
        raise RuntimeError('throttle replica 중복: 해제 보류')
    return dict(entry, value=sorted(pairs))


def validate_throttle_values(before, target, current, throttle, allow_cleared=False):
    expected = expected_configs(before, target, throttle)
    actual = current['configs']
    if set(actual) != set(expected):
        raise RuntimeError('throttle resource 변경: 해제 보류')
    for resource, values in expected.items():
        if set(actual[resource]) != set(values):
            raise RuntimeError('throttle config 목록 변경: 해제 보류')
        for key, value in values.items():
            live = actual[resource][key]
            original = before['configs'][resource].get(key)
            matches_expected = canonical_config(key, live) == canonical_config(key, value)
            matches_original = original is not None and canonical_config(key, live) == canonical_config(key, original)
            if not matches_expected and not (allow_cleared and matches_original):
                raise RuntimeError('이번 작업의 expected throttle과 실제 값/source 불일치: 해제 보류')

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('action', choices=['plan', 'execute', 'verify', 'check', 'snapshot'])
    p.add_argument('--bootstrap', required=True)
    p.add_argument('--command-config', default='-', help='인증 properties 경로; 로그/저장소에 넣지 않음')
    p.add_argument('--directory', type=pathlib.Path, required=True)
    p.add_argument('--throttle', type=int, default=1048576)
    p.add_argument('--apply', action='store_true')
    p.add_argument('--reviewed-sha256')
    p.add_argument('--allow-cleared', action='store_true')
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
        if not (args.throttle > 0): raise AssertionError('안전 검증 실패')
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
        if not (args.reviewed_sha256 == digest): raise AssertionError('검토한 계획 SHA256 불일치')
        expected = expected_configs(json.loads((d/'before.json').read_text()), json.loads(target.read_text()), args.throttle)
        (d/'expected-throttle.json').write_text(json.dumps(expected, indent=2))
    else:
        cmd += ['--verify']
        if not args.apply:
            cmd += ['--preserve-throttles']
            print('조회만 수행: throttle 보존. 완료 후 --apply verify로 해제')
        else:
            import time
            current = d/('pre-clear-%s.json' % time.time_ns())
            run(java+['snapshot']+common+[str(current)])
            before, goal, live = json.loads((d/'before.json').read_text()), json.loads(target.read_text()), json.loads(current.read_text())
            validate_complete(before, goal, live)
            expected_path = d/'expected-throttle.json'
            if not expected_path.is_file() or json.loads(expected_path.read_text()) != expected_configs(before, goal, args.throttle):
                raise RuntimeError('저장한 expected throttle 증거와 지정값 불일치: 해제 보류')
            validate_throttle_values(before, goal, live, args.throttle, args.allow_cleared)
    run(cmd)

if __name__ == '__main__':
    if not __debug__:
        raise RuntimeError('안전 검증을 생략하는 python -O 실행은 금지')
    main()
