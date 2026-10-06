#!/usr/bin/env python3
"""Interactive company procedure. No automatic retries of execute or rollback."""
import argparse
import fcntl
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import sys
import time

from reassign import validate_complete

ROOT = Path(__file__).resolve().parents[1]


def ask(label, default=''):
    value = input(label + (' [' + default + ']' if default else '') + ': ').strip()
    return value or default


def confirm(label):
    if ask(label + ' (계속하려면 YES 입력)') != 'YES':
        raise RuntimeError('사용자가 진행을 보류했습니다.')


def write_state(path, state):
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2))
    tmp.replace(path)


def prerequisites():
    if sys.version_info < (3, 8):
        raise RuntimeError('Python 3.8 이상이 필요합니다.')
    if not shutil.which('flock'):
        raise RuntimeError('Linux util-linux flock이 필요합니다.')
    kh = Path.home() / 'kafka/current'
    if not (kh / 'bin/kafka-reassign-partitions.sh').is_file():
        raise RuntimeError('~/kafka/current/bin에 Kafka 실행 파일이 없습니다.')
    jh = os.environ.get('JAVA_HOME')
    if not jh or not (Path(jh) / 'bin/javac').is_file():
        javac = shutil.which('javac')
        if not javac:
            raise RuntimeError('JDK 17의 javac가 필요합니다. JAVA_HOME을 지정하세요.')
        jh = str(Path(javac).resolve().parent.parent)
    out = subprocess.check_output([str(Path(jh)/'bin/javac'), '-version'], stderr=subprocess.STDOUT, text=True)
    if not out.startswith('javac 17.'):
        raise RuntimeError('검증된 JDK 17을 사용하세요. 현재: ' + out.strip())
    os.environ.update(JAVA_HOME=jh, KAFKA_HOME=str(kh), KAFKA_HEAP_OPTS='-Xms64m -Xmx256m')
    out = subprocess.check_output(['bash', str(kh/'bin/kafka-topics.sh'), '--version'], stderr=subprocess.STDOUT, text=True)
    if not any(line.startswith('3.9.1') for line in out.splitlines()):
        raise RuntimeError('Kafka 3.9.1 배포본인지 확인하세요.')
    return kh


def quorum_status(output, expected_cluster=None):
    fields = dict(re.findall(r'^([A-Za-z]+):\s*(.+)$', output, re.MULTILINE))
    try:
        leader, epoch, hwm = [int(fields[k]) for k in ('LeaderId', 'LeaderEpoch', 'HighWatermark')]
        if min(leader, epoch, hwm) < 0 or not fields['ClusterId'].strip():
            raise ValueError('leader/epoch/high watermark 미준비')
        if expected_cluster is not None and fields['ClusterId'] != expected_cluster:
            raise ValueError('cluster ID 불일치')
        voters = fields['CurrentVoters']
        ids = re.findall(r'"?id"?\s*[:=]\s*(\d+)', voters)
        if not ids:
            if not re.fullmatch(r'\[\s*\d+(?:\s*,\s*\d+)*\s*\]', voters):
                raise ValueError('voter 형식 불명확')
            ids = re.findall(r'\d+', voters)
        ids = tuple(sorted(map(int, ids)))
        if len(ids) != 3 or len(set(ids)) != 3 or leader not in ids:
            raise ValueError('controller voter/leader 불일치')
    except (KeyError, ValueError) as exc:
        raise RuntimeError('quorum status 값 검증 실패: '+str(exc))
    return dict(cluster_id=fields['ClusterId'], leader=leader, epoch=epoch, hwm=hwm, voters=ids)


def validate_quorum_status(output, expected_cluster=None):
    return quorum_status(output, expected_cluster)['cluster_id']


def validate_quorum_replication(output):
    lines = [line.split() for line in output.splitlines() if line.strip()]
    try:
        header, rows = lines[0], lines[1:]
        id_index, offset_index, lag_index, status_index = [header.index(k) for k in
            ('NodeId', 'LogEndOffset', 'Lag', 'Status')]
        voters = [r for r in rows if r[status_index].lower() in ('leader', 'follower')]
        ids = tuple(sorted(int(r[id_index]) for r in voters))
        if len(ids) != 3 or len(set(ids)) != 3:
            raise ValueError('3 voter 필요')
        leaders = [int(r[id_index]) for r in voters if r[status_index].lower() == 'leader']
        offsets = [int(r[offset_index]) for r in voters]
        if len(leaders) != 1 or min(offsets) < 0:
            raise ValueError('leader/offset 미준비')
        if any(int(r[lag_index]) != 0 for r in voters) or len(set(offsets)) != 1:
            raise ValueError('quorum 복제 미동기화')
    except (IndexError, ValueError) as exc:
        raise RuntimeError('quorum replication 값 검증 실패: '+str(exc))
    return dict(leader=leaders[0], voters=ids, log_end_offset=offsets[0])


def stable_quorum_gate(fetch, expected_cluster=None, timeout=180, interval=2,
                       clock=time.monotonic, sleep=time.sleep, observe=None):
    """status→replication→status로 전환을 검사하고 간격 있는 2회 안정 관측 요구."""
    if timeout <= 0 or interval <= 0:
        raise ValueError('양의 제한시간과 관측 간격 필요')
    deadline = clock()+timeout
    previous = None
    cluster = expected_cluster
    voters = None
    last_error = '안정 관측 부족'
    def read(kind):
        remaining = deadline-clock()
        if remaining <= 0:
            raise RuntimeError('quorum gate 제한시간 소진')
        return fetch(kind, min(90, remaining))
    while clock() < deadline:
        try:
            first = quorum_status(read('--status'))
            if cluster is None:
                cluster = first['cluster_id']
            if first['cluster_id'] != cluster:
                raise ValueError('quorum cluster ID 변경: 작업 보류')
            if voters is None:
                voters = first['voters']
            if first['voters'] != voters:
                raise ValueError('quorum voter 구성 변경: 작업 보류')
            replication = validate_quorum_replication(read('--replication'))
            last = quorum_status(read('--status'))
            if last['cluster_id'] != cluster or last['voters'] != voters:
                raise ValueError('quorum cluster/voter 변경: 작업 보류')
            signature = tuple(first[k] for k in ('cluster_id','voters','leader','epoch'))
            if signature != tuple(last[k] for k in ('cluster_id','voters','leader','epoch')):
                raise RuntimeError('status 조회 사이 leader/epoch 전환')
            if replication['leader'] != first['leader'] or replication['voters'] != voters:
                raise RuntimeError('status/replication leader 또는 voter 불일치')
            if last['hwm'] < first['hwm'] or replication['log_end_offset'] < first['hwm']:
                raise RuntimeError('high watermark/복제 offset 불일치')
            stable = previous is not None and previous[0] == signature and first['hwm'] >= previous[1]
            if observe:
                observe({'status_before':first,'replication':replication,'status_after':last,'consecutive_stable':2 if stable else 1})
            if stable and clock() < deadline:
                return cluster
            previous = (signature, last['hwm'])
            last_error = '간격 있는 연속 안정 관측 부족'
        except (RuntimeError, subprocess.TimeoutExpired) as exc:
            previous = None
            last_error = str(exc)
            if observe:
                observe({'retry_reason':last_error})
        remaining = deadline-clock()
        if remaining > 0:
            sleep(min(interval, remaining))
    raise RuntimeError('quorum stable gate 시간 초과: 작업 보류. '+last_error)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--resume', help='기존 작업 디렉터리: 변경을 재실행하지 않고 검증부터 재개')
    args = parser.parse_args()
    os.umask(0o077)
    kh = prerequisites()
    base = Path.home()/'kafka-rf3-work'
    base.mkdir(mode=0o700, exist_ok=True)
    lock = (base/'operation.lock').open('w')
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise RuntimeError('같은 계정에서 이미 작업 프로그램이 실행 중입니다.')
    try:
        if args.resume:
            work = Path(args.resume).expanduser().resolve()
            state = json.loads((work/'state.json').read_text())
            if state['phase'] not in ('submitted', 'uncertain', 'clearing', 'done'):
                raise RuntimeError('변경 요청 전 작업입니다. 상태 확인 후 새 작업으로 계획을 만드세요.')
        else:
            import tempfile
            work = Path(tempfile.mkdtemp(prefix='run-', dir=str(base)))
            state = {'phase': 'prepared'}
            state['bootstrap'] = ask('접속할 broker 주소:포트 (여러 개는 쉼표 구분)')
            if not state['bootstrap']:
                raise RuntimeError('broker 주소가 필요합니다.')
            auth = ask('기존 client.properties 절대경로 (인증 없으면 -)', '-')
            state['auth'] = '-' if auth == '-' else str(Path(auth).expanduser().resolve())
            state['group'] = ask('상태를 확인할 대표 consumer group.id (없으면 Enter)')
        if state['auth'] != '-' and not Path(state['auth']).is_file():
            raise RuntimeError('인증 파일을 찾을 수 없습니다.')
        state_path = work/'state.json'
        write_state(state_path, state)
        print('\nKafka:', kh, '\n작업 증거:', work, flush=True)
        print('이후 터미널 종료/오류 시: bash offsets-rf3.sh --resume ' + str(work), flush=True)
        print('진행 중 Ctrl+C는 이 프로그램만 종료합니다. 서버 재할당은 자동 취소하지 않습니다.')
        common = ['--bootstrap-server', state['bootstrap']]
        if state['auth'] != '-':
            common += ['--command-config', state['auth']]
        plan = work/'plan'

        def run(cmd, label, timeout=90):
            try:
                result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=timeout)
            except subprocess.TimeoutExpired as exc:
                output = exc.stdout or ''
                if isinstance(output, bytes):
                    output = output.decode(errors='replace')
                (work/(str(time.time_ns())+'-'+label+'-timeout.log')).write_text(output+'\nCLI timeout\n')
                raise
            (work/(str(time.time_ns())+'-'+label+'.log')).write_text(result.stdout)
            print(result.stdout, end='', flush=True)
            if result.returncode:
                raise RuntimeError(label + ' 실패. 증거를 보존했습니다. 반복 execute하지 마세요.')
            return result.stdout

        def cli(tool, *params, timeout=90):
            return run(['bash', str(kh/'bin'/tool)] + common + list(params), tool, timeout=timeout)

        def helper(action, *params, directory=None):
            return run([sys.executable, str(ROOT/'scripts/reassign.py'), action,
                        '--bootstrap', state['bootstrap'], '--command-config', state['auth'],
                        '--directory', str(directory or plan)] + list(params), action)

        def snapshot():
            path = work/('snapshot-'+str(time.time_ns())+'.json')
            helper('snapshot', directory=path)
            return json.loads(path.read_text())

        def group():
            if state['group']:
                cli('kafka-consumer-groups.sh', '--describe', '--group', state['group'])

        if not args.resume:
            print('\n[1/5] 사전 조회와 계획 생성')
            cli('kafka-broker-api-versions.sh')
            quorum_observations = []
            def observe_quorum(item):
                quorum_observations.append(dict(item, time_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())))
                (work/'quorum-gate.json').write_text(json.dumps(quorum_observations, indent=2))
            quorum_cluster = stable_quorum_gate(
                lambda flag, remaining: cli('kafka-metadata-quorum.sh', 'describe', flag, timeout=remaining),
                observe=observe_quorum)
            for flag in ('--unavailable-partitions', '--under-replicated-partitions'):
                output = cli('kafka-topics.sh', '--describe', flag)
                if 'Partition:' in output:
                    raise RuntimeError('불건전한 파티션이 있습니다. 작업 보류.')
            group()
            helper('plan')
            helper('execute')  # dry-run validates original replica, ISR and throttle.
            before = json.loads((plan/'before.json').read_text())
            target = json.loads((plan/'target.json').read_text())
            if before['cluster_id'] != quorum_cluster:
                raise RuntimeError('quorum과 Admin cluster ID 불일치')
            digest = hashlib.sha256((plan/'target.json').read_bytes()).hexdigest()
            state.update(cluster_id=before['cluster_id'], sha256=digest)
            print('\n[2/5] 검토: cluster ID', before['cluster_id'], 'brokers', before['brokers'])
            print('파티션', len(target['partitions']), '개, 기존 replica를 첫 위치에 보존하고 RF 1 → 3')
            for item in target['partitions']:
                print('partition', item['partition'], '→', item['replicas'])
            print('계획 SHA256:', digest)
            print('3개 서버 디스크·네트워크 여유, quorum, 중요 그룹의 commit/lag 기준 및 변경 창은 담당자가 확인해야 합니다.')
            confirm('위 클러스터가 맞고 자원/업무 지표가 정상이며 동시 재할당·throttle 작업이 없습니까?')
            raw = ask('승인한 복제 속도 상한 bytes/sec (예: 1048576=1MiB/s; 운영 권장값 아님)')
            if not raw.isdecimal() or int(raw) <= 0:
                raise RuntimeError('양의 정수 throttle 값이 필요합니다.')
            state['throttle'] = int(raw)
            confirm('producer/consumer를 유지한 채 이 계획으로 실제 RF를 변경할까요?')
            from reassign import expected_configs
            (plan/'expected-throttle.json').write_text(json.dumps(expected_configs(before, target, state['throttle']), indent=2))
            state['phase'] = 'uncertain'
            write_state(state_path, state)  # Written before execute: never replay automatically.
            print('\n[3/5] 재할당 요청')
            helper('execute', '--apply', '--throttle', raw, '--reviewed-sha256', digest)
            state['phase'] = 'submitted'
            write_state(state_path, state)

        before = json.loads((plan/'before.json').read_text())
        target = json.loads((plan/'target.json').read_text())
        if hashlib.sha256((plan/'target.json').read_bytes()).hexdigest() != state['sha256']:
            raise RuntimeError('저장한 계획의 해시가 변경됐습니다.')
        print('\n[4/5] 완료 대기: 최대 30분, 30초 간격. 업무 지표는 별도 관측하세요.')
        deadline = time.monotonic()+1800
        while True:
            current = snapshot()
            if current['cluster_id'] != state['cluster_id'] or set(current['brokers']) != set(before['brokers']):
                raise RuntimeError('클러스터 또는 broker 구성이 변경됐습니다.')
            originals = {p['partition']: p['replicas'][0] for p in before['partitions']}
            if any(originals.get(p['partition']) not in p['isr'] or p['leader'] not in p['isr'] for p in current['partitions']):
                raise RuntimeError('원본 replica 또는 leader의 ISR 이상. 추가 변경을 보류합니다.')
            try:
                validate_complete(before, target, current)
                break
            except AssertionError:
                count = sum(len(p['replicas']) == 3 and len(p['isr']) == 3 for p in current['partitions'])
                print('RF3/ISR3:', count, '/', len(current['partitions']), flush=True)
                if current['reassignments'] == '{}':
                    raise RuntimeError('진행 중인 재할당이 없지만 목표와 다릅니다. 자동 재실행하지 않습니다.')
            group()
            if time.monotonic() >= deadline:
                raise RuntimeError('30분 관측 종료. 서버 작업은 취소하지 않았습니다. 원인 확인 후 --resume으로 조회를 재개하세요.')
            time.sleep(30)
        print('\n[5/5] 전체 파티션 RF3/ISR3 및 목표 assignment 일치 확인')
        group()
        if state['phase'] != 'done':
            confirm('업무 지표가 정상이고 다른 throttle 작업이 없습니까? 이번 작업의 throttle을 해제합니다.')
            was_clearing = state['phase'] == 'clearing'
            state['phase'] = 'clearing'
            write_state(state_path, state)
            helper('verify', '--apply', '--throttle', str(state['throttle']), *(['--allow-cleared'] if was_clearing else []))
            after = snapshot()
            if before['configs'] != after['configs']:
                raise RuntimeError('원본과 완료 설정이 다릅니다. throttle 원복 여부를 수동 확인하세요.')
            state['phase'] = 'done'
            write_state(state_path, state)
        if before['configs'] != snapshot()['configs']:
            raise RuntimeError('현재 설정이 원본과 다릅니다. 완료 선언 보류')
        print('\nRF 1 → 3 변경과 throttle 원복 확인 완료. 합의한 관측 시간 동안 업무 지표를 계속 확인하세요.')
        print('broker/producer/consumer 재시작은 수행하지 않았습니다. 증거:', work)

    finally:
        lock.close()

if __name__ == '__main__':
    try:
        main()
    except (Exception, KeyboardInterrupt) as exc:
        print('\n중단:', str(exc) or type(exc).__name__, file=sys.stderr)
        print('자동 rollback/재시작/재할당 재실행은 하지 않았습니다. 출력된 작업 디렉터리를 보존하세요.', file=sys.stderr)
        sys.exit(1)
