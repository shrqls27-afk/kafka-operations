#!/usr/bin/env python3
"""단일 배포 .sh 실제 검증 전용. RF1 준비는 이 실험 코드에만 존재한다."""
import argparse
import codecs
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import select
import shutil
import signal
import socket
import subprocess
import time
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def existing_bootstrap(environment, value):
    if environment == 'nas' and not (value and value.strip()):
        raise RuntimeError('NAS 시험은 --existing-bootstrap 또는 RF3_EXISTING_BOOTSTRAP으로 상시 broker 주소를 지정해야 합니다.')
    return value.strip() if value else None


def existing_services(environment="nas"):
    """NAS 상시 서비스의 현재 PID를 조회하며 이전 실행 PID를 가정하지 않는다."""
    services = {}
    for p in Path('/proc').iterdir():
        if not p.name.isdecimal(): continue
        try: raw=(p/'cmdline').read_bytes()
        except (FileNotFoundError, PermissionError, ProcessLookupError): continue
        if environment == 'local':
            if b'kafka.Kafka' in raw or b'kafka-ui' in raw:
                services['existing-'+p.name] = int(p.name)
            continue
        if b'kafka.Kafka' in raw and b'kafka-stack/config/node-' in raw:
            m=re.search(rb'node-(\d+)\.properties',raw)
            if m: services['Kafka-'+m[1].decode()]=int(p.name)
        elif b'java' in raw.split(b'\0')[0] and b'kafka-ui-api' in raw:
            services['UI']=int(p.name)
        elif b'codex' in raw.split(b'\0')[0] and b'\0app-server\0' in raw:
            services['Codex']=int(p.name)
        elif b'python' in raw.split(b'\0')[0] and b'telegram-codex.py' in raw:
            services['Telegram']=int(p.name)
    if environment == 'nas' and not (set(services)=={'Kafka-1','Kafka-2','Kafka-3','UI','Codex','Telegram'}): raise AssertionError('NAS 상시 서비스 식별 실패')
    return services


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--throttle', type=int, default=1048576, help='격리 시험 전용 bytes/sec, 운영 추천값 아님')
    parser.add_argument('--environment', choices=['nas', 'local'], default='nas')
    parser.add_argument('--existing-bootstrap', default=os.environ.get('RF3_EXISTING_BOOTSTRAP'),
                        help='NAS 상시 클러스터 사전조회 주소. 저장소에 실제 주소를 기록하지 않음')
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--port-base', type=int, default=43000)
    args = parser.parse_args()
    permanent_bootstrap = existing_bootstrap(args.environment, args.existing_bootstrap)
    if args.throttle <= 0:
        raise RuntimeError('시험 throttle은 양의 정수 필요')
    d = args.directory.resolve()
    kh, jh = Path(os.environ['KAFKA_HOME']), Path(os.environ['JAVA_HOME'])
    if not (not d.exists()): raise AssertionError('새 전용 경로만 허용')
    if not ((kh/'libs/kafka-clients-3.9.1.jar').is_file()): raise AssertionError('안전 검증 실패')
    mem = {line.split(':')[0]: int(line.split()[1]) for line in Path('/proc/meminfo').read_text().splitlines() if line.startswith(('MemAvailable:', 'SwapFree:'))}
    if not (mem['MemAvailable'] >= 2500000): raise AssertionError('안전 검증 실패')
    if not (shutil.disk_usage(ROOT).free >= 5*1024**3): raise AssertionError('안전 검증 실패')
    ports = [args.port_base+i for i in range(1, 7)]
    for port in ports:
        with socket.socket() as s: s.bind(('127.0.0.1', port))
    # 실험 디렉터리 생성/format/기동 전에 상시 클러스터 전체 health를 확인한다.
    services_before = existing_services(args.environment)
    initial_commands = {name:Path('/proc/%d/cmdline'%pid).read_bytes() for name,pid in services_before.items()}
    if args.environment == 'nas':
        health_dir = tempfile.mkdtemp(prefix='rf3-health-')
        subprocess.run(['bash', str(ROOT/'scripts/java.sh'), 'health',
                        permanent_bootstrap,
                        '-', str(Path(health_dir)/'health.json')], check=True, timeout=90)
        if existing_services(args.environment) != services_before:
            raise RuntimeError('health gate 중 상시 서비스 PID 변경')
    d.mkdir(parents=True)
    home = d/'test-home'; (home/'kafka').mkdir(parents=True)
    (home/'kafka/current').symlink_to(kh, target_is_directory=True)
    standalone = d/'standalone'; standalone.mkdir()
    shutil.copy2(ROOT/'offsets-rf3.sh', standalone/'offsets-rf3.sh')
    if not ([p.name for p in standalone.iterdir()] == ['offsets-rf3.sh']): raise AssertionError('안전 검증 실패')
    digest = hashlib.sha256((standalone/'offsets-rf3.sh').read_bytes()).hexdigest()
    env = dict(os.environ, KAFKA_HEAP_OPTS='-Xms256m -Xmx256m')
    # 사용자 지정 HOME은 단일 실험 프로세스와 그 자식에만 적용한다.
    company_env = dict(env, HOME=str(home), PYTHONUNBUFFERED='1')
    bootstrap = ','.join('127.0.0.1:%d'%p for p in ports[:3])
    nodes = [201,202,203]
    quorum = ','.join('%d@127.0.0.1:%d'%(n,ports[3+i]) for i,n in enumerate(nodes))
    java = ['bash',str(ROOT/'scripts/java.sh')]
    owned = []; handles = []; stages = []
    services = services_before

    def save(name, data):
        (d/name).write_text(json.dumps(data,indent=2,ensure_ascii=False)+'\n')

    boundaries = []
    def phase(name):
        (d/'phase').write_text(name)
        boundaries.append({'phase':name,'time_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'monotonic_ns':time.monotonic_ns()})
        save('phase-boundaries.json',boundaries)

    def command(cmd, label, timeout=90):
        ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with (d/(label+'.log')).open('w') as f:
            result = subprocess.run(cmd,env=env,stdout=f,stderr=subprocess.STDOUT,timeout=timeout)
        stages.append({'stage':label,'start_utc':ts,'exit_code':result.returncode})
        save('stages.json',stages)
        if result.returncode: raise RuntimeError(label+' 실패')

    def readiness(mode, label, timeout=180):
        deadline = time.monotonic() + timeout
        attempt = 0
        while time.monotonic() < deadline:
            attempt += 1
            if any(p.poll() is not None for p, _, role in owned if role == 'broker'):
                raise RuntimeError('readiness 중 테스트 broker 종료')
            try:
                command(java+[mode,bootstrap,'-',str(d/(label+'-%d.json'%attempt))],
                        label+'-%d'%attempt, timeout=min(40, max(1, deadline-time.monotonic())))
                return
            except (RuntimeError, subprocess.TimeoutExpired):
                time.sleep(min(2, max(0, deadline-time.monotonic())))
        raise RuntimeError(label+' bounded readiness 시간 초과')

    def snapshot(label):
        command(java+['snapshot',bootstrap,'-',str(d/(label+'.json'))],label)
        return json.loads((d/(label+'.json')).read_text())

    def partitions(s):
        return {p['partition']:p for p in s['partitions']}

    def unchanged(before, after):
        if not (before['cluster_id'] == after['cluster_id']): raise AssertionError('안전 검증 실패')
        if not (partitions(before) == partitions(after)): raise AssertionError('안전 검증 실패')
        if not (before['configs'] == after['configs'] and after['reassignments'] == '{}'): raise AssertionError('안전 검증 실패')

    def completed(before, after, target):
        if not (before['cluster_id'] == after['cluster_id'] and after['reassignments'] == '{}'): raise AssertionError('안전 검증 실패')
        old = partitions(before); live = partitions(after)
        if not (len(old) == len(live) == len(target['partitions']) == 50): raise AssertionError('안전 검증 실패')
        for t in target['partitions']:
            p=live[t['partition']]
            if not (t['topic']=='__consumer_offsets'): raise AssertionError('안전 검증 실패')
            if not (t['replicas'] == p['replicas'] and p['replicas'][:1] == old[t['partition']]['replicas']): raise AssertionError('안전 검증 실패')
            if not (len(p['replicas']) == len(p['isr']) == 3 and set(p['replicas']) == set(p['isr'])): raise AssertionError('안전 검증 실패')
            if not (p['leader'] in p['isr']): raise AssertionError('안전 검증 실패')

    def company(label, baseline, decline=False, resume=None):
        before_dirs = set((home/'kafka-rf3-work').glob('run-*')) if (home/'kafka-rf3-work').exists() else set()
        cmd=['bash',str(standalone/'offsets-rf3.sh')]+(['--resume',str(resume)] if resume else [])
        p=subprocess.Popen(cmd,cwd=standalone,env=company_env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,start_new_session=True)
        owned.append((p,'company.py', 'launcher'))
        decoder=codecs.getincrementaldecoder('utf-8')(); text=''; confirmations=0; answered=set(); work=resume
        first_inputs=[('접속할 broker 주소:포트',bootstrap),('기존 client.properties 절대경로','-'),('상태를 확인할 대표 consumer group.id','rf-lab-stable-group')]
        started=datetime.datetime.now(datetime.timezone.utc).isoformat(); deadline=time.monotonic()+600
        with (d/(label+'.log')).open('w') as log:
            while True:
                if time.monotonic()>deadline: raise RuntimeError('단일 실행기 응답 시간 초과')
                if select.select([p.stdout],[],[],1)[0]:
                    data=os.read(p.stdout.fileno(),65536)
                    if not data: break
                    chunk=decoder.decode(data);text+=chunk;log.write(chunk);log.flush()
                # 주소/인증/그룹 입력도 실제 stdin으로 제공한다.
                for marker,value in first_inputs:
                    if marker in text and marker not in answered:
                        if not (not resume): raise AssertionError('resume에서 초기 입력이 다시 요청됨')
                        p.stdin.write((value+'\n').encode());p.stdin.flush();answered.add(marker)
                marker='승인한 복제 속도 상한 bytes/sec'
                if marker in text and marker not in answered:
                    p.stdin.write((str(args.throttle)+'\n').encode());p.stdin.flush();answered.add(marker)
                count=text.count('(계속하려면 YES 입력)')
                if count>confirmations:
                    if not (not resume): raise AssertionError('done resume에서 변경 승인 재요청')
                    if work is None:
                        new=set((home/'kafka-rf3-work').glob('run-*'))-before_dirs
                        if not (len(new)==1): raise AssertionError('안전 검증 실패')
                        work=new.pop()
                    before=json.loads((work/'plan/before.json').read_text())
                    target=json.loads((work/'plan/target.json').read_text())
                    # 입력을 보내기 전에 독립 Admin 조회로 대상 ID/50개 replica/ISR를 확인.
                    current=snapshot(label+'-gate-%d'%count)
                    if not (before['cluster_id']==baseline['cluster_id']==current['cluster_id']): raise AssertionError('안전 검증 실패')
                    if not (before['brokers']==baseline['brokers']==current['brokers']): raise AssertionError('안전 검증 실패')
                    if not (len(target['partitions'])==50 and set(before['brokers'])==set(nodes)): raise AssertionError('안전 검증 실패')
                    if count in (1,2):
                        unchanged(baseline,current)
                        for t in target['partitions']:
                            if not (t['replicas'][:1]==partitions(baseline)[t['partition']]['replicas']): raise AssertionError('안전 검증 실패')
                            if not (set(t['replicas'])==set(nodes)): raise AssertionError('안전 검증 실패')
                        if not ('DRY RUN:' in text): raise AssertionError('안전 검증 실패')
                    elif count==3:
                        completed(baseline,current,target)
                        if not (len(current['configs'])==4): raise AssertionError('안전 검증 실패')
                        save('throttled.json',current)
                        phase('after')
                    else:raise RuntimeError('예상 밖 승인 질문')
                    value='NO' if decline and count==2 else 'YES'
                    print(label,'대상/계획 독립 확인 후 승인 질문',count,value,flush=True)
                    if count == 2 and not decline:
                        phase('during')
                    p.stdin.write((value+'\n').encode());p.stdin.flush();confirmations=count
            rc=p.wait(timeout=30)
        stages.append({'stage':label,'start_utc':started,'exit_code':rc,'confirmations':confirmations})
        save('stages.json',stages)
        if not (rc == (1 if decline else 0)): raise AssertionError(label+' 예상 종료코드와 다름')
        if not decline:
            if not ('RF 1 → 3 변경과 throttle 원복 확인 완료' in text): raise AssertionError('안전 검증 실패')
        return work

    save('preflight.json',{'environment':args.environment,'existing_services_absent':not services,'time_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'memory_kb':mem,'free_disk_bytes':shutil.disk_usage(d).free,'ports':ports,'script_sha256':digest,'services':services,'python':os.sys.version.split()[0]})
    try:
        cluster_id=subprocess.check_output([str(kh/'bin/kafka-storage.sh'),'random-uuid'],env=env,text=True).strip()
        configs = []
        for i,n in enumerate(nodes):
            cfg=d/('node-%d.properties'%n); logs=d/('logs-%d'%n); logs.mkdir()
            cfg.write_text('\n'.join([
                'process.roles=broker,controller','node.id=%d'%n,'controller.quorum.voters='+quorum,
                'listeners=PLAINTEXT://127.0.0.1:%d,CONTROLLER://127.0.0.1:%d'%(ports[i],ports[3+i]),
                'advertised.listeners=PLAINTEXT://127.0.0.1:%d'%ports[i],
                'listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT','controller.listener.names=CONTROLLER','inter.broker.listener.name=PLAINTEXT',
                'log.dirs='+str(d/('data-%d'%n)),'offsets.topic.replication.factor=1','offsets.topic.num.partitions=50',
                'num.network.threads=2','num.io.threads=2','num.replica.fetchers=1','log.cleaner.threads=1','log.cleaner.dedupe.buffer.size=16777216',
                'log.segment.bytes=16777216','offsets.topic.segment.bytes=1048576','group.initial.rebalance.delay.ms=1000','controlled.shutdown.enable=false',
                'auto.create.topics.enable=false','transaction.state.log.replication.factor=3','transaction.state.log.min.isr=2','']))
            command([str(kh/'bin/kafka-storage.sh'),'format','-t',cluster_id,'-c',str(cfg)],'format-%d'%n)
            configs.append((n, cfg, logs))
        # 모든 format 성공 후에만 전체 노드를 기동한다.
        for n, cfg, logs in configs:
            f=(d/('broker-%d.log'%n)).open('w');handles.append(f)
            p=subprocess.Popen([str(kh/'bin/kafka-server-start.sh'),str(cfg)],env=dict(env,LOG_DIR=str(logs)),stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
            owned.append((p,str(cfg),'broker'))
        readiness('quorum-ready', 'quorum-ready')
        command([str(kh/'bin/kafka-topics.sh'),'--bootstrap-server',bootstrap,'--create','--topic','rf-lab-messages','--partitions','1','--replication-factor','3','--config','min.insync.replicas=2'],'create-data-topic')
        readiness('ready', 'topic-ready')
        phase('warmup')
        f=(d/'client.log').open('w');handles.append(f)
        client=subprocess.Popen(java+['workload',bootstrap,'-',str(d)],env=env,stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
        owned.append((client,'OffsetsLab','workload'))
        save('owned-pids.json',[{'pid':p.pid,'expected':needle,'role':role} for p,needle,role in owned])
        warmup_deadline=time.monotonic()+120
        while not (d/'warmup-ready').exists():
            if client.poll() is not None or time.monotonic() >= warmup_deadline:
                raise RuntimeError('첫 commit warmup 준비 실패')
            time.sleep(0.2)
        readiness('health', 'offsets-ready')
        phase('baseline')
        time.sleep(30)
        baseline=snapshot('independent-before')
        if not (baseline['cluster_id']==cluster_id): raise AssertionError('안전 검증 실패')
        if not (len(baseline['partitions'])==50 and all(len(p['replicas'])==len(p['isr'])==1 for p in baseline['partitions'])): raise AssertionError('안전 검증 실패')
        print('RF1/ISR1 실제 offsets 50개 및 테스트 ID 확인:',cluster_id,'포트',ports,flush=True)
        phase('review')
        declined=company('standalone-no',baseline,decline=True)
        after_no=snapshot('independent-after-no');unchanged(baseline,after_no)
        if not (json.loads((declined/'state.json').read_text())['phase']=='prepared'): raise AssertionError('안전 검증 실패')
        print('변경 직전 NO: assignment/leader/ISR/configs/재할당 불변 확인',flush=True)
        work=company('standalone-yes',baseline)
        target=json.loads((work/'plan/target.json').read_text())
        after=snapshot('independent-after');completed(baseline,after,target)
        if not (baseline['configs']==after['configs']): raise AssertionError('안전 검증 실패')
        if not (json.loads((work/'state.json').read_text())['phase']=='done'): raise AssertionError('안전 검증 실패')
        command([str(kh/'bin/kafka-topics.sh'),'--bootstrap-server',bootstrap,'--topic','__consumer_offsets','--describe'],'independent-topics')
        command([str(kh/'bin/kafka-reassign-partitions.sh'),'--bootstrap-server',bootstrap,'--list'],'independent-list')
        prior=set(work.glob('*-execute.log'))
        # done resume도 after 관측 구간에 포함한다.
        company('standalone-resume',baseline,resume=work)
        if not (set(work.glob('*-execute.log'))==prior): raise AssertionError('안전 검증 실패')
        after_resume=snapshot('independent-after-resume');unchanged(after,after_resume)
        print('단일 실행기 완료 결과와 독립 조회 일치; done resume execute 재전송 없음',flush=True)
        if client.poll() is not None:
            raise RuntimeError('RF 변경 중 workload 종료')
        save('client-preservation.json', {'environment':args.environment,'workload_pid':client.pid,'same_process_through_migration':True})
        time.sleep(30)
        phase('drain');(d/'stop-send').write_text('stop')
        client.wait(timeout=90)
        if not (client.returncode==0): raise AssertionError('안전 검증 실패')
        command([str(kh/'bin/kafka-consumer-groups.sh'),'--bootstrap-server',bootstrap,'--group','rf-lab-stable-group','--describe'],'final-group')
        save('standalone-result.json',{'environment':args.environment,'script_sha256':digest,'cluster_id':cluster_id,'inputs':{'bootstrap':bootstrap,'auth':'-','group':'rf-lab-stable-group','throttle':args.throttle},'decline_before_execute_unchanged':True,'full_launcher_exit_code':0,'done_resume_exit_code':0,'done_resume_no_new_execute_logs':True,'offsets_partitions_rf3_isr3':50,'original_replica_first_preserved':True,'throttle_configs_original_equal':True,'no_reassignments':True,'work_directory':str(work),'declined_work_directory':str(declined),'standalone_directory_only_script':True,'tls_sasl_tested':False,'interrupted_inflight_resume_tested':False})
        print((d/'workload-summary.json').read_text(),flush=True)
    finally:
        (d/'stop-send').write_text('stop')
        shutdown=[]
        for p,needle,role in reversed(owned):
            if p.poll() is None:
                raw=Path('/proc/%d/cmdline'%p.pid).read_bytes().replace(b'\0',b' ').decode(errors='replace')
                if not (needle in raw): raise AssertionError((p.pid,needle))
                p.send_signal(signal.SIGTERM)
                try:p.wait(timeout=40)
                except subprocess.TimeoutExpired:print('종료 대기: 강제 kill하지 않음',p.pid,flush=True)
                shutdown.append({'pid':p.pid,'role':role,'verified_command_contains':Path(needle).name,'signal':'SIGTERM'})
            shutdown.append({'pid':p.pid,'role':role,'exit_code':p.poll()})
        save('shutdown.json',shutdown)
        for f in handles:f.close()
        if not (all(p.poll() is not None for p,_,_ in owned)): raise AssertionError('미종료 테스트 PID 존재')
        for name,pid in services.items():
            if not (Path('/proc/%d/cmdline'%pid).read_bytes()==initial_commands[name]): raise AssertionError(name)
        if existing_services(args.environment) != services:
            raise RuntimeError('시험 전후 기존 서비스 목록/PID 변경')
        for port in ports:
            with socket.socket() as s:
                if not (s.connect_ex(('127.0.0.1',port))!=0): raise AssertionError(port)
        save('service-preservation.json',{'environment':args.environment,'existing_services_absent':not services,'time_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'same_pid_and_command':services,'test_ports_closed':True,'all_owned_test_processes_exited':True})
        print('테스트 PID 종료, 기존 프로세스 PID와 명령 보존 확인; environment='+args.environment,flush=True)


if __name__=='__main__':
    if not __debug__: raise RuntimeError('python -O 금지')
    main()
