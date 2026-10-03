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

ROOT = Path(__file__).resolve().parents[1]


def existing_services():
    """NAS 상시 서비스의 현재 PID를 조회하며 이전 실행 PID를 가정하지 않는다."""
    services = {}
    for p in Path('/proc').iterdir():
        if not p.name.isdecimal(): continue
        try: raw=(p/'cmdline').read_bytes()
        except (FileNotFoundError, PermissionError, ProcessLookupError): continue
        if b'kafka.Kafka' in raw and b'kafka-stack/config/node-' in raw:
            m=re.search(rb'node-(\d+)\.properties',raw)
            if m: services['Kafka-'+m[1].decode()]=int(p.name)
        elif b'java' in raw.split(b'\0')[0] and b'kafka-ui-api' in raw:
            services['UI']=int(p.name)
        elif b'codex' in raw.split(b'\0')[0] and b'\0app-server\0' in raw:
            services['Codex']=int(p.name)
        elif b'python' in raw.split(b'\0')[0] and b'telegram-codex.py' in raw:
            services['Telegram']=int(p.name)
    assert set(services)=={'Kafka-1','Kafka-2','Kafka-3','UI','Codex','Telegram'}, 'NAS 상시 서비스 식별 실패'
    return services


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--port-base', type=int, default=43000)
    args = parser.parse_args()
    d = args.directory.resolve()
    kh, jh = Path(os.environ['KAFKA_HOME']), Path(os.environ['JAVA_HOME'])
    assert not d.exists(), '새 전용 경로만 허용'
    assert (kh/'libs/kafka-clients-3.9.1.jar').is_file()
    mem = {line.split(':')[0]: int(line.split()[1]) for line in Path('/proc/meminfo').read_text().splitlines() if line.startswith(('MemAvailable:', 'SwapFree:'))}
    assert mem['MemAvailable'] >= 2500000
    assert shutil.disk_usage(ROOT).free >= 5*1024**3
    ports = [args.port_base+i for i in range(1, 7)]
    for port in ports:
        with socket.socket() as s: s.bind(('127.0.0.1', port))
    d.mkdir(parents=True)
    home = d/'test-home'; (home/'kafka').mkdir(parents=True)
    (home/'kafka/current').symlink_to(kh, target_is_directory=True)
    standalone = d/'standalone'; standalone.mkdir()
    shutil.copy2(ROOT/'offsets-rf3.sh', standalone/'offsets-rf3.sh')
    assert [p.name for p in standalone.iterdir()] == ['offsets-rf3.sh']
    digest = hashlib.sha256((standalone/'offsets-rf3.sh').read_bytes()).hexdigest()
    env = dict(os.environ, KAFKA_HEAP_OPTS='-Xms256m -Xmx256m')
    # 사용자 지정 HOME은 단일 실험 프로세스와 그 자식에만 적용한다.
    company_env = dict(env, HOME=str(home), PYTHONUNBUFFERED='1')
    bootstrap = ','.join('127.0.0.1:%d'%p for p in ports[:3])
    nodes = [201,202,203]
    quorum = ','.join('%d@127.0.0.1:%d'%(n,ports[3+i]) for i,n in enumerate(nodes))
    java = ['bash',str(ROOT/'scripts/java.sh')]
    owned = []; handles = []; stages = []
    services = existing_services()
    initial_commands = {name:Path('/proc/%d/cmdline'%pid).read_bytes() for name,pid in services.items()}

    def save(name, data):
        (d/name).write_text(json.dumps(data,indent=2,ensure_ascii=False)+'\n')

    def command(cmd, label, timeout=90):
        ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with (d/(label+'.log')).open('w') as f:
            result = subprocess.run(cmd,env=env,stdout=f,stderr=subprocess.STDOUT,timeout=timeout)
        stages.append({'stage':label,'start_utc':ts,'exit_code':result.returncode})
        save('stages.json',stages)
        if result.returncode: raise RuntimeError(label+' 실패')

    def snapshot(label):
        command(java+['snapshot',bootstrap,'-',str(d/(label+'.json'))],label)
        return json.loads((d/(label+'.json')).read_text())

    def partitions(s):
        return {p['partition']:p for p in s['partitions']}

    def unchanged(before, after):
        assert before['cluster_id'] == after['cluster_id']
        assert partitions(before) == partitions(after)
        assert before['configs'] == after['configs'] and after['reassignments'] == '{}'

    def completed(before, after, target):
        assert before['cluster_id'] == after['cluster_id'] and after['reassignments'] == '{}'
        old = partitions(before); live = partitions(after)
        assert len(old) == len(live) == len(target['partitions']) == 50
        for t in target['partitions']:
            p=live[t['partition']]
            assert t['topic']=='__consumer_offsets'
            assert t['replicas'] == p['replicas'] and p['replicas'][:1] == old[t['partition']]['replicas']
            assert len(p['replicas']) == len(p['isr']) == 3 and set(p['replicas']) == set(p['isr'])
            assert p['leader'] in p['isr']

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
                        assert not resume, 'resume에서 초기 입력이 다시 요청됨'
                        p.stdin.write((value+'\n').encode());p.stdin.flush();answered.add(marker)
                marker='승인한 복제 속도 상한 bytes/sec'
                if marker in text and marker not in answered:
                    p.stdin.write(b'1048576\n');p.stdin.flush();answered.add(marker)
                count=text.count('(계속하려면 YES 입력)')
                if count>confirmations:
                    assert not resume, 'done resume에서 변경 승인 재요청'
                    if work is None:
                        new=set((home/'kafka-rf3-work').glob('run-*'))-before_dirs
                        assert len(new)==1;work=new.pop()
                    before=json.loads((work/'plan/before.json').read_text())
                    target=json.loads((work/'plan/target.json').read_text())
                    # 입력을 보내기 전에 독립 Admin 조회로 대상 ID/50개 replica/ISR를 확인.
                    current=snapshot(label+'-gate-%d'%count)
                    assert before['cluster_id']==baseline['cluster_id']==current['cluster_id']
                    assert before['brokers']==baseline['brokers']==current['brokers']
                    assert len(target['partitions'])==50 and set(before['brokers'])==set(nodes)
                    if count in (1,2):
                        unchanged(baseline,current)
                        for t in target['partitions']:
                            assert t['replicas'][:1]==partitions(baseline)[t['partition']]['replicas']
                            assert set(t['replicas'])==set(nodes)
                        assert 'DRY RUN:' in text
                    elif count==3:
                        completed(baseline,current,target)
                        assert len(current['configs'])==4
                        save('throttled.json',current)
                    else:raise RuntimeError('예상 밖 승인 질문')
                    value='NO' if decline and count==2 else 'YES'
                    print(label,'대상/계획 독립 확인 후 승인 질문',count,value,flush=True)
                    p.stdin.write((value+'\n').encode());p.stdin.flush();confirmations=count
            rc=p.wait(timeout=30)
        stages.append({'stage':label,'start_utc':started,'exit_code':rc,'confirmations':confirmations})
        save('stages.json',stages)
        assert rc == (1 if decline else 0),label+' 예상 종료코드와 다름'
        if not decline:
            assert 'RF 1 → 3 변경과 throttle 원복 확인 완료' in text
        return work

    save('preflight.json',{'time_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'memory_kb':mem,'free_disk_bytes':shutil.disk_usage(d).free,'ports':ports,'script_sha256':digest,'services':services,'python':os.sys.version.split()[0]})
    try:
        cluster_id=subprocess.check_output([str(kh/'bin/kafka-storage.sh'),'random-uuid'],env=env,text=True).strip()
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
            f=(d/('broker-%d.log'%n)).open('w');handles.append(f)
            p=subprocess.Popen([str(kh/'bin/kafka-server-start.sh'),str(cfg)],env=dict(env,LOG_DIR=str(logs)),stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
            owned.append((p,str(cfg),'broker'))
        time.sleep(10)
        command([str(kh/'bin/kafka-topics.sh'),'--bootstrap-server',bootstrap,'--create','--topic','rf-lab-messages','--partitions','1','--replication-factor','3','--config','min.insync.replicas=2'],'create-data-topic')
        (d/'phase').write_text('baseline')
        f=(d/'client.log').open('w');handles.append(f)
        client=subprocess.Popen(java+['workload',bootstrap,'-',str(d)],env=env,stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
        owned.append((client,'OffsetsLab','workload'))
        save('owned-pids.json',[{'pid':p.pid,'expected':needle,'role':role} for p,needle,role in owned])
        time.sleep(30)
        baseline=snapshot('independent-before')
        assert baseline['cluster_id']==cluster_id
        assert len(baseline['partitions'])==50 and all(len(p['replicas'])==len(p['isr'])==1 for p in baseline['partitions'])
        print('RF1/ISR1 실제 offsets 50개 및 테스트 ID 확인:',cluster_id,'포트',ports,flush=True)
        (d/'phase').write_text('decline-before-execute')
        declined=company('standalone-no',baseline,decline=True)
        after_no=snapshot('independent-after-no');unchanged(baseline,after_no)
        assert json.loads((declined/'state.json').read_text())['phase']=='prepared'
        print('변경 직전 NO: assignment/leader/ISR/configs/재할당 불변 확인',flush=True)
        (d/'phase').write_text('standalone-migration')
        work=company('standalone-yes',baseline)
        target=json.loads((work/'plan/target.json').read_text())
        after=snapshot('independent-after');completed(baseline,after,target)
        assert baseline['configs']==after['configs']
        assert json.loads((work/'state.json').read_text())['phase']=='done'
        command([str(kh/'bin/kafka-topics.sh'),'--bootstrap-server',bootstrap,'--topic','__consumer_offsets','--describe'],'independent-topics')
        command([str(kh/'bin/kafka-reassign-partitions.sh'),'--bootstrap-server',bootstrap,'--list'],'independent-list')
        prior=set(work.glob('*-execute.log'))
        (d/'phase').write_text('done-resume')
        company('standalone-resume',baseline,resume=work)
        assert set(work.glob('*-execute.log'))==prior
        after_resume=snapshot('independent-after-resume');unchanged(after,after_resume)
        print('단일 실행기 완료 결과와 독립 조회 일치; done resume execute 재전송 없음',flush=True)
        (d/'phase').write_text('after');time.sleep(30)
        (d/'phase').write_text('drain-and-reconnect');(d/'stop-send').write_text('stop')
        client.wait(timeout=90);assert client.returncode==0
        command([str(kh/'bin/kafka-consumer-groups.sh'),'--bootstrap-server',bootstrap,'--group','rf-lab-stable-group','--describe'],'final-group')
        save('standalone-result.json',{'script_sha256':digest,'cluster_id':cluster_id,'inputs':{'bootstrap':bootstrap,'auth':'-','group':'rf-lab-stable-group','throttle':1048576},'decline_before_execute_unchanged':True,'full_launcher_exit_code':0,'done_resume_exit_code':0,'done_resume_no_new_execute_logs':True,'offsets_partitions_rf3_isr3':50,'original_replica_first_preserved':True,'throttle_configs_original_equal':True,'no_reassignments':True,'work_directory':str(work),'declined_work_directory':str(declined),'standalone_directory_only_script':True,'tls_sasl_tested':False,'interrupted_inflight_resume_tested':False})
        print((d/'workload-summary.json').read_text(),flush=True)
    finally:
        (d/'stop-send').write_text('stop')
        shutdown=[]
        for p,needle,role in reversed(owned):
            if p.poll() is None:
                raw=Path('/proc/%d/cmdline'%p.pid).read_bytes().replace(b'\0',b' ').decode(errors='replace')
                assert needle in raw,(p.pid,needle)
                p.send_signal(signal.SIGTERM)
                try:p.wait(timeout=40)
                except subprocess.TimeoutExpired:print('종료 대기: 강제 kill하지 않음',p.pid,flush=True)
                shutdown.append({'pid':p.pid,'role':role,'verified_command_contains':Path(needle).name,'signal':'SIGTERM'})
            shutdown.append({'pid':p.pid,'role':role,'exit_code':p.poll()})
        save('shutdown.json',shutdown)
        for f in handles:f.close()
        assert all(p.poll() is not None for p,_,_ in owned),'미종료 테스트 PID 존재'
        for name,pid in services.items():assert Path('/proc/%d/cmdline'%pid).read_bytes()==initial_commands[name],name
        for port in ports:
            with socket.socket() as s:assert s.connect_ex(('127.0.0.1',port))!=0,port
        save('service-preservation.json',{'time_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'same_pid_and_command':services,'test_ports_closed':True,'all_owned_test_processes_exited':True})
        print('테스트 PID 종료, 기존 Kafka/UI/Codex/Telegram PID와 명령 유지 확인',flush=True)


if __name__=='__main__':
    if not __debug__: raise RuntimeError('python -O 금지')
    main()
