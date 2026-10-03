#!/usr/bin/env python3
"""Linux 직접 설치 격리 실험. 기본은 자원/포트 확인, --run이 새 클러스터를 생성."""
import argparse, datetime, hashlib, json, os, pathlib, shutil, signal, socket, subprocess, time

ROOT = pathlib.Path(__file__).resolve().parents[1]

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--run', action='store_true')
    ap.add_argument('--directory', type=pathlib.Path, required=True, help='아직 없는 전용 실행 경로')
    ap.add_argument('--port-base', type=int, default=41000)
    ap.add_argument('--baseline-seconds', type=int, default=45)
    ap.add_argument('--after-seconds', type=int, default=45)
    args = ap.parse_args()
    d = args.directory.resolve()
    kh, jh = pathlib.Path(os.environ['KAFKA_HOME']), pathlib.Path(os.environ['JAVA_HOME'])
    assert (kh/'libs/kafka-clients-3.9.1.jar').is_file(), 'Kafka 3.9.1 배포본 필요'
    mem = {line.split(':')[0]: int(line.split()[1]) for line in pathlib.Path('/proc/meminfo').read_text().splitlines() if line.startswith(('MemAvailable:', 'SwapFree:'))}
    assert mem['MemAvailable'] >= 2500000, '가용 메모리 2.5GB 미만: 기존 서비스 보존을 위해 보류'
    assert shutil.disk_usage(d.parent if d.parent.exists() else ROOT).free >= 5*1024**3, '디스크 5GB 미만'
    ports = [args.port_base+i for i in range(1,7)]
    for port in ports:
        with socket.socket() as s: s.bind(('127.0.0.1', port))
    print('사전점검 통과:', mem, '테스트 포트', ports, flush=True)
    if not args.run: return
    assert not d.exists(), '새 실행 경로 필요; 기존 데이터를 재사용/덮어쓰지 않음'
    d.mkdir(parents=True)
    (d/'preflight.json').write_text(json.dumps({'time':datetime.datetime.now(datetime.timezone.utc).isoformat(), 'memory_kb':mem,'ports':ports,'free_disk_bytes':shutil.disk_usage(d).free},indent=2))
    env = dict(os.environ, KAFKA_HEAP_OPTS='-Xms256m -Xmx256m')
    java = ['bash',str(ROOT/'scripts/java.sh')]
    bs = ','.join('127.0.0.1:%d'%p for p in ports[:3])
    nodes = [101,102,103]
    quorum = ','.join('%d@127.0.0.1:%d'%(n,ports[3+i]) for i,n in enumerate(nodes))
    processes=[]; handles=[]
    def cli(tool,*extra):
        return [str(kh/'bin'/tool),'--bootstrap-server',bs]+list(extra)
    def run(cmd, name=None):
        if name:
            with (d/name).open('w') as f: subprocess.run(cmd,env=env,stdout=f,stderr=subprocess.STDOUT,check=True,timeout=90)
        else: subprocess.run(cmd,env=env,check=True,timeout=90)
    def owned(proc, needle):
        path=pathlib.Path('/proc/%d/cmdline'%proc.pid)
        return path.exists() and needle in path.read_bytes().replace(b'\0',b' ').decode(errors='replace')
    try:
        cluster_id=subprocess.check_output([str(kh/'bin/kafka-storage.sh'),'random-uuid'],env=env,text=True).strip()
        for i,n in enumerate(nodes):
            config=d/('node-%d.properties'%n);logs=d/('logs-%d'%n);logs.mkdir()
            config.write_text('\n'.join([
              'process.roles=broker,controller','node.id=%d'%n,'controller.quorum.voters='+quorum,
              'listeners=PLAINTEXT://127.0.0.1:%d,CONTROLLER://127.0.0.1:%d'%(ports[i],ports[3+i]),
              'advertised.listeners=PLAINTEXT://127.0.0.1:%d'%ports[i],
              'listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT',
              'controller.listener.names=CONTROLLER','inter.broker.listener.name=PLAINTEXT',
              'log.dirs='+str(d/('data-%d'%n)),'offsets.topic.replication.factor=1','offsets.topic.num.partitions=50',
              'num.network.threads=2','num.io.threads=2','num.replica.fetchers=1','log.cleaner.threads=1',
              'log.cleaner.dedupe.buffer.size=16777216','log.segment.bytes=16777216',
              'offsets.topic.segment.bytes=1048576','group.initial.rebalance.delay.ms=1000',
              # 최종 전체 종료에서 quorum 상실 후 controlled shutdown 대기를 피한다.
              # 운영에는 적용하지 않으며 workload 종료 뒤 OS SIGTERM은 그대로 사용한다.
              'controlled.shutdown.enable=false',
              'auto.create.topics.enable=false','transaction.state.log.replication.factor=3','transaction.state.log.min.isr=2','']) )
            run([str(kh/'bin/kafka-storage.sh'),'format','-t',cluster_id,'-c',str(config)],'format-%d.log'%n)
            f=(d/('broker-%d.log'%n)).open('w');handles.append(f)
            proc=subprocess.Popen([str(kh/'bin/kafka-server-start.sh'),str(config)],env=dict(env,LOG_DIR=str(logs)),stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
            processes.append((proc,str(config)))
        (d/'owned-pids.json').write_text(json.dumps([{'pid':p.pid,'expected_command':needle} for p,needle in processes],indent=2))
        ready=False
        for _ in range(30):
            time.sleep(2)
            if any(p.poll() is not None for p,_ in processes): raise RuntimeError('테스트 broker 조기 종료')
            try:
                run(cli('kafka-topics.sh','--create','--topic','rf-lab-messages','--partitions','1','--replication-factor','3','--config','min.insync.replicas=2'),'create-topic.log')
                ready=True;break
            except subprocess.SubprocessError: pass
        assert ready,'클러스터 준비 실패'
        # 내부 토픽은 직접 create하지 않고 stable group 첫 commit으로 실제 생성한다.
        (d/'phase').write_text('baseline')
        f=(d/'client.log').open('w');handles.append(f)
        client=subprocess.Popen(java+['workload',bs,'-',str(d)],env=env,stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
        processes.append((client,'OffsetsLab'))
        (d/'owned-pids.json').write_text(json.dumps([{'pid':p.pid,'expected_command':needle} for p,needle in processes],indent=2))
        print('클러스터/연속 workload 시작:',d,flush=True)
        time.sleep(args.baseline_seconds)
        assert client.poll() is None,'workload 조기 종료'
        plan=d/'plan'
        run(java+['plan',bs,'-',str(plan)],'plan.log')
        tool=['python3',str(ROOT/'scripts/reassign.py')]
        options=['--bootstrap',bs,'--directory',str(plan)]
        run(tool+['execute']+options,'dry-run.log')
        digest=hashlib.sha256((plan/'target.json').read_bytes()).hexdigest()
        (d/'phase').write_text('reassignment')
        run(tool+['execute']+options+['--apply','--reviewed-sha256',digest],'execute.log')
        run(java+['snapshot',bs,'-',str(d/'throttled.json')],'throttled-snapshot.log')
        for attempt in range(30):
            run(tool+['verify']+options,'verify-%d.log'%attempt)
            result=subprocess.run(java+['check',bs,'-',str(plan)],env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
            (d/('check-%d.log'%attempt)).write_text(result.stdout)
            if result.returncode==0:break
            time.sleep(2)
        else:raise RuntimeError('RF/ISR 완료 시간 초과')
        run(tool+['verify']+options+['--apply'],'verify-clear.log')
        run(java+['snapshot',bs,'-',str(d/'after.json')],'after-snapshot.log')
        (d/'phase').write_text('after')
        print('50개 파티션 RF=3 ISR=3 확인. throttle 해제 후 연속 처리 관측 중',flush=True)
        time.sleep(args.after_seconds)
        (d/'phase').write_text('drain-and-reconnect')
        (d/'stop-send').write_text('stop')
        client.wait(timeout=90)
        assert client.returncode==0,'workload 검증 실패'
        run(cli('kafka-consumer-groups.sh','--group','rf-lab-stable-group','--describe'),'final-group.log')
        run(cli('kafka-topics.sh','--topic','__consumer_offsets','--describe'),'final-topic.log')
        run(java+['check',bs,'-',str(plan)],'final-check.log')
        print((d/'workload-summary.json').read_text(),flush=True)
    finally:
        shutdown=[]
        for p,needle in reversed(processes):
            if p.poll() is None:
                assert owned(p,needle), 'PID 소유/명령 확인 실패: %d'%p.pid
                command=pathlib.Path('/proc/%d/cmdline'%p.pid).read_bytes().replace(b'\0',b' ').decode(errors='replace')
                shutdown.append({'pid':p.pid,'verified_command_contains':needle,'signal':'SIGTERM'})
                print('소유 PID 정상 종료:',p.pid,needle,flush=True)
                p.send_signal(signal.SIGTERM)
                try:p.wait(timeout=40)
                except subprocess.TimeoutExpired:
                    print('정상 종료 지연: 강제 kill하지 않음',p.pid,flush=True)
            shutdown.append({'pid':p.pid,'exit_code':p.poll()})
        (d/'shutdown.json').write_text(json.dumps(shutdown,indent=2))
        for f in handles:f.close()
        if any(p.poll() is None for p,_ in processes):
            raise RuntimeError('테스트 PID 정상 종료 미완료. shutdown.json 확인; 강제 종료/삭제하지 않음')

if __name__=='__main__':
    if not __debug__: raise RuntimeError('안전 검증을 생략하는 python -O 실행은 금지')
    main()
