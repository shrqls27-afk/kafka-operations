#!/usr/bin/env python3
"""격리 NAS 실측용. Kafka 기본 CLI만 클라이언트로 사용. 수동 절차는 MANUAL-RUNBOOK.md."""
import argparse, collections, datetime, hashlib, json, os, re, shutil, signal, socket, subprocess, threading, time
from pathlib import Path
from gates import replication_rows, voters_caught_up, SequentialGate

p = argparse.ArgumentParser()
p.add_argument('--kafka-home', required=True)
p.add_argument('--java-home', required=True)
p.add_argument('--existing-bootstrap', required=True)
p.add_argument('--runtime', required=True)
a = p.parse_args()
d = Path(a.runtime).resolve()
if d.exists(): raise RuntimeError('새 runtime 디렉터리가 필요합니다')
d.mkdir(parents=True)
env = dict(os.environ, JAVA_HOME=a.java_home, PATH=a.java_home+'/bin:'+os.environ['PATH'], KAFKA_HEAP_OPTS='-Xms128m -Xmx256m')
kh = Path(a.kafka_home).resolve()
owned = []; events = []; snapshots = []; lock = threading.Lock(); halt = threading.Event()
runid = 'failover-'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
bs = ','.join('127.0.0.1:%d'%v for v in (62192,62292,62392))
topic = runid; group = runid+'-group'
def now(): return datetime.datetime.now(datetime.timezone.utc).isoformat()
def save(name, obj): (d/name).write_text(json.dumps(obj, indent=2, ensure_ascii=False)+'\n')
def cli(tool, args, label, bootstrap=None, timeout=75):
    cmd = [str(kh/'bin'/('kafka-'+tool+'.sh'))]+args
    r = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=timeout)
    (d/(label+'.out')).write_text(r.stdout); (d/(label+'.err')).write_text(r.stderr)
    if r.returncode: raise RuntimeError(label+': CLI exit '+str(r.returncode))
    return r.stdout
def services():
    found = {}
    for f in Path('/proc').glob('[0-9]*/cmdline'):
        try: raw=f.read_bytes()
        except (OSError, PermissionError): continue
        first=raw.split(b'\0')[0]
        m=re.search(rb'kafka-stack/config/node-(\d+)\.properties',raw)
        if b'kafka.Kafka' in raw and m: found['broker'+m[1].decode()]=(int(f.parent.name), raw.hex())
        elif b'java' in first and b'kafka-ui-api' in raw: found['ui']=(int(f.parent.name),raw.hex())
        elif b'codex' in first and b'\0app-server\0' in raw: found['codex']=(int(f.parent.name),raw.hex())
        elif b'python' in first and b'telegram-codex.py' in raw: found['telegram']=(int(f.parent.name),raw.hex())
    if len(found)!=6: raise RuntimeError('상시 서비스 6개 식별 실패')
    return found
def resource(label):
    mem={x.split(':')[0]:int(x.split()[1]) for x in Path('/proc/meminfo').read_text().splitlines() if x.startswith(('MemAvailable:', 'SwapFree:'))}
    raid=Path('/proc/mdstat').read_text()
    procs=subprocess.check_output(['ps','-eo','comm'],text=True)
    obj=dict(time=now(),memory_kb=mem,free_disk_bytes=shutil.disk_usage(d).free,load=os.getloadavg(),raid_busy=bool(re.search(r'resync|recovery|check =',raid)),scrub_process=bool(re.search(r'(?m)^.*scrub.*$',procs)))
    save(label+'-resources.json',obj)
    for command in (['uptime'],['df','-h',str(d)],['dmesg']):
        r=subprocess.run(command,capture_output=True,text=True)
        (d/(label+'-'+command[0]+'.txt')).write_text(r.stdout+r.stderr)
        if command[0]=='dmesg' and re.search(r'Out of memory|Killed process.*java',r.stdout,re.I): raise RuntimeError('OOM 기록: 원인 확인 전 보류')
    if mem['MemAvailable']<1500000 or obj['free_disk_bytes']<2*1024**3 or obj['raid_busy'] or obj['scrub_process']: raise RuntimeError('자원 gate 보류')
    return obj
def parse_topics(out):
    rows=[]
    for line in out.splitlines():
        m=re.search(r'Topic:\s+(\S+).*Partition:\s+(\d+).*Leader:\s+(-?\d+).*Replicas:\s+([\d,]+).*Isr:\s+([\d,]+)',line)
        if m: rows.append(dict(topic=m[1],partition=int(m[2]),leader=int(m[3]),replicas=list(map(int,m[4].split(','))),isr=list(map(int,m[5].split(',')))))
    return rows
def permanent(label):
    out=cli('topics',['--bootstrap-server',a.existing_bootstrap,'--describe'],label+'-topics')
    rows=parse_topics(out)
    if not rows or any(x['leader']<0 or set(x['replicas'])!=set(x['isr']) for x in rows): raise RuntimeError('상시 ISR 불건전')
    offsets=[x for x in rows if x['topic']=='__consumer_offsets']
    if not offsets or any(len(x['replicas'])!=3 for x in offsets): raise RuntimeError('상시 offsets RF3 조건 실패')
    status=cli('metadata-quorum',['--bootstrap-server',a.existing_bootstrap,'describe','--status'],label+'-quorum')
    replication=cli('metadata-quorum',['--bootstrap-server',a.existing_bootstrap,'describe','--replication'],label+'-replication')
    if not re.search(r'LeaderId:\s+\d+',status): raise RuntimeError('상시 quorum leader 없음')
    if not voters_caught_up(replication,{1,2,3}): raise RuntimeError('상시 quorum lag')
    return dict(partitions=len(rows),offsets_partitions=len(offsets),full_isr=True,quorum_voters=3,quorum_lag_zero=True,cluster_id=re.search(r'ClusterId:\s+(\S+)',status)[1],controller=int(re.search(r'LeaderId:\s+(\d+)',status)[1]),epoch=int(re.search(r'LeaderEpoch:\s+(\d+)',status)[1]))
def spawn(tool,args,label,stdin=None):
    e=dict(env,LOG_DIR=str(d/(label+'-logs')))
    if label in ('producer','consumer'):
        logging=d/'client-log4j.properties'
        logging.write_text('log4j.rootLogger=INFO, stderr\nlog4j.appender.stderr=org.apache.log4j.ConsoleAppender\nlog4j.appender.stderr.Target=System.err\nlog4j.appender.stderr.layout=org.apache.log4j.PatternLayout\nlog4j.appender.stderr.layout.ConversionPattern=[%d] %p %m (%c)%n\n')
        e['KAFKA_LOG4J_OPTS']='-Dlog4j.configuration=file:'+str(logging)
    out=open(d/(label+'.out'),'w'); err=open(d/(label+'.err'),'w')
    proc=subprocess.Popen([str(kh/'bin'/('kafka-'+tool+'.sh'))]+args,env=e,stdin=stdin,stdout=subprocess.PIPE if label=='consumer' else out,stderr=err,start_new_session=True)
    if label=='consumer':
        def record():
            with open(d/'received.jsonl','w') as f:
                for line in iter(proc.stdout.readline,b''):
                    text=line.decode(errors='replace'); out.write(text); out.flush()
                    f.write(json.dumps(dict(time_ms=time.time_ns()//1000000,line=text.rstrip()))+'\n'); f.flush()
        threading.Thread(target=record,daemon=True).start()
    owned.append((proc,label)); return proc
def stop(proc, marker):
    if proc.poll() is not None: return
    raw=Path('/proc/%d/cmdline'%proc.pid).read_bytes()
    if marker.encode() not in raw or b'java' not in raw.split(b'\0')[0]: raise RuntimeError('대상 PID 경로 불일치')
    proc.send_signal(signal.SIGTERM); proc.wait(timeout=60)
def snapshot(label):
    rows=parse_topics(cli('topics',['--bootstrap-server',bs,'--describe'],label+'-topics'))
    status=cli('metadata-quorum',['--bootstrap-server',bs,'describe','--status'],label+'-status')
    rep=cli('metadata-quorum',['--bootstrap-server',bs,'describe','--replication'],label+'-replication')
    cg=cli('consumer-groups',['--bootstrap-server',bs,'--group',group,'--describe'],label+'-group')
    commits=[]
    for l in cg.splitlines():
        x=l.split()
        if len(x)>=6 and x[0]==group and x[1]==topic and x[3].isdigit(): commits.append(dict(partition=int(x[2]),current=int(x[3]),end=int(x[4]),lag=int(x[5])))
    consumed=collections.Counter()
    if (d/'received.jsonl').exists():
        for l in (d/'received.jsonl').read_text().splitlines():
            m=re.search(r'Partition:(\d+)',json.loads(l)['line'])
            if m: consumed[m[1]]+=1
    command_hashes=[]
    for client in (producer,consumer):
        try: command_hashes.append(hashlib.sha256(Path('/proc/%d/cmdline'%client.pid).read_bytes()).hexdigest())
        except FileNotFoundError: command_hashes.append(None)
    obj=dict(label=label,time=now(),topics=rows,controller=int(re.search(r'LeaderId:\s+(\d+)',status)[1]),epoch=int(re.search(r'LeaderEpoch:\s+(\d+)',status)[1]),replication=rep,commits=commits,consumed_by_partition=dict(consumed),client_pids=[producer.pid,consumer.pid],client_command_sha256=command_hashes,clients_alive=producer.poll() is None and consumer.poll() is None)
    snapshots.append(obj); save('snapshots.json',snapshots); return obj
def ready(label, count=3):
    end=time.monotonic()+180; stable=0; previous=None
    while time.monotonic()<end:
        try:
            s=snapshot(label+'-'+str(len(snapshots)))
            rows=s['topics']
            ok=rows and all(len(x['replicas'])==3 and len(x['isr'])==count and x['leader']>=0 for x in rows)
            live_ids={101,102,103} if count==3 else {x for r in rows for x in r['isr']}
            ok=ok and len(live_ids)==count and voters_caught_up(s['replication'],live_ids)
            if ok and s['clients_alive'] and len(s['commits'])==3 and sum(x['lag'] for x in s['commits'])<30:
                identity=(s['controller'],s['epoch'])
                stable=stable+1 if identity==previous else 1
                previous=identity
                if stable>=2: return s
            else: stable=0
        except (RuntimeError, subprocess.TimeoutExpired): stable=0
        time.sleep(3)
    raise RuntimeError('복구/ISR/quorum/lag gate 시간 초과')
def permanent_gate(label):
    deadline=time.monotonic()+180; previous=None; tries=0
    while time.monotonic()<deadline:
        tries+=1
        try:
            h=permanent(label+'-'+str(tries))
            identity=(h['cluster_id'],h['controller'],h['epoch'])
            if previous==identity: return h
            previous=identity
        except (RuntimeError,subprocess.TimeoutExpired): previous=None
        time.sleep(3)
    raise RuntimeError('상시 클러스터 연속 안정성 gate 시간 초과')
save('executed-script.json',dict(sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()))
save('executed-gates.json',dict(sha256=hashlib.sha256(Path(__file__).with_name('gates.py').read_bytes()).hexdigest()))
before=services(); resource('before'); health_before=permanent_gate('permanent-before')
save('services-before.json',before)
for port in (62192,62292,62392,62193,62293,62393):
    low,high=map(int,Path('/proc/sys/net/ipv4/ip_local_port_range').read_text().split())
    if low<=port<=high: raise RuntimeError('시험 listener가 OS ephemeral 범위에 포함됨')
    with socket.socket() as s: s.bind(('127.0.0.1',port))
for tool in ('storage','server-start','topics','console-producer','console-consumer','consumer-groups','metadata-quorum','broker-api-versions'):
    r=subprocess.run([str(kh/'bin'/('kafka-'+tool+'.sh')),'--help'],env=env,capture_output=True,text=True,timeout=40)
    (d/(tool+'-help.txt')).write_text(r.stdout+r.stderr)
cid=cli('storage',['random-uuid'],'cluster-id').strip()
if cid==health_before['cluster_id']: raise RuntimeError('cluster UUID 충돌')
nodes={}; configs={}
producer=consumer=None; failure=None
try:
    voters=','.join('%d@127.0.0.1:%d'%(100+i,62193+(i-1)*100) for i in (1,2,3))
    for i in (1,2,3):
        cfg=d/('node%d.properties'%i); configs[i]=cfg
        cfg.write_text('\n'.join(['process.roles=broker,controller','node.id='+str(100+i),'controller.quorum.voters='+voters,'listeners=PLAINTEXT://127.0.0.1:%d,CONTROLLER://127.0.0.1:%d'%(62192+(i-1)*100,62193+(i-1)*100),'advertised.listeners=PLAINTEXT://127.0.0.1:'+str(62192+(i-1)*100),'controller.listener.names=CONTROLLER','inter.broker.listener.name=PLAINTEXT','listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT','log.dirs='+str(d/('data%d'%i)),'offsets.topic.replication.factor=3','offsets.topic.num.partitions=3','min.insync.replicas=2','default.replication.factor=3','unclean.leader.election.enable=false','auto.create.topics.enable=false','group.initial.rebalance.delay.ms=0','log.segment.bytes=16777216'])+'\n')
        cli('storage',['format','--cluster-id',cid,'--config',str(cfg)],'format'+str(i))
    for i in (1,2,3): nodes[i]=spawn('server-start',[str(configs[i])],'node'+str(i))
    for attempt in range(30):
        if any(node.poll() is not None for node in nodes.values()): raise RuntimeError('기동 중 격리 broker 종료: 원본 로그 확인')
        try:
            api=cli('broker-api-versions',['--bootstrap-server',bs],'startup-api',timeout=20)
            if all('id: %d'%(100+i) in api for i in (1,2,3)): break
        except (RuntimeError,subprocess.TimeoutExpired): pass
        time.sleep(3)
    else: raise RuntimeError('broker3 기동 실패')
    cli('topics',['--bootstrap-server',bs,'--create','--topic',topic,'--replica-assignment','101:102:103,102:103:101,103:101:102','--config','min.insync.replicas=2','--config','unclean.leader.election.enable=false'],'create')
    consumer=spawn('console-consumer',['--bootstrap-server',bs,'--topic',topic,'--group',group,'--from-beginning','--consumer-property','enable.auto.commit=true','--consumer-property','auto.commit.interval.ms=1000','--property','print.partition=true','--property','print.offset=true','--property','print.timestamp=true'],'consumer')
    producer=spawn('console-producer',['--bootstrap-server',bs,'--topic',topic,'--sync','--producer-property','acks=all','--producer-property','enable.idempotence=true','--producer-property','retries=2147483647','--producer-property','delivery.timeout.ms=120000','--producer-property','request.timeout.ms=30000','--producer-property','linger.ms=0','--producer-property','partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner'],'producer',subprocess.PIPE)
    def feed():
        n=0
        with open(d/'input.jsonl','w') as f:
            while not halt.is_set():
                n+=1; ts=time.time_ns()//1000000; value='%s|%08d|%d'%(runid,n,ts)
                try: producer.stdin.write((value+'\n').encode()); producer.stdin.flush()
                except BrokenPipeError: halt.set(); break
                f.write(json.dumps(dict(id=n,time_ms=ts,value=value))+'\n'); f.flush(); time.sleep(.1)
    feeder=threading.Thread(target=feed); feeder.start()
    baseline=ready('baseline');
    config_log=(d/'producer.err').read_text()
    applied={}
    for key,wanted in {'acks':('-1','all'),'enable.idempotence':('true',),'retries':('2147483647',),'delivery.timeout.ms':('120000',),'request.timeout.ms':('30000',),'linger.ms':('0',),'partitioner.class':('class org.apache.kafka.clients.producer.RoundRobinPartitioner',)}.items():
        match=re.search(r'(?m)^\s*'+re.escape(key)+r'\s*=\s*(.+)$',config_log)
        if not match or match[1].strip() not in wanted: raise RuntimeError('실제 ProducerConfig 확인 실패: '+key)
        applied[key]=match[1].strip()
    save('producer-applied-config.json',applied)
    if {r['leader'] for r in baseline['topics'] if r['topic']==topic}!={101,102,103}: raise RuntimeError('leader 분산 실패')
    sequence=[1,2,3]; sequential=SequentialGate()
    for index in range(4):
        if index==3:
            current=ready('controller-extra-before'); i=current['controller']-100
            if any(e['controller_before']==e['node_id'] for e in events): break
        else: i=sequence[index]; current=ready('node%d-before'%i)
        # 선호 replica 리더 복원: 각 순차 정지가 실제 데이터 leader를 바꾸게 한다.
        cli('leader-election',['--bootstrap-server',bs,'--election-type','PREFERRED','--all-topic-partitions'],'preferred-'+str(index))
        current=ready('preferred-stable-'+str(index)); resource('node%d-before'%i)
        prior_commit=sum(x['current'] for x in current['commits'])
        event=dict(node=i,node_id=100+i,pid=nodes[i].pid,stop_signal='SIGTERM',stop_at=now(),controller_before=current['controller'],leaders_before={str(r['partition']):r['leader'] for r in current['topics'] if r['topic']==topic})
        save('pending-event.json',event)
        sequential.stop(i,True)
        stop(nodes[i],str(configs[i])); event['exit_at']=now(); event['exit_code']=nodes[i].returncode
        down=ready('node%d-down'%i,2); save('pending-event.json',dict(event,down_start=down['time'])); time.sleep(45); resource('node%d-down'%i); down_end=ready('node%d-down-end'%i,2)
        event.update(down_start=down['time'],down_end=down_end['time'],controller_down=down_end['controller'],leaders_down={str(r['partition']):r['leader'] for r in down_end['topics'] if r['topic']==topic},commit_before=prior_commit,commit_down=sum(x['current'] for x in down_end['commits']))
        event['commits_before']=current['commits']; event['commits_down_start']=down['commits']; event['commits_down_end']=down_end['commits']
        event['consumed_down_start']=down['consumed_by_partition']; event['consumed_down_end']=down_end['consumed_by_partition']
        if event['commit_down']<=event['commit_before'] or not down_end['clients_alive']: raise RuntimeError('장애 중 commit/client 지속 실패')
        if not voters_caught_up(down_end['replication'],{101,102,103}-{100+i}) or down_end['controller']==100+i: raise RuntimeError('down_end 살아있는 두 voter 복제 실패')
        if any(len(r['isr'])!=2 or 100+i in r['isr'] or r['leader']==100+i for r in down_end['topics']): raise RuntimeError('down_end ISR2 실패')
        start_commits={x['partition']:x['current'] for x in down['commits']}
        if any(x['current']<=start_commits[x['partition']] for x in down_end['commits']): raise RuntimeError('파티션별 commit 증가 실패')
        if any(down_end['consumed_by_partition'].get(str(j),0)<=down['consumed_by_partition'].get(str(j),0) for j in (0,1,2)): raise RuntimeError('파티션별 소비 증가 실패')
        if not any(v==100+i and event['leaders_down'][k]!=v for k,v in event['leaders_before'].items()): raise RuntimeError('실제 데이터 leader 교체 없음')
        event['start_at']=now(); nodes[i]=spawn('server-start',[str(configs[i])],'node%d-restart-%d'%(i,index)); event['new_pid']=nodes[i].pid
        recovered=ready('node%d-recovered'%i); sequential.recovered(i,True); event.update(recovered_at=recovered['time'],controller_recovered=recovered['controller'],recovered_full_isr=True)
        events.append(event); save('events.json',events)
    if not any(e['controller_before']==e['node_id'] and e['controller_down']!=e['node_id'] for e in events): raise RuntimeError('controller leader 종료 실관측 없음')
    halt.set(); feeder.join(); producer.stdin.close(); producer.wait(timeout=150)
    if producer.returncode: raise RuntimeError('producer 비정상 종료')
    time.sleep(8); final=snapshot('final-drain')
    if any(x['lag'] for x in final['commits']): raise RuntimeError('최종 lag 0 실패')
    stop(consumer,group)
except BaseException as exc:
    failure=repr(exc); save('failure.json',dict(time=now(),error=failure)); print('FAILED',failure,flush=True)
finally:
    halt.set()
    if 'feeder' in globals(): feeder.join(timeout=5)
    # 측정·client drain 이후의 전체 격리랩 정리. 순차 failover 시험과 별도 단계.
    # controller quorum이 먼저 사라져 controlled shutdown이 대기하지 않도록
    # 살아있는 격리 broker들에 정상 TERM을 전달한 뒤 각각 완료를 기다린다.
    cleanup_nodes=[]
    for proc,label in owned:
        if label.startswith('node') and proc.poll() is None:
            marker=str(configs[int(label[4])])
            raw=Path('/proc/%d/cmdline'%proc.pid).read_bytes()
            if marker.encode() not in raw or b'java' not in raw.split(b'\0')[0]: raise RuntimeError('정리 PID/path 불일치')
            cleanup_nodes.append((proc,label))
    save('cleanup-start.json',dict(time=now(),phase='after measurement; full isolated lab shutdown, not failover injection',pids=[proc.pid for proc,label in cleanup_nodes]))
    for proc,label in cleanup_nodes: proc.send_signal(signal.SIGTERM)
    for proc,label in reversed(owned):
        if proc.poll() is None:
            marker=str(configs[int(label[4])]) if label.startswith('node') else (group if label=='consumer' else topic)
            try:
                if label.startswith('node'): proc.wait(timeout=60)
                else: stop(proc,marker)
            except Exception as exc: print('cleanup 보류',label,str(exc),flush=True); failure=failure or str(exc)
    preserved=services()==before
    health_after=permanent_gate('permanent-after'); resource('after')
    if not preserved or any(health_before[k]!=health_after[k] for k in ('cluster_id','partitions','offsets_partitions','full_isr','quorum_voters')): failure=failure or '상시 보존 확인 실패'
    closed=True
    for port in (62192,62292,62392,62193,62293,62393):
        with socket.socket() as s:
            if s.connect_ex(('127.0.0.1',port))==0: closed=False
    save('preservation.json',dict(same_pid_and_cmdline=preserved,health_before=health_before,health_after=health_after,test_ports_closed=closed,data_preserved=True,all_owned_processes_exited=all(proc.poll() is not None for proc,label in owned)))
    save('run.json',dict(run_id=runid,topic=topic,group=group,cluster_id=cid,time=now(),failure=failure,events=events,producer_exit_code=producer.returncode if producer else None,consumer_exit_code=consumer.returncode if consumer else None))
    print('runtime',d,'failure',failure,flush=True)
if failure: raise SystemExit(1)
