#!/usr/bin/env python3
"""NAS A/B/C 순차 격리 실험. 운영 경로에 format/재시작을 하지 않는다."""
import argparse, datetime, hashlib, json, os, re, secrets, shutil, signal, socket, subprocess, time
from pathlib import Path
from migration_gate import wait_for_secure_clients

ROOT=Path(__file__).resolve().parents[1]

def run_lab(which,d,base,kh,jh,profile="short",quorum_profile="default"):
    assert quorum_profile=="default" or which=="C"
    assert profile=="short" or which=="C"
    assert not d.exists();d.mkdir(parents=True);d.chmod(0o700)
    home=d/'home';(home/'kafka').mkdir(parents=True);(home/'kafka/current').symlink_to(kh,target_is_directory=True)
    env=dict(os.environ,HOME=str(home),JAVA_HOME=str(jh),KAFKA_HOME=str(kh),KAFKA_HEAP_OPTS='-Xms256m -Xmx256m')
    ports=[base+i for i in range(1,13)]
    ephemeral_low,ephemeral_high=map(int,Path('/proc/sys/net/ipv4/ip_local_port_range').read_text().split())
    assert all(not ephemeral_low<=p<=ephemeral_high for p in ports), 'listener 추가 도중 outbound 임시 포트 충돌 방지: ephemeral 범위 밖 포트 필요'
    for port in ports:
        with socket.socket() as s:s.bind(('127.0.0.1',port))
    nodes=[301,302,303];legacy=ports[:3];ctrl=ports[3:6];secure=ports[6:9];tlsctrl=ports[9:12]
    bs=lambda pp:','.join('localhost:%d'%p for p in pp)
    creds={u:secrets.token_hex(24) for u in ['security-root','topic-admin','producer','consumer']+['broker-%d'%n for n in nodes]}
    storepass=secrets.token_hex(20);(d/'store-password').write_text(storepass+'\n');(d/'store-password').chmod(0o600)
    users=d/'users.properties';users.write_text(''.join('%s=%s\n'%(u,p) for u,p in creds.items()));users.chmod(0o600)
    owned={};all_owned=[];handles=[];steps=[]
    java=[str(jh/'bin/java'),'-Xms64m','-Xmx192m','-cp',str(ROOT/'build')+':'+str(kh/'libs/*'),'SecurityProbe']
    def save(name,data): (d/name).write_text(json.dumps(data,indent=2,ensure_ascii=False)+'\n')
    def event(name,**kv):
        steps.append(dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),event=name,**kv));save('steps.json',steps)
        print(which,name,kv,flush=True)
    def run(cmd,label,timeout=90,expected=0):
        with (d/(label+'.log')).open('w') as f:r=subprocess.run(cmd,env=env,stdout=f,stderr=subprocess.STDOUT,timeout=timeout)
        event(label,exit_code=r.returncode)
        assert r.returncode==expected,(label,r.returncode)
    def probe(mode,p,extra,label,expected=0,timeout=90):run(java+[mode,str(p)]+[str(x) for x in extra],label,timeout,expected)
    # 전용 CA와 노드 인증서. 비밀은 private 파일에서만 읽고 argv에 넣지 않는다.
    ca=d/'ca.crt';key=d/'ca.key';pwfile=d/'store-password'
    run(['openssl','req','-new','-x509','-newkey','rsa:2048','-sha256','-days','3','-subj','/CN=security-lab-ca','-passout','file:'+str(pwfile),'-keyout',str(key),'-out',str(ca)],'tls-ca')
    run([str(jh/'bin/keytool'),'-importcert','-noprompt','-alias','lab-ca','-file',str(ca),'-keystore',str(d/'trust.p12'),'-storetype','PKCS12','-storepass:file',str(pwfile)],'tls-trust')
    for n in nodes:
        ext=d/('node-%d.ext'%n);ext.write_text('subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth,clientAuth\n')
        run(['openssl','req','-new','-newkey','rsa:2048','-nodes','-subj','/CN=node%d'%n,'-keyout',str(d/('node-%d.key'%n)),'-out',str(d/('node-%d.csr'%n))],'tls-key-%d'%n)
        run(['openssl','x509','-req','-in',str(d/('node-%d.csr'%n)),'-CA',str(ca),'-CAkey',str(key),'-passin','file:'+str(pwfile),'-CAcreateserial','-days','3','-sha256','-extfile',str(ext),'-out',str(d/('node-%d.crt'%n))],'tls-cert-%d'%n)
        run(['openssl','pkcs12','-export','-in',str(d/('node-%d.crt'%n)),'-inkey',str(d/('node-%d.key'%n)),'-certfile',str(ca),'-name','node%d'%n,'-passout','file:'+str(pwfile),'-out',str(d/('node-%d.p12'%n))],'tls-p12-%d'%n)
    def client(name,user,secure_connection=True,passwd=None,hostname='localhost'):
        p=d/(name+'.properties');props={'bootstrap.servers':','.join('%s:%d'%(hostname,x) for x in (secure if secure_connection else legacy)),'default.api.timeout.ms':'15000','request.timeout.ms':'8000'}
        if secure_connection:props.update({'security.protocol':'SASL_SSL','sasl.mechanism':'SCRAM-SHA-512','ssl.truststore.location':str(d/'trust.p12'),'ssl.truststore.type':'PKCS12','ssl.truststore.password':storepass,'ssl.endpoint.identification.algorithm':'HTTPS','sasl.jaas.config':'org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";'%(user,passwd if passwd is not None else creds[user])})
        else:props['security.protocol']='PLAINTEXT'
        p.write_text(''.join('%s=%s\n'%(k,v) for k,v in props.items()));p.chmod(0o600);return p
    plain=client('plain-admin','security-root',False)
    root=client('root','security-root');client('topic-admin','topic-admin');client('producer','producer');client('consumer','consumer')
    client('wrong','producer',passwd=secrets.token_hex(24));client('bad-host','security-root',hostname='127.0.0.1')
    anon=client('anonymous','security-root');anon.write_text(anon.read_text().replace('security.protocol=SASL_SSL','security.protocol=SSL'))
    for role in ['producer','consumer']:
        client('plain-'+role,role,False);shutil.copy2(d/(role+'.properties'),d/('secure-'+role+'.properties'))
    if which=='C':
        profile_data={'profile':profile,'producer_delivery_timeout_ms':120000 if profile=='reference' else 20000,'producer_max_block_ms':60000 if profile=='reference' else 12000,'producer_request_timeout_ms':30000 if profile=='reference' else 8000,'consumer_default_api_timeout_ms':60000 if profile=='reference' else 15000,'commit_timeout_ms':60000 if profile=='reference' else 10000,'acks':'all','idempotence':True,'manual_commit':True}
        save('client-profile.json',profile_data)
        save('lab-quorum-profile.json',{'profile':quorum_profile,'election_timeout_ms':10000 if quorum_profile=='extended' else 1000,'fetch_timeout_ms':20000 if quorum_profile=='extended' else 2000,'request_timeout_ms':20000 if quorum_profile=='extended' else 2000,'operating_recommendation':False})
        if profile=='reference':
            for role in ['producer','consumer']:
                path=d/('plain-'+role+'.properties')
                path.write_text(path.read_text()+('delivery.timeout.ms=120000\nmax.block.ms=60000\nrequest.timeout.ms=30000\n' if role=='producer' else 'default.api.timeout.ms=60000\n'))
    shutil.copy2(plain if which in ['A','C'] else root,d/'monitor.properties')
    def config(n,wave):
        i=nodes.index(n);secured=(which=='B' or wave>=1);ct=(which=='B' or wave>=2);auth=(which=='B' or wave>=3);final=(which=='B' or wave>=5)
        listeners=[];adv=[]
        if not final:listeners.append('OLD://127.0.0.1:%d'%legacy[i]);adv.append('OLD://localhost:%d'%legacy[i])
        if secured:
            listeners.append('NEW://127.0.0.1:%d'%secure[i]);adv.append('NEW://localhost:%d'%secure[i])
            if which!='C':listeners.append('CTRLTLS://localhost:%d'%tlsctrl[i])
        if not final:listeners.append('CTRL://localhost:%d'%ctrl[i])
        first='CTRLTLS' if ct else 'CTRL';controllers=first+(',CTRL' if ct and not final else ',CTRLTLS' if secured and not ct and which!='C' else '')
        common={'process.roles':'broker,controller','node.id':str(n),'controller.quorum.voters':','.join('%d@localhost:%d'%(x,(tlsctrl if ct else ctrl)[j]) for j,x in enumerate(nodes)),
                'listeners':','.join(listeners),'advertised.listeners':','.join(adv),'controller.listener.names':controllers,
                'listener.security.protocol.map':'OLD:PLAINTEXT,NEW:SASL_SSL,CTRL:PLAINTEXT,CTRLTLS:SSL',
                'inter.broker.listener.name':'NEW' if auth else 'OLD','log.dirs':str(d/('data-%d'%n)),
                'offsets.topic.replication.factor':'3','offsets.topic.num.partitions':'50','transaction.state.log.replication.factor':'3','transaction.state.log.min.isr':'2',
                'auto.create.topics.enable':'false','num.network.threads':'2','num.io.threads':'2','num.replica.fetchers':'1','log.cleaner.dedupe.buffer.size':'16777216','log.segment.bytes':'16777216','offsets.topic.segment.bytes':'1048576','controlled.shutdown.enable':'true' if which=='C' else 'false','group.initial.rebalance.delay.ms':'1000'}
        if which=='C' and quorum_profile=='extended':common.update({'controller.quorum.election.timeout.ms':'10000','controller.quorum.fetch.timeout.ms':'20000','controller.quorum.request.timeout.ms':'20000'})
        if secured:common.update({'ssl.keystore.location':str(d/('node-%d.p12'%n)),'ssl.keystore.type':'PKCS12','ssl.keystore.password':storepass,'ssl.key.password':storepass,'ssl.truststore.location':str(d/'trust.p12'),'ssl.truststore.type':'PKCS12','ssl.truststore.password':storepass,'ssl.endpoint.identification.algorithm':'HTTPS','listener.name.ctrltls.ssl.client.auth':'required',
                                'sasl.enabled.mechanisms':'SCRAM-SHA-512','sasl.mechanism.inter.broker.protocol':'SCRAM-SHA-512','listener.name.new.scram-sha-512.sasl.jaas.config':'org.apache.kafka.common.security.scram.ScramLoginModule required username="broker-%d" password="%s";'%(n,creds['broker-%d'%n])})
        if auth:common.update({'authorizer.class.name':'org.apache.kafka.metadata.authorizer.StandardAuthorizer','super.users':';'.join(['User:security-root']+['User:broker-%d'%x for x in nodes]+['User:CN=node%d'%x for x in nodes]),'allow.everyone.if.no.acl.found':'true' if which=='A' and wave==3 else 'false'})
        cfg=d/('node-%d.properties'%n);cfg.write_text(''.join('%s=%s\n'%(k,v) for k,v in common.items()));cfg.chmod(0o600);return cfg
    def start(n):
        cfg=d/('node-%d.properties'%n);logs=d/('logs-%d'%n);logs.mkdir(exist_ok=True)
        f=(d/('broker-%d-%d.log'%(n,len(all_owned)))).open('w');handles.append(f)
        p=subprocess.Popen([str(kh/'bin/kafka-server-start.sh'),str(cfg)],env=dict(env,LOG_DIR=str(logs)),stdout=f,stderr=subprocess.STDOUT,start_new_session=True);owned[n]=p;all_owned.append((p,str(cfg)));event('broker_start',node=n,pid=p.pid)
    def stop(n):
        p=owned[n]
        if p.poll() is None:
            assert str(d/('node-%d.properties'%n)).encode() in Path('/proc/%d/cmdline'%p.pid).read_bytes();p.send_signal(signal.SIGTERM);p.wait(timeout=90 if which=='C' else 45)
        event('broker_stop',node=n,pid=p.pid,exit_code=p.poll())
    def health(label,p,tries=30):
        stable=0;previous=None
        for attempt in range(tries):
            out=d/('%s-%d.json'%(label,attempt))
            with (d/('%s-%d.log'%(label,attempt))).open('w') as f:r=subprocess.run(java+['health',str(p),str(out)],env=env,stdout=f,stderr=subprocess.STDOUT,timeout=35)
            if any(p.poll() is not None for p in owned.values()):raise RuntimeError('테스트 broker 조기 종료: '+label)
            h=json.loads(out.read_text()) if r.returncode==0 else {}
            key=(h.get('quorum_leader'),h.get('quorum_leader_epoch'))
            if h.get('healthy') and h.get('quorum_max_offset_lag',999)<=1:
                stable=stable+1 if key==previous else 1;previous=key
                if stable>=3:event('health_ready',stage=label);return
            else:stable=0;previous=None
            time.sleep(2)
        raise RuntimeError('건전성/ISR 회복 실패: '+label)
    def roll(wave,label,p):
        (d/'phase').write_text(label)
        for n in nodes:
            health(label+'-before-'+str(n),p);stop(n);config(n,wave);start(n);health(label+'-after-'+str(n),p)
    def acl_specs(legacy_allow=False):
        specs=[]
        def add(u,t,name,ops,host='*'):
            for op in ops:specs.append({'principal':'User:'+u,'type':t,'name':name,'operation':op,'host':host})
        add('topic-admin','TOPIC','*',['CREATE','DELETE','ALTER','ALTER_CONFIGS','DESCRIBE','DESCRIBE_CONFIGS']);add('topic-admin','CLUSTER','kafka-cluster',['DESCRIBE'])
        add('producer','TOPIC','app-data',['WRITE','DESCRIBE']);add('producer','TRANSACTIONAL_ID','app-tx',['WRITE','DESCRIBE'])
        add('consumer','TOPIC','app-data',['READ','DESCRIBE']);add('consumer','GROUP','app-group',['READ','DESCRIBE'])
        if legacy_allow:
            add('ANONYMOUS','TOPIC','app-data',['READ','WRITE','DESCRIBE'],'127.0.0.1');add('ANONYMOUS','GROUP','app-group',['READ','DESCRIBE'],'127.0.0.1')
        return specs
    try:
        cid=subprocess.check_output([str(kh/'bin/kafka-storage.sh'),'random-uuid'],env=env,text=True).strip();save('cluster.json',{'cluster_id':cid,'scenario':which,'ports':ports})
        if which=='B':
            run(['bash',str(ROOT/'kafka-security.sh'),'prepare-new','--new-root',str(d/'entry-prepared-empty'),'--apply','--confirm','NEW EMPTY CLUSTER'],'entry-prepare-new')
            (d/'.approved-new-cluster').write_text('new-only\n');(d/'.approved-new-cluster').chmod(0o600)
        for n in nodes:
            cfg=config(n,0 if which in ['A','C'] else 5);data=d/('data-%d'%n)
            assert not data.exists()
            args=['format','--cluster-id',cid,'--config',str(cfg)]
            if which=='B':
                for u,p in creds.items():args+=['--add-scram','SCRAM-SHA-512=[name=%s,iterations=8192,password=%s]'%(u,p)]
            argfile=d/('format-%d.args'%n);argfile.write_text('\n'.join(args)+'\n');argfile.chmod(0o600)
            if which=='B':
                run(['bash',str(ROOT/'kafka-security.sh'),'format-new','--new-root',str(d),'--server-config',str(cfg),'--cluster-id',cid,'--input',str(users),'--apply','--confirm',cid],'format-%d'%n)
            else:run([str(kh/'bin/kafka-storage.sh'),'@'+str(argfile)],'format-%d'%n)
            start(n)
        time.sleep(12)
        active=plain if which in ['A','C'] else root
        health('initial',active);probe('create',active,[],'create-data');
        if which in ['A','C']:probe('users',plain,[users],'bootstrap-credentials-existing')
        else:
            acl=d/'acl-final.json';save(acl.name,acl_specs());probe('acls',root,[acl],'acl-bootstrap')
        if which=='C':
            (d/'phase').write_text('baseline');(d/'clients').write_text('plain')
            f=(d/'stream.log').open('w');handles.append(f)
            stream=subprocess.Popen(java+['stream',str(d)],env=env,stdout=f,stderr=subprocess.STDOUT,start_new_session=True);all_owned.append((stream,'SecurityProbe stream'))
            time.sleep(30);health('offsets-rf3-before-rolling',plain)
            roll(1,'add-secure-client-listener-only',plain)
            probe('coexist-probe',d,[d/'coexist-probe.json'],'secure-shadow-and-negative-probes',timeout=120)
            health('secure-listener-health',root);health('plaintext-still-healthy',plain)
            (d/'phase').write_text('coexist-observation');time.sleep(60)
            (d/'phase').write_text('drain');(d/'stop-send').write_text('stop');stream.wait(timeout=120);assert stream.returncode==0;event('stream_exit',exit_code=stream.returncode)
            probe('resume-check',plain,[d/'resume-check.json'],'plaintext-committed-offset-resume',timeout=90)
            health('final',plain)
            for port in legacy+ctrl+secure:
                with socket.socket() as sock:assert sock.connect_ex(('127.0.0.1',port))==0
            for n in nodes:
                props=dict(line.split('=',1) for line in (d/('node-%d.properties'%n)).read_text().splitlines() if '=' in line)
                assert props['inter.broker.listener.name']=='OLD' and props['controller.listener.names']=='CTRL'
                assert 'authorizer.class.name' not in props and 'OLD://' in props['listeners'] and 'NEW://' in props['listeners']
            save('result.json',{'scenario':'C','completed':True,'scope':'SASL_SSL client listener addition with unchanged PLAINTEXT applications and internal paths','plaintext_retained':True,'authorizer_enabled':False,'internal_paths_unchanged':True,'application_transport_switched':False,'tls_hostname_verification':'HTTPS','drain_completed':True,'committed_offset_resume_verified':True})
            return
        if which=='A':
            (d/'phase').write_text('baseline');(d/'clients').write_text('plain')
            f=(d/'stream.log').open('w');handles.append(f);stream=subprocess.Popen(java+['stream',str(d)],env=env,stdout=f,stderr=subprocess.STDOUT,start_new_session=True);all_owned.append((stream,'SecurityProbe stream'));time.sleep(25);health('offsets-rf3-before-rolling',plain)
            roll(1,'add-dual-listeners',plain)
            roll(2,'controller-dual-to-mtls',plain)
            roll(3,'scram-internal-authorizer-bootstrap',root)
            legacy_acl=d/'acl-with-legacy.json';save(legacy_acl.name,acl_specs(True));probe('acls',root,[legacy_acl],'acl-legacy-and-apps')
            shutil.copy2(root,d/'monitor.properties');roll(4,'remove-default-allow',root)
            (d/'phase').write_text('client-switch');(d/'clients').write_text('secure');event('application_client_config_switch');wait_for_secure_clients(d);event('secure_clients_verified_before_plaintext_removal')
            roll(5,'remove-plaintext-listeners',root)
            old=[x for x in acl_specs(True) if x['principal']=='User:ANONYMOUS'];save('acl-legacy-only.json',old);probe('remove-acls',root,[d/'acl-legacy-only.json'],'remove-anonymous-acl')
            (d/'phase').write_text('after-security');time.sleep(20);(d/'phase').write_text('drain');(d/'stop-send').write_text('stop');stream.wait(timeout=90);assert stream.returncode==0;event('stream_exit',exit_code=stream.returncode)
        health('final-before-matrix',root)
        probe('warm-transaction',root,[],'warm-transaction-coordinator',timeout=90);health('transaction-internal-ready',root)
        probe('warm-consumer',root,[],'warm-offsets-coordinator',timeout=90);health('offsets-internal-ready',root)
        probe('matrix',d,[d/'matrix.json'],'permission-matrix',timeout=360)
        if which=='B':
            old=d/'producer.properties';shutil.copy2(old,d/'old-producer.properties');creds['producer']=secrets.token_hex(24);(d/'rotate.properties').write_text('producer='+creds['producer']+'\n');(d/'rotate.properties').chmod(0o600)
            run(['bash',str(ROOT/'kafka-security.sh'),'issue-users','--client',str(root),'--input',str(d/'rotate.properties'),'--apply','--confirm',cid],'rotate-producer');client('producer','producer')
            probe('send',d/'old-producer.properties',[],'old-password-denied',expected=1,timeout=40);probe('send',d/'producer.properties',[],'new-password-allowed')
            roll(5,'restart-persistence',root);probe('send',d/'producer.properties',[],'after-restart-new-password');probe('matrix',d,[d/'matrix-after-restart.json'],'matrix-after-restart',timeout=360)
        health('final',root)
        run(['bash',str(ROOT/'kafka-security.sh'),'inspect','--client',str(root),'--output',str(d/'entry-inspect.json')],'entry-inspect')
        run(['bash',str(ROOT/'kafka-security.sh'),'issue-users','--client',str(root),'--input',str(users)],'entry-users-dry-run')
        final_acl=d/'entry-acl.json';save(final_acl.name,acl_specs())
        run(['bash',str(ROOT/'kafka-security.sh'),'apply-acls','--client',str(root),'--input',str(final_acl),'--apply','--confirm',cid],'entry-apply-acls')
        probe('audit',root,[d/'audit.json'],'acl-and-user-audit')
        for port in legacy+ctrl:
            with socket.socket() as s:assert s.connect_ex(('127.0.0.1',port))!=0
        final_configs=[]
        for n in nodes:
            p=d/('node-%d.properties'%n);props=dict(line.strip().split('=',1) for line in p.read_text().splitlines() if '=' in line)
            assert props['allow.everyone.if.no.acl.found']=='false' and 'ANONYMOUS' not in props['super.users'] and 'OLD://' not in props['listeners'] and 'CTRL://' not in props['listeners']
            final_configs.append({k:v for k,v in props.items() if k in ['node.id','listeners','advertised.listeners','controller.quorum.voters','controller.listener.names','inter.broker.listener.name','authorizer.class.name','super.users','allow.everyone.if.no.acl.found','ssl.endpoint.identification.algorithm','listener.name.ctrltls.ssl.client.auth','auto.create.topics.enable','sasl.enabled.mechanisms','sasl.mechanism.inter.broker.protocol']})
        save('final-configs-redacted.json',final_configs);save('result.json',{'scenario':which,'completed':True,'legacy_client_and_controller_ports_closed':True,'tls_hostname_verification':'HTTPS','tls_sasl_scram_sha512':True,'matrix_passed':True})
    finally:
        if (d/'events.jsonl').exists():(d/'stop-send').write_text('stop')
        if which=='C':
            for p,needle in reversed(all_owned):
                if p.poll() is None:
                    assert needle.encode() in Path('/proc/%d/cmdline'%p.pid).read_bytes().replace(b'\0',b' ')
                    p.send_signal(signal.SIGTERM)
        for p,needle in reversed(all_owned):
            if p.poll() is None:
                assert needle.encode() in Path('/proc/%d/cmdline'%p.pid).read_bytes().replace(b'\0',b' ')
                p.send_signal(signal.SIGTERM)
                try:p.wait(timeout=90 if which=='C' else 45)
                except subprocess.TimeoutExpired:event('normal_shutdown_pending',pid=p.pid)
        for f in handles:f.close()
        save('shutdown.json',[{'pid':p.pid,'command_contains':Path(needle).name,'exit_code':p.poll()} for p,needle in all_owned])
        for path in d.rglob('*'):
            if not path.is_symlink():path.chmod(0o700 if path.is_dir() else 0o600)
        assert all(p.poll() is not None for p,_ in all_owned)

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--scenario',choices=['A','B','C'],required=True);ap.add_argument('--directory',type=Path,required=True);ap.add_argument('--port-base',type=int,default=16000);ap.add_argument('--client-profile',choices=['short','reference'],default='short');ap.add_argument('--lab-quorum-timeouts',choices=['default','extended'],default='default');a=ap.parse_args()
    os.umask(0o077)
    assert int(next(x for x in Path('/proc/meminfo').read_text().splitlines() if x.startswith('MemAvailable:')).split()[1])>=2500000
    assert shutil.disk_usage(ROOT).free>5*1024**3
    kh=Path(os.environ['KAFKA_HOME']);jh=Path(os.environ['JAVA_HOME']);assert (kh/'libs/kafka-clients-3.9.1.jar').exists()
    (ROOT/'build').mkdir(exist_ok=True);subprocess.run([str(jh/'bin/javac'),'-cp',str(kh/'libs/*'),'-d',str(ROOT/'build'),str(ROOT/'tests/SecurityProbe.java')],check=True)
    run_lab(a.scenario,a.directory.resolve(),a.port_base,kh,jh,a.client_profile,a.lab_quorum_timeouts)

if __name__=='__main__':
    if not __debug__:raise RuntimeError('python -O 금지')
    main()
