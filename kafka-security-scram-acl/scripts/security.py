#!/usr/bin/env python3
"""기본 조회/계획, 운영 계정/ACL 변경과 신규 format을 명확히 분리한다."""
import argparse, json, os, re, stat, subprocess, sys, tempfile
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]

def properties(p):
    return dict(line.strip().split('=',1) for line in p.read_text().splitlines() if line.strip() and not line.lstrip().startswith('#') and '=' in line)

def private_file(p):
    if p.is_symlink() or not p.is_file() or stat.S_IMODE(p.stat().st_mode)&0o077:raise ValueError('비밀 파일은 symlink 아닌 600/400 권한 일반 파일이어야 함')

def check_new(root,config):
    root=root.resolve();c=properties(config)
    if not (root/'.approved-new-cluster').is_file():raise ValueError('prepare-new 승인 marker 필요')
    if c.get('authorizer.class.name')!='org.apache.kafka.metadata.authorizer.StandardAuthorizer' or c.get('allow.everyone.if.no.acl.found')!='false':raise ValueError('신규는 StandardAuthorizer와 default deny 필요')
    if 'ANONYMOUS' in c.get('super.users',''):raise ValueError('anonymous superuser 금지')
    paths=c.get('log.dirs','').split(',')+[c.get('metadata.log.dir','')]
    if not c.get('log.dirs'):raise ValueError('명시적 전용 log.dirs 필요')
    for name in filter(None,paths):
        raw=Path(name)
        if not raw.is_absolute():raise ValueError('신규 데이터는 절대경로 필요')
        path=raw.resolve()
        if root not in path.parents or path==root:raise ValueError('데이터는 승인된 새 root의 하위 전용 경로만 허용')
        if path.exists() and (not path.is_dir() or any(path.iterdir())):raise ValueError('기존/비어 있지 않은 데이터 경로 format 거부')
    return c

def main():
    if sys.version_info<(3,8):raise ValueError('Python3.8 이상 필요')
    ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('action',choices=['inspect','audit','issue-users','apply-acls','prepare-new','format-new'])
    ap.add_argument('--client',type=Path);ap.add_argument('--input',type=Path);ap.add_argument('--output',type=Path)
    ap.add_argument('--new-root',type=Path);ap.add_argument('--server-config',type=Path);ap.add_argument('--cluster-id');ap.add_argument('--apply',action='store_true');ap.add_argument('--confirm')
    a=ap.parse_args();os.umask(0o077)
    kh=Path(os.environ['KAFKA_HOME']);jh=Path(os.environ['JAVA_HOME'])
    if not (kh/'libs/kafka-clients-3.9.1.jar').is_file():raise ValueError('Kafka3.9.1 배포본 필요')
    version=subprocess.check_output([str(jh/'bin/javac'),'-version'],stderr=subprocess.STDOUT,text=True)
    if not version.startswith('javac 17.'):raise ValueError('JDK17/javac 필요')
    launcher=['bash',str(ROOT/'scripts/java.sh')]
    def call(mode,p,out):subprocess.run(launcher+[mode,str(p),str(out)],check=True)
    if a.action=='prepare-new':
        if not a.new_root or a.new_root.exists():raise ValueError('아직 존재하지 않는 새 전용 root만 허용')
        print('신규 전용 root 생성 계획. 기존 클러스터에 사용 금지.')
        if a.apply:
            if a.confirm!='NEW EMPTY CLUSTER':raise ValueError('--confirm "NEW EMPTY CLUSTER" 필요')
            a.new_root.mkdir(parents=True,mode=0o700);a.new_root.chmod(0o700);(a.new_root/'.approved-new-cluster').write_text('new-only\n');(a.new_root/'.approved-new-cluster').chmod(0o600)
        return
    if a.action=='format-new':
        if not a.new_root or not a.server_config or not a.cluster_id:raise ValueError('new-root/server-config/cluster-id 필요')
        check_new(a.new_root,a.server_config);private_file(a.server_config)
        if not a.input:raise ValueError('신규 최초 SCRAM 계정의 보호된 --input 파일 필요')
        private_file(a.input)
        if not properties(a.input):raise ValueError('최초 SCRAM 계정 파일이 비어 있음')
        print('신규 비어 있는 경로 format 계획. 기존 데이터/ignore-formatted 사용 금지.')
        if not a.apply:return
        if a.confirm!=a.cluster_id:raise ValueError('검토한 신규 cluster ID로 --confirm 필요')
        check_new(a.new_root,a.server_config)
        values=['format','--cluster-id',a.cluster_id,'--config',str(a.server_config.resolve())]
        if a.input:
            for u,p in properties(a.input).items():
                if not re.fullmatch(r'[A-Za-z0-9._-]+',u) or not re.fullmatch(r'[A-Za-z0-9._~!@%+=:-]{16,}',p):raise ValueError('반입 SCRAM user/password 형식 검토 필요; 비밀 출력 없음')
                values+=['--add-scram','SCRAM-SHA-512=[name=%s,iterations=8192,password=%s]'%(u,p)]
        argfile=a.new_root/('format-private-%s.args'%properties(a.server_config).get('node.id','node'))
        if argfile.exists():raise ValueError('기존 format 인수 파일 존재: 상태 수동 확인')
        argfile.write_text('\n'.join(values)+'\n');argfile.chmod(0o600)
        subprocess.run(['bash',str(kh/'bin/kafka-storage.sh'),'@'+str(argfile.resolve())],check=True)
        return
    if not a.client:raise ValueError('--client 보호된 접속 properties 필요')
    private_file(a.client)
    if a.action in ('inspect','audit'):
        if not a.output or a.output.exists():raise ValueError('아직 없는 --output 경로 필요')
        call('health' if a.action=='inspect' else 'audit',a.client,a.output);return
    if not a.input:raise ValueError('--input 파일 필요')
    if a.action=='issue-users':private_file(a.input);print('계정 발급/회전 계획:',', '.join(properties(a.input).keys()))
    else:
        spec=json.loads(a.input.read_text());print('ACL 계획:',json.dumps(spec,ensure_ascii=False))
        if any(x.get('principal')=='User:ANONYMOUS' for x in spec):raise ValueError('최종 ACL 진입점은 anonymous 권한을 허용하지 않음; legacy 단계는 별도 검토')
    with tempfile.TemporaryDirectory(prefix='security-inspect-') as temporary:
        os.chmod(temporary,0o700);out=Path(temporary)/'health.json';call('health',a.client,out);snapshot=json.loads(out.read_text())
        print('대상 cluster ID:',snapshot['cluster_id'],'brokers:',snapshot['brokers'],'healthy:',snapshot['healthy'])
        if not a.apply:return
        if a.confirm!=snapshot['cluster_id'] or not snapshot['healthy']:raise ValueError('건전성 및 대상 cluster ID 검토/확인 필요')
        call('users' if a.action=='issue-users' else 'acls',a.client,a.input)

if __name__=='__main__':
    try:
        if not __debug__:raise ValueError('python -O 금지')
        main()
    except Exception as e:
        print('작업 보류:',str(e) if isinstance(e,ValueError) else type(e).__name__,file=sys.stderr);sys.exit(1)
