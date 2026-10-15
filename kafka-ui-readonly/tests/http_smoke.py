#!/usr/bin/env python3
"""Start only the supplied test JAR on loopback, test HTTP, then terminate that PID."""
import argparse
import http.cookiejar
import json
from pathlib import Path
import secrets
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

p=argparse.ArgumentParser()
p.add_argument('--jar',required=True,type=Path)
p.add_argument('--java-home',required=True,type=Path)
p.add_argument('--work',required=True,type=Path)
a=p.parse_args()
a.work.mkdir(parents=True,exist_ok=False)
a.work.chmod(0o700)
with socket.socket() as sock:
    sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
password=secrets.token_urlsafe(32)
config={'server':{'address':'127.0.0.1','port':port},
        'ui':{'read-only':True},
        'spring':{'security':{'user':{'name':'readonly-test','password':password}}},
        'auth':{'type':'LOGIN_FORM'},'kafka':{'clusters':[]},
        'dynamic':{'config':{'enabled':False}}}
cfg=a.work/'test-config.yml';cfg.write_text(json.dumps(config));cfg.chmod(0o600)
command=[str(a.java_home/'bin/java'),'-Xms128m','-Xmx384m',
         '-Dspring.config.additional-location=file:'+str(cfg.resolve()),
         '-jar',str(a.jar.resolve())]
base='http://127.0.0.1:'+str(port)
summary={'kafka_connected':False,'existing_services_changed':False}
log=(a.work/'ui.log').open('w')
proc=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT)
try:
    deadline=time.monotonic()+120
    while True:
        if proc.poll() is not None:raise RuntimeError('Test UI exited before ready')
        try:
            with urllib.request.urlopen(base+'/auth',timeout=2) as response:
                if response.status==200:break
        except (OSError,urllib.error.URLError):pass
        if time.monotonic()>deadline:raise RuntimeError('Test UI readiness timeout')
        time.sleep(1)
    jar=http.cookiejar.CookieJar()
    opener=urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    with opener.open(base+'/api/clusters') as response:
        assert '/auth' in response.url
    summary['anonymous_login_required']=True
    bad=urllib.parse.urlencode({'username':'readonly-test','password':'wrong-test-value'}).encode()
    with opener.open(base+'/auth',bad) as response:
        assert 'error' in response.url
    summary['wrong_password_rejected']=True
    body=urllib.parse.urlencode({'username':'readonly-test','password':password}).encode()
    opener.open(base+'/auth',body).close()
    with opener.open(base+'/api/clusters') as response:
        assert response.status==200
        assert json.load(response)==[]
    summary['authenticated_read_status']=200
    with opener.open(base+'/api/readonly-policy') as response:
        assert json.load(response)=={'readOnly':True}
    summary['readonly_policy_status']=200
    attempts=[('PUT','/api/clusters/test/brokers/1/configs/retention.ms'),
              ('POST','/api/clusters/test/topics'),
              ('DELETE','/api/clusters/test/topics/example'),
              ('PATCH','/api/clusters/test/topics/example/partitions'),
              ('POST','/api/clusters/test/consumer-groups/example/offsets'),
              ('PUT','/api/config')]
    for method,path in attempts:
        request=urllib.request.Request(base+path,data=b'{}',method=method,
                                       headers={'Content-Type':'application/json'})
        try:
            opener.open(request).close()
            raise AssertionError('Write request escaped: '+method+' '+path)
        except urllib.error.HTTPError as response:
            assert response.code==403
            assert json.load(response)['code']=='UI_READ_ONLY'
    summary['write_requests_forbidden']=len(attempts)
    with opener.open(base+'/') as response:
        html=response.read().decode()
        assert response.status==200 and '<html' in html
    summary['frontend_index_status']=200
finally:
    if proc.poll() is None:
        # Popen owns this exact PID; never locate or signal existing services.
        proc.terminate()
        try:proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            raise RuntimeError('Owned UI did not terminate; PID='+str(proc.pid))
    log.close()
    summary['owned_test_ui_stopped']=proc.poll() is not None
    with socket.socket() as sock:
        summary['test_port_closed']=sock.connect_ex(('127.0.0.1',port))!=0
    (a.work/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,indent=2))
