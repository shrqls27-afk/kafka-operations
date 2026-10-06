import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'scripts'))
import company
original_gate = company.stable_quorum_gate


class Workflow(unittest.TestCase):
    def setUp(self):
        base=ROOT/'runtime/test-company'
        base.mkdir(parents=True, exist_ok=True)
        self.home=Path(tempfile.mkdtemp(dir=base))
        self.before={'cluster_id':'company-test','brokers':[11,22,33], 'reassignments':'{}', 'configs':{'ConfigResource(type=TOPIC, name=__consumer_offsets)':{}, **{'ConfigResource(type=BROKER, name=%d)'%b:{} for b in [11,22,33]}},
                     'partitions':[{'partition':0,'replicas':[22],'isr':[22],'leader':22}]}
        self.target={'version':1,'partitions':[{'topic':'__consumer_offsets','partition':0,'replicas':[22,11,33]}]}
        self.applies=0
        self.fail_execute=False
        self.current=copy.deepcopy(self.before)

    def run_command(self, cmd, **kwargs):
        if len(cmd)>2 and cmd[1].endswith('reassign.py'):
            action=cmd[2];directory=Path(cmd[cmd.index('--directory')+1])
            if action=='plan':
                directory.mkdir()
                (directory/'before.json').write_text(json.dumps(self.before))
                (directory/'target.json').write_text(json.dumps(self.target))
            elif action=='execute' and '--apply' in cmd:
                self.applies+=1
                if self.fail_execute:return subprocess.CompletedProcess(cmd,1,'timeout')
                self.current['partitions'][0].update(replicas=[22,11,33],isr=[22,11,33])
            elif action=='snapshot':directory.write_text(json.dumps(self.current))
        output=''
        if 'kafka-metadata-quorum.sh' in str(cmd):
            output=('ClusterId: company-test\nLeaderId: 1\nLeaderEpoch: 4\nHighWatermark: 5\nCurrentVoters: [1, 2, 3]\n' if '--status' in cmd else 'NodeId LogEndOffset Lag Status\n1 5 0 Leader\n2 5 0 Follower\n3 5 0 Follower\n')
        return subprocess.CompletedProcess(cmd,0,output)

    def invoke(self, answers, resume=None):
        args=['company.py']+(['--resume',str(resume)] if resume else [])
        with patch.object(company.Path,'home',return_value=self.home), \
             patch.object(company,'stable_quorum_gate',side_effect=lambda fetch, **kw: original_gate(fetch, sleep=lambda _:None, **kw)), \
             patch.object(company,'prerequisites',return_value=self.home/'kafka/current'), \
             patch.object(company,'ask',side_effect=answers), \
             patch.object(company.subprocess,'run',side_effect=self.run_command), \
             patch.object(sys,'argv',args):
            company.main()

    def state(self):
        path=next((self.home/'kafka-rf3-work').glob('run-*/state.json'))
        return path,json.loads(path.read_text())

    def test_success_and_resume_never_repeat_execute(self):
        self.invoke(['host:9092','-','','YES','1048576','YES','YES'])
        p,state=self.state();self.assertEqual(state['phase'],'done');self.assertEqual(self.applies,1)
        self.invoke([],p.parent)
        self.assertEqual(self.applies,1)

    def test_decline_before_execute(self):
        with self.assertRaises(RuntimeError):self.invoke(['host:9092','-','','NO'])
        self.assertEqual(self.applies,0)

    def test_uncertain_execute_is_saved_and_resume_does_not_retry(self):
        self.fail_execute=True
        with self.assertRaises(RuntimeError):self.invoke(['host:9092','-','','YES','1000','YES'])
        p,state=self.state();self.assertEqual(state['phase'],'uncertain')
        with self.assertRaises(RuntimeError):self.invoke([],p.parent)
        self.assertEqual(self.applies,1)

    def test_tampered_plan_blocks_resume(self):
        self.invoke(['host:9092','-','','YES','1000','YES','YES'])
        p,_=self.state();(p.parent/'plan/target.json').write_text('{}')
        with self.assertRaisesRegex(RuntimeError,'해시'):self.invoke([],p.parent)
        self.assertEqual(self.applies,1)
