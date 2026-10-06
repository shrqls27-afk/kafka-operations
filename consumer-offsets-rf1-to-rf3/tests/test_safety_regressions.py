import copy
import os
from pathlib import Path
import subprocess
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'scripts'))
import company
import reassign

class SafetyRegressions(unittest.TestCase):
    def test_nas_without_explicit_address_fails_closed(self):
        import verify_standalone
        with self.assertRaises(RuntimeError):
            verify_standalone.existing_bootstrap('nas', None)
    def test_nas_address_is_supplied_at_runtime(self):
        import verify_standalone
        value = 'broker-a:9092,broker-b:9092,broker-c:9092'
        self.assertEqual(verify_standalone.existing_bootstrap('nas', value), value)
    def test_local_needs_no_permanent_cluster_address(self):
        import verify_standalone
        self.assertIsNone(verify_standalone.existing_bootstrap('local', None))
    def setUp(self):
        self.before={'configs':{'ConfigResource(type=TOPIC, name=__consumer_offsets)':{},
            **{'ConfigResource(type=BROKER, name=%d)'%i:{} for i in [1,2,3]}},
            'partitions':[{'partition':0,'replicas':[1]}]}
        self.target={'partitions':[{'partition':0,'replicas':[1,2,3]}]}
        self.live={'configs':reassign.expected_configs(self.before,self.target,1000)}
    def test_exact_owned_throttle(self):
        reassign.validate_throttle_values(self.before,self.target,self.live,1000)
    def test_other_admin_rate_blocks_clear(self):
        self.live['configs']['ConfigResource(type=BROKER, name=2)']['leader.replication.throttled.rate']['value']='999'
        with self.assertRaises(RuntimeError):reassign.validate_throttle_values(self.before,self.target,self.live,1000)
    def test_other_admin_topic_blocks_uncertain_resume(self):
        self.live['configs']['ConfigResource(type=TOPIC, name=__consumer_offsets)']['follower.replication.throttled.replicas']['value']='*'
        with self.assertRaises(RuntimeError):reassign.validate_throttle_values(self.before,self.target,self.live,1000,True)
    def test_source_change_blocks_clear(self):
        self.live['configs']['ConfigResource(type=BROKER, name=1)']['leader.replication.throttled.rate']['source']='STATIC_BROKER_CONFIG'
        with self.assertRaises(RuntimeError):reassign.validate_throttle_values(self.before,self.target,self.live,1000)
    def test_replica_order_is_semantic(self):
        resource='ConfigResource(type=TOPIC, name=__consumer_offsets)'
        self.live['configs'][resource]['follower.replication.throttled.replicas']['value']='0:3,0:2'
        reassign.validate_throttle_values(self.before,self.target,self.live,1000)
    def test_duplicate_replica_rejected(self):
        resource='ConfigResource(type=TOPIC, name=__consumer_offsets)'
        self.live['configs'][resource]['follower.replication.throttled.replicas']['value']='0:2,0:3,0:2'
        with self.assertRaises(RuntimeError):reassign.validate_throttle_values(self.before,self.target,self.live,1000)
    def test_both_rates_on_all_three_brokers(self):
        for i in [1,2,3]:
            values=self.live['configs']['ConfigResource(type=BROKER, name=%d)'%i]
            for role in ['leader','follower']:
                self.assertEqual(values[role+'.replication.throttled.rate']['value'],'1000')
    def test_clearing_accepts_only_original_or_expected(self):
        resource='ConfigResource(type=BROKER, name=1)'
        key='leader.replication.throttled.rate'
        original={'value':'9223372036854775807','source':'DEFAULT_CONFIG'}
        self.before['configs'][resource][key]=original
        self.live['configs'][resource][key]=original
        reassign.validate_throttle_values(self.before,self.target,self.live,1000,True)
        with self.assertRaises(RuntimeError):reassign.validate_throttle_values(self.before,self.target,self.live,1000,False)
    def test_local_environment_is_explicit(self):
        import verify_standalone
        from unittest.mock import patch
        with patch.object(verify_standalone.Path,'iterdir',return_value=[]):
            self.assertEqual(verify_standalone.existing_services('local'),{})
            with self.assertRaises(AssertionError):verify_standalone.existing_services('nas')
    def verify_helper(self, changed=False, allow_cleared=False):
        import json
        import tempfile
        from unittest.mock import patch
        d=Path(tempfile.mkdtemp(dir=ROOT/'runtime/test-company'))
        before=dict(self.before,cluster_id='test',brokers=[1,2,3],reassignments='{}',
            partitions=[{'partition':0,'replicas':[1],'isr':[1],'leader':1}])
        target=dict(self.target,version=1)
        target['partitions'][0]['topic']='__consumer_offsets'
        live=dict(self.live,cluster_id='test',brokers=[1,2,3],reassignments='{}',
            partitions=[{'partition':0,'replicas':[1,2,3],'isr':[1,2,3],'leader':1}])
        expected=reassign.expected_configs(before,target,1000)
        for name,data in [('before.json',before),('target.json',target),('expected-throttle.json',expected)]:
            (d/name).write_text(json.dumps(data))
        if changed:live['configs']['ConfigResource(type=BROKER, name=1)']['follower.replication.throttled.rate']['value']='999'
        commands=[]
        def fake_run(cmd):
            commands.append(cmd)
            if 'snapshot' in cmd:Path(cmd[-1]).write_text(json.dumps(live))
        args=['reassign.py','verify','--bootstrap','unused:9092','--directory',str(d),'--apply','--throttle','1000']
        if allow_cleared:args.append('--allow-cleared')
        with patch.object(sys,'argv',args),patch.dict(os.environ,{'KAFKA_HOME':'/unused'}),patch.object(reassign,'run',side_effect=fake_run):
            if changed:
                with self.assertRaises(RuntimeError):reassign.main()
            else:reassign.main()
        return commands
    def test_verify_apply_reaches_cli_only_after_value_match(self):
        commands=self.verify_helper()
        self.assertEqual(sum('--verify' in c for c in commands),1)
    def test_verify_apply_changed_rate_never_calls_clear_cli(self):
        self.assertEqual(sum('--verify' in c for c in self.verify_helper(True)),0)
    def test_clearing_resume_changed_rate_never_calls_clear_cli(self):
        self.assertEqual(sum('--verify' in c for c in self.verify_helper(True,True)),0)
    def test_quorum_negative_leader(self):
        with self.assertRaises(RuntimeError):company.validate_quorum_status('LeaderId: -1\nHighWatermark: 0\n')
    def test_quorum_negative_watermark(self):
        with self.assertRaises(RuntimeError):company.validate_quorum_status('LeaderId: 1\nHighWatermark: -1\nCurrentVoters: [1,2,3]\nClusterId: x\n')
    def test_quorum_replication_lag(self):
        with self.assertRaises(RuntimeError):company.validate_quorum_replication('NodeId LogEndOffset Lag Status\n1 5 0 Leader\n2 4 1 Follower\n3 5 0 Follower')
    def test_optimized_import_still_validates(self):
        code="import reassign; reassign.validate({'cluster_id':'a'}, {}, {'cluster_id':'b'})"
        p=subprocess.run([sys.executable,'-O','-c',code],env=dict(os.environ,PYTHONPATH=str(ROOT/'scripts')),capture_output=True,text=True)
        self.assertNotEqual(p.returncode,0)
        self.assertIn('AssertionError',p.stderr)
    def test_format_all_before_launch_and_health_before_format(self):
        import ast
        source=(ROOT/'scripts/verify_standalone.py').read_text()
        ast.parse(source)
        self.assertLess(source.index("'health'"),source.index("'format'"))
        self.assertLess(source.index('configs.append'),source.index('for n, cfg, logs in configs:'))
        self.assertNotIn('time.sleep(10)',source)
    def test_sampler_not_on_consumer_poll_thread(self):
        source=(ROOT/'tests/OffsetsLab.java').read_text()
        self.assertIn('scheduleWithFixedDelay',source)
        poll=source[source.index('var records=c.poll'):source.index('if(done.get()')]
        self.assertNotIn('snapshot(',poll)

    def test_warmup_and_intentional_sample_stop_are_separate(self):
        java=(ROOT/'tests/OffsetsLab.java').read_text()
        verify=(ROOT/'scripts/verify_standalone.py').read_text()
        self.assertIn('if(!Files.exists(dir.resolve("warmup-ready")))return',java)
        self.assertIn('stoppingSamples.get()?"sample_cancelled":"sample_error"',java)
        self.assertLess(verify.index("while not (d/'warmup-ready').exists()"),verify.index("phase('baseline')"))
    def test_during_starts_at_actual_execute_confirmation(self):
        source=(ROOT/'scripts/verify_standalone.py').read_text()
        self.assertIn("if count == 2 and not decline:\n                        phase('during')",source)
        self.assertIn("save('throttled.json',current)\n                        phase('after')",source)
    def test_compile_cache_uses_source_hash(self):
        source=(ROOT/'scripts/java.sh').read_text()
        self.assertIn('hashlib.sha256()',source)
        self.assertIn('[[ ! -f "$CACHE/compiled" ]]',source)
        self.assertIn('flock 9',source)
