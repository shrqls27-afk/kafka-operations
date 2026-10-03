import copy
import importlib.util
import pathlib
import unittest

path=pathlib.Path(__file__).resolve().parents[1]/'scripts/reassign.py'
spec=importlib.util.spec_from_file_location('reassign',path)
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

class PlanSafety(unittest.TestCase):
    def setUp(self):
        self.before={'cluster_id':'test','brokers':[7,19,42], 'partitions':[{'partition':0,'replicas':[19],'isr':[19],'leader':19}], 'reassignments':'{}','configs':{}}
        self.target={'version':1,'partitions':[{'topic':'__consumer_offsets','partition':0,'replicas':[19,7,42]}]}
        self.current=copy.deepcopy(self.before)
    def test_preserves_actual_original_replica(self):
        module.validate(self.before,self.target,self.current)
    def test_rejects_changed_cluster(self):
        self.current['cluster_id']='another'
        with self.assertRaises(AssertionError):module.validate(self.before,self.target,self.current)
    def test_rejects_missing_original_replica(self):
        self.target['partitions'][0]['replicas']=[7,19,42]
        with self.assertRaises(AssertionError):module.validate(self.before,self.target,self.current)
    def test_rejects_missing_partition(self):
        self.target['partitions']=[]
        with self.assertRaises(AssertionError):module.validate(self.before,self.target,self.current)
    def test_rejects_under_replicated_original(self):
        self.current['partitions'][0]['isr']=[]
        with self.assertRaises(AssertionError):module.validate(self.before,self.target,self.current)
    def test_rejects_new_throttle(self):
        self.current['configs']={'broker':{'leader.replication.throttled.rate':{'value':'10000','source':'DYNAMIC_BROKER_CONFIG'}}}
        with self.assertRaises(AssertionError):module.validate(self.before,self.target,self.current)
    def test_rejects_concurrent_reassignment(self):
        self.current['reassignments']='{other=active}'
        with self.assertRaises(AssertionError):module.validate(self.before,self.target,self.current)
    def test_rejects_duplicate_broker(self):
        self.target['partitions'][0]['replicas']=[19,7,7]
        with self.assertRaises(AssertionError):module.validate(self.before,self.target,self.current)
    def test_complete_gate_accepts_full_isr(self):
        self.current['partitions'][0].update(replicas=[19,7,42],isr=[7,42,19])
        module.validate_complete(self.before,self.target,self.current)
    def test_complete_gate_rejects_incomplete_isr(self):
        self.current['partitions'][0].update(replicas=[19,7,42],isr=[19,7])
        with self.assertRaises(AssertionError):module.validate_complete(self.before,self.target,self.current)
    def test_complete_gate_rejects_wrong_cluster(self):
        self.current['cluster_id']='other'
        self.current['partitions'][0].update(replicas=[19,7,42],isr=[19,7,42])
        with self.assertRaises(AssertionError):module.validate_complete(self.before,self.target,self.current)

if __name__=='__main__':unittest.main()
