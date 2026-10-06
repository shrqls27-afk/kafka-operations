import sys
from pathlib import Path
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
import company

class QuorumGate(unittest.TestCase):
    def setUp(self):
        self.now=0
        self.waits=[]
    def sleep(self,seconds):
        self.waits.append(seconds)
        self.now+=seconds
    def status(self,leader=203,epoch=4,cid='test',hwm=212):
        return 'ClusterId: %s\nLeaderId: %d\nLeaderEpoch: %d\nHighWatermark: %d\nCurrentVoters: [201,202,203]\n'%(cid,leader,epoch,hwm)
    def replication(self,leader=203,lag=0):
        return 'NodeId LogEndOffset Lag Status\n'+''.join('%d %d %d %s\n'%(i,216 if i==leader else 216-lag,0 if i==leader else lag,'Leader' if i==leader else 'Follower') for i in [201,202,203])
    def gate(self,outputs,timeout=10):
        it=iter(outputs)
        def fetch(kind,remaining):
            self.assertGreater(remaining,0)
            return next(it)
        return company.stable_quorum_gate(fetch,timeout=timeout,interval=2,clock=lambda:self.now,sleep=self.sleep)
    def observation(self,leader=203,epoch=4):
        return [self.status(leader,epoch),self.replication(leader),self.status(leader,epoch)]
    def test_two_stable_observations_require_interval(self):
        self.assertEqual(self.gate(self.observation()*2),'test')
        self.assertEqual(self.waits,[2])
    def test_transient_lag_retries_then_two_healthy(self):
        self.assertEqual(self.gate([self.status(),self.replication(lag=1)]+self.observation()*2),'test')
        self.assertEqual(self.waits,[2,2])
    def test_status_replication_leader_transition_retries(self):
        bad=[self.status(),self.replication(202),self.status(202,5)]
        self.assertEqual(self.gate(bad+self.observation(202,5)*2),'test')
        self.assertEqual(self.waits,[2,2])
    def test_epoch_change_resets_consecutive_observations(self):
        self.assertEqual(self.gate(self.observation()+self.observation(epoch=5)*2),'test')
        self.assertEqual(self.waits,[2,2])
    def test_persistent_lag_times_out_closed(self):
        with self.assertRaisesRegex(RuntimeError,'시간 초과'):
            self.gate([self.status(),self.replication(lag=1)]*3,timeout=5)
        self.assertEqual(self.now,5)
    def test_bad_observation_resets_prior_success(self):
        with self.assertRaisesRegex(RuntimeError,'시간 초과'):
            self.gate(self.observation()+[self.status(),self.replication(lag=1)]+self.observation(),timeout=6)
    def test_cluster_change_blocks_immediately(self):
        with self.assertRaisesRegex(ValueError,'cluster ID 변경'):
            self.gate(self.observation()+[self.status(cid='other')])
    def test_missing_epoch_cannot_pass(self):
        with self.assertRaises(RuntimeError):
            company.quorum_status(self.status().replace('LeaderEpoch: 4\n',''))
