import unittest
from gates import replication_rows, voters_caught_up, SequentialGate
SAMPLE='NodeId DirectoryId LogEndOffset Lag LastFetchTimestamp LastCaughtUpTimestamp Status\n2 A 590787 0 100 100 Leader\n1 A 590787 0 100 100 Follower\n3 A 590787 0 100 100 Follower\n'
class GatesTests(unittest.TestCase):
    def test_log_end_is_not_lag(self):
        self.assertEqual(replication_rows(SAMPLE)[0]['Lag'],0)
        self.assertTrue(voters_caught_up(SAMPLE,{1,2,3}))
    def test_reordered_header(self):
        self.assertEqual(replication_rows('Lag Status NodeId LogEndOffset\n0 Leader 2 99\n')[0]['Lag'],0)
    def test_transient_lag_not_healthy(self):
        lag=SAMPLE.replace('1 A 590787 0','1 A 590786 1')
        self.assertFalse(voters_caught_up(lag,{1,2,3}))
        self.assertTrue(voters_caught_up(SAMPLE,{1,2,3}))
    def test_missing_header(self):
        with self.assertRaises(ValueError): replication_rows('NodeId Status\n1 Leader\n')
    def test_missing_live_voter(self):
        self.assertFalse(voters_caught_up(SAMPLE,{1,2,3,4}))
    def test_down_voter_excluded_but_two_live_required(self):
        lag=SAMPLE.replace('3 A 590787 0','3 A 590780 7')
        self.assertTrue(voters_caught_up(lag,{1,2}))
        self.assertFalse(voters_caught_up(lag,{1,3}))
    def test_second_stop_blocked(self):
        gate=SequentialGate(); gate.stop(1,True)
        with self.assertRaises(RuntimeError): gate.stop(2,True)
    def test_bad_recovery_blocked(self):
        gate=SequentialGate(); gate.stop(1,True)
        with self.assertRaises(RuntimeError): gate.recovered(1,False)
        with self.assertRaises(RuntimeError): gate.stop(2,True)
    def test_wrong_node_recovery(self):
        gate=SequentialGate(); gate.stop(1,True)
        with self.assertRaises(RuntimeError): gate.recovered(2,True)
    def test_recovered_allows_next(self):
        gate=SequentialGate(); gate.stop(1,True); gate.recovered(1,True); gate.stop(2,True)
    def test_unhealthy_initial_stop(self):
        with self.assertRaises(RuntimeError): SequentialGate().stop(1,False)
if __name__=='__main__': unittest.main()
