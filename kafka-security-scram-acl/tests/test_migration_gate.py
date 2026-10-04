import importlib.util,pathlib,unittest
p=pathlib.Path(__file__).resolve().parents[1]/'scripts/migration_gate.py'
s=importlib.util.spec_from_file_location('gate',p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
class Gate(unittest.TestCase):
 def complete(self):return [{'event':'producer_client_recreated','transport':'secure'},{'event':'send_ok'},{'event':'consumer_client_recreated','transport':'secure'},{'event':'consume'},{'event':'commit_ok'}]
 def test_complete_allowed(self):self.assertTrue(m.secure_clients_ready(self.complete()))
 def test_missing_consumer_blocked(self):self.assertFalse(m.secure_clients_ready(self.complete()[:2]))
 def test_no_secure_send_blocked(self):self.assertFalse(m.secure_clients_ready([e for e in self.complete() if e['event']!='send_ok']))
 def test_no_commit_blocked(self):self.assertFalse(m.secure_clients_ready(self.complete()[:-1]))
 def test_reversion_blocked(self):self.assertFalse(m.secure_clients_ready(self.complete()+[{'event':'consumer_client_recreated','transport':'plain'}]))
if __name__=='__main__':unittest.main()
