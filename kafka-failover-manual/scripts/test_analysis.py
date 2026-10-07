"""전수 대조가 누락/중복/순서/프로세스 오류를 실제로 거부하는지 검증."""
import copy, json, subprocess, sys, tempfile, unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parent
class AnalysisTests(unittest.TestCase):
    def execute(self, change=None):
        # 실패 재현 자료도 보존. 테스트에서 삭제하지 않는다.
        base=Path(tempfile.mkdtemp(prefix='kafka-failover-analysis-')); d=base/'raw'; d.mkdir()
        values=[dict(id=i,time_ms=1000+i,value='run|%08d|%d'%(i,1000+i)) for i in (1,2,3)]
        data={'run.json':dict(run_id='run',failure=None,events=[],producer_exit_code=0),
          'snapshots.json':[dict(label='baseline',client_pids=[10,11],clients_alive=True,commits=[dict(partition=i,current=1,end=1,lag=0) for i in range(3)]),dict(label='final-drain',client_pids=[10,11],clients_alive=False,commits=[dict(partition=i,current=1,end=1,lag=0) for i in range(3)])],
          'preservation.json':dict(health_before=dict(cluster_id='A'),health_after=dict(cluster_id='A'),same_pid_and_cmdline=True,test_ports_closed=True,all_owned_processes_exited=True),
          'executed-script.json':dict(sha256='fixture')}
        received=[dict(time_ms=1100+i,line='Partition:%d\tOffset:0\trun|%08d|%d'%(i-1,i,1000+i)) for i in (1,2,3)]
        errors={'producer':'','consumer':''}
        if change: change(data,values,received,errors)
        for name,obj in data.items(): (d/name).write_text(json.dumps(obj))
        (d/'input.jsonl').write_text('\n'.join(map(json.dumps,values))+'\n')
        (d/'received.jsonl').write_text('\n'.join(map(json.dumps,received))+'\n')
        for k,v in errors.items(): (d/(k+'.err')).write_text(v)
        r=subprocess.run([sys.executable,str(ROOT/'analyze.py'),str(d),str(base/'out')],capture_output=True,text=True)
        return r.returncode,json.loads((base/'out/summary.json').read_text())
    def test_exact_set_passes(self): self.assertEqual(self.execute()[0],0)
    def test_missing_rejected(self):
        code,s=self.execute(lambda d,v,r,e:r.pop()); self.assertNotEqual(code,0); self.assertEqual(s['missing_ids'],[3])
    def test_duplicate_rejected(self):
        code,s=self.execute(lambda d,v,r,e:r.append(copy.deepcopy(r[0]))); self.assertNotEqual(code,0); self.assertEqual(s['duplicate_count'],1)
    def test_unexpected_rejected(self):
        code,s=self.execute(lambda d,v,r,e:r.append(dict(time_ms=1110,line='Partition:0\tOffset:1\trun|00000004|1004'))); self.assertNotEqual(code,0); self.assertEqual(s['unexpected_ids'],[4])
    def test_payload_mismatch_rejected(self):
        code,s=self.execute(lambda d,v,r,e:r[0].update(line='Partition:0\tOffset:0\trun|00000001|9999')); self.assertNotEqual(code,0); self.assertFalse(s['payload_match'])
    def test_producer_exit1_rejected(self): self.assertNotEqual(self.execute(lambda d,v,r,e:d['run.json'].update(producer_exit_code=1))[0],0)
    def test_error_log_rejected(self): self.assertNotEqual(self.execute(lambda d,v,r,e:e.update(producer='ERROR send failed'))[0],0)
    def test_info_exception_is_not_fatal(self):
        code,s=self.execute(lambda d,v,r,e:e.update(producer='[time] INFO retry after DisconnectException'))
        self.assertEqual(code,0); self.assertEqual(s['errors']['producer']['error_lines'],0); self.assertEqual(s['errors']['producer']['exception_mentions'],1)
    def test_warn_exception_is_not_fatal(self):
        code,s=self.execute(lambda d,v,r,e:e.update(consumer='[time] WARN retry after DisconnectException'))
        self.assertEqual(code,0); self.assertEqual(s['errors']['consumer']['warning_lines'],1)
    def test_client_pid_change_rejected(self): self.assertNotEqual(self.execute(lambda d,v,r,e:d['snapshots.json'][1].update(client_pids=[12,11]))[0],0)
    def test_partition_offset_regression_rejected(self):
        code,s=self.execute(lambda d,v,r,e:r.append(dict(time_ms=1110,line='Partition:0\tOffset:0\trun|00000004|1004'))); self.assertNotEqual(code,0); self.assertGreater(s['partition_order_errors'],0)
if __name__=='__main__': unittest.main()
