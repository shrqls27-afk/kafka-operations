"""출력 열 이름을 사용한 복제 판정과 순차 종료 불변조건."""
def replication_rows(text):
    lines=[x.split() for x in text.splitlines() if x.strip()]
    if not lines: raise ValueError('빈 quorum replication 출력')
    header=lines[0]
    required={'NodeId','LogEndOffset','Lag','Status'}
    if not required.issubset(header): raise ValueError('필수 replication header 누락')
    rows=[]
    for fields in lines[1:]:
        if len(fields)!=len(header): raise ValueError('replication 열 개수 불일치')
        row=dict(zip(header,fields))
        for key in ('NodeId','LogEndOffset','Lag'): row[key]=int(row[key])
        rows.append(row)
    return rows

def voters_caught_up(text, live_ids):
    rows=[r for r in replication_rows(text) if r['Status'] in ('Leader','Follower') and r['NodeId'] in live_ids]
    return {r['NodeId'] for r in rows}==set(live_ids) and all(r['Lag']==0 for r in rows) and sum(r['Status']=='Leader' for r in rows)==1

class SequentialGate:
    def __init__(self): self.stopped=None
    def stop(self,node,healthy):
        if self.stopped is not None: raise RuntimeError('누적 종료 금지')
        if not healthy: raise RuntimeError('정상 복구 전 종료 금지')
        self.stopped=node
    def recovered(self,node,healthy):
        if self.stopped!=node or not healthy: raise RuntimeError('복구 gate 미충족')
        self.stopped=None
