#!/usr/bin/env python3
"""STDOUT에 RUN_ID|증가ID|입력시각을 생성. stop-input 파일을 만들면 정상 EOF."""
import argparse, json, sys, time
from pathlib import Path
p=argparse.ArgumentParser(); p.add_argument('runtime'); p.add_argument('run_id'); p.add_argument('--interval',type=float,default=.1); a=p.parse_args()
d=Path(a.runtime); d.mkdir(parents=True,exist_ok=True)
if (d/'input.jsonl').exists(): raise SystemExit('기존 입력 기록 덮어쓰기 금지')
n=0
with open(d/'input.jsonl','w') as f:
    while not (d/'stop-input').exists():
        n+=1; ts=time.time_ns()//1000000; value='%s|%08d|%d'%(a.run_id,n,ts)
        print(value,flush=True)
        f.write(json.dumps(dict(id=n,time_ms=ts,value=value))+'\n'); f.flush(); time.sleep(a.interval)
