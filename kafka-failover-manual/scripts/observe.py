#!/usr/bin/env python3
"""CLI 출력 파일을 관측. 시각은 출력 관측 시각이며 애플리케이션 처리 지연이 아님."""
import argparse, json, time
from pathlib import Path
p=argparse.ArgumentParser(); p.add_argument('runtime'); a=p.parse_args(); d=Path(a.runtime)
with open(d/'consumer.out') as src, open(d/'received.jsonl','x') as dst:
    while True:
        line=src.readline()
        if line:
            dst.write(json.dumps(dict(time_ms=time.time_ns()//1000000,line=line.rstrip()))+'\n'); dst.flush()
        elif (d/'stop-observer').exists(): break
        else: time.sleep(.02)
