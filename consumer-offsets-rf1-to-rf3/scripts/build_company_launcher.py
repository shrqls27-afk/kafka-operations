"""Build a self-contained .sh; bundled source is kept alongside for review."""
import base64
import io
from pathlib import Path
import zipfile
root = Path(__file__).resolve().parents[1]
buf = io.BytesIO()
with zipfile.ZipFile(buf, 'w', compression=zipfile.ZIP_DEFLATED) as z:
    for name in ['scripts/company.py', 'scripts/reassign.py', 'scripts/java.sh', 'tests/OffsetsLab.java']:
        info = zipfile.ZipInfo(name, date_time=(2026, 1, 1, 0, 0, 0))
        info.compress_type = zipfile.ZIP_DEFLATED
        z.writestr(info, (root/name).read_bytes())
payload = base64.b64encode(buf.getvalue()).decode()
head = '''#!/usr/bin/env bash
# 단일 파일 운영용 실행기. 원본: scripts/company.py 등. 외부 다운로드 없음.
set -euo pipefail
command -v python3 >/dev/null || { echo 'Python 3.8 이상이 필요합니다.' >&2; exit 1; }
exec python3 -c '
import base64, io, os, pathlib, subprocess, sys, tempfile, zipfile
os.umask(0o077)
base=pathlib.Path.home()/"kafka-rf3-work"
base.mkdir(mode=0o700, exist_ok=True)
root=pathlib.Path(tempfile.mkdtemp(prefix="launcher-", dir=str(base)))
with zipfile.ZipFile(io.BytesIO(base64.b64decode("PAYLOAD"))) as z:
    z.extractall(root)
sys.exit(subprocess.call([sys.executable, str(root/"scripts/company.py")]+sys.argv[1:]))
' "$@"
'''
(root/'offsets-rf3.sh').write_text(head.replace('PAYLOAD', payload))
print('offsets-rf3.sh generated')
