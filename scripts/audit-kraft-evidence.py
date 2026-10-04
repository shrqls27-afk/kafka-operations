#!/usr/bin/env python3
"""원본 보존 자료에서 버전/모드만 추출. Kafka 시작·재시작·format·재할당 없음."""
import argparse, base64, datetime, hashlib, io, json, os, re, subprocess, zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOPICS = ["consumer-offsets-rf1-to-rf3", "kafka-security-scram-acl"]

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def audit():
    result = {"audited_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "expected_kafka_version": "3.9.1", "expected_mode": "KRaft static quorum",
              "new_cluster_experiment_performed": False, "runs": []}
    for topic in TOPICS:
        for directory in sorted((ROOT / topic / "runtime").iterdir()):
            if not directory.is_dir():
                continue
            configs = sorted(p for p in directory.glob("node-*.properties") if re.fullmatch(r"node-\d+\.properties", p.name))
            if not configs:
                continue
            entry = {"topic": topic, "run": directory.name, "configs": [], "broker_logs": []}
            for path in configs:
                props = dict(line.split("=", 1) for line in path.read_text().splitlines() if "=" in line and not line.startswith("#"))
                entry["configs"].append({"file": path.name, "roles": props.get("process.roles"),
                    "node_id": int(props["node.id"]), "static_voter_count": len(props.get("controller.quorum.voters", "").split(",")),
                    "zookeeper_connect_present": "zookeeper.connect" in props, "sha256": sha(path)})
            for path in sorted(directory.glob("broker*.log")):
                text = path.read_text(errors="replace")
                versions = sorted(set(re.findall(r"Kafka version:\s*(\S+)", text)))
                if not versions:
                    continue  # 성공 기동 전 로그는 버전 확인 근거로 사용하지 않음
                entry["broker_logs"].append({"file": path.name, "versions": versions,
                    "kafka_commit_ids": sorted(set(re.findall(r"Kafka commitId:\s*(\S+)", text))),
                    "startup_roles_broker_controller": bool(re.search(r"process.roles\s*=\s*\[broker,\s*controller\]", text)),
                    "sha256": sha(path)})
            entry["matches_version_and_mode"] = (len(entry["configs"]) == 3 and bool(entry["broker_logs"])
                and all(x["roles"] == "broker,controller" and x["static_voter_count"] == 3 and not x["zookeeper_connect_present"] for x in entry["configs"])
                and all(x["versions"] == ["3.9.1"] and x["startup_roles_broker_controller"] for x in entry["broker_logs"]))
            result["runs"].append(entry)
    result["run_count"] = len(result["runs"])
    result["broker_startup_logs_checked"] = sum(len(x["broker_logs"]) for x in result["runs"])
    result["all_runs_match_version_and_mode"] = bool(result["runs"]) and all(x["matches_version_and_mode"] for x in result["runs"])
    result["raw_evidence_integrity"] = []
    manifests = [(TOPICS[0], n, ROOT / TOPICS[0] / "evidence" / n / "raw-evidence-sha256.json", False)
                 for n in ["run-20261003-01", "run-20261003-02", "standalone-20261003-01"]]
    manifests += [(TOPICS[1], n, ROOT / TOPICS[1] / "evidence" / f, True) for n, f in [
        ("A-20261004-03", "A-summary.json"), ("B-20261004-03", "B-summary.json"),
        ("C-20261004-01", "C-short-timeout-summary.json"), ("C-20261004-03", "C-reference-summary.json")]]
    for topic, run, path, nested in manifests:
        manifest = json.loads(path.read_text())
        if nested:
            manifest = manifest["raw_evidence_sha256"]
        directory = ROOT / topic / "runtime" / run
        mismatches = []
        for name, expected in manifest.items():
            raw = directory / name
            if directory.resolve() not in raw.resolve().parents:
                raise ValueError("허용된 원본 디렉터리 밖의 해시 경로")
            if not raw.is_file() or sha(raw) != expected:
                mismatches.append(name)
        result["raw_evidence_integrity"].append({"topic": topic, "run": run, "files_checked": len(manifest), "mismatches": mismatches})
    offsets = ROOT / TOPICS[0]
    extract = lambda path: re.search(r'base64.b64decode\("([A-Za-z0-9+/=]+)"\)', path.read_text()).group(1)
    payload = extract(offsets / "offsets-rf3.sh")
    original = extract(offsets / "runtime/standalone-20261003-01/standalone/offsets-rf3.sh")
    with zipfile.ZipFile(io.BytesIO(base64.b64decode(payload))) as z:
        equals_sources = all(z.read(name) == (offsets / name).read_bytes() for name in z.namelist())
    result["offsets_launcher"] = {"current_sha256": sha(offsets / "offsets-rf3.sh"),
        "payload_equals_current_sources": equals_sources, "payload_equals_actual_tested_file": payload == original}
    result["passed"] = result["all_runs_match_version_and_mode"] and equals_sources and payload == original and all(not x["mismatches"] for x in result["raw_evidence_integrity"])
    return result

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--kafka-home", type=Path)
    parser.add_argument("--run-unit-tests", action="store_true")
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("기존 감사 증거를 덮어쓰지 않음")
    result = audit()
    if args.kafka_home:
        env = dict(os.environ, KAFKA_HEAP_OPTS="-Xms64m -Xmx128m")
        process = subprocess.run([str(args.kafka_home / "bin/kafka-topics.sh"), "--version"], env=env, capture_output=True, text=True, timeout=60)
        versions = re.findall(r"(?m)^3\.9\.1(?:\s|$)", process.stdout)
        result["installed_cli"] = {"exit_code": process.returncode, "version_3_9_1_confirmed": bool(versions)}
        result["passed"] &= process.returncode == 0 and bool(versions)
    if args.run_unit_tests:
        result["unit_tests"] = []
        for topic in TOPICS:
            process = subprocess.run(["python3", "-m", "unittest", "discover", "-s", str(ROOT / topic / "tests"), "-p", "test_*.py"], capture_output=True, text=True, timeout=120)
            text = process.stdout + process.stderr
            match = re.search(r"Ran (\d+) tests", text)
            result["unit_tests"].append({"topic": topic, "exit_code": process.returncode,
                "test_count": int(match.group(1)) if match else None, "resource_warning_observed": "ResourceWarning" in text})
            result["passed"] &= process.returncode == 0
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({k: result[k] for k in ["run_count", "broker_startup_logs_checked", "all_runs_match_version_and_mode", "passed"]}))
    if not result["passed"]:
        raise SystemExit(2)

if __name__ == "__main__":
    main()
