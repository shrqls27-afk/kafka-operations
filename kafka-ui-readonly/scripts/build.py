#!/usr/bin/env python3
"""Build the local v0.7.2 overlay. No service changes and no file deletion."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import zipfile

BASE_SHA256 = 'd5e4c2819cb8c819b0baf9261ce22c63b610bb1d9e5f52ee51424c725e6b2cb3'
ROOT = Path(__file__).resolve().parents[1]
FILES = {
    'ReadOnlyGuardFilter.java': 'kafka-ui-api/src/main/java/com/provectus/kafka/ui/config/ReadOnlyGuardFilter.java',
    'useReadOnlyGuard.ts': 'kafka-ui-react-app/src/lib/hooks/useReadOnlyGuard.ts',
    'usePermission.ts': 'kafka-ui-react-app/src/lib/hooks/usePermission.ts',
    'useCreatePermisson.ts': 'kafka-ui-react-app/src/lib/hooks/useCreatePermisson.ts',
}

def run(args, **kwargs):
    subprocess.run([str(x) for x in args], check=True, **kwargs)

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--upstream', required=True, type=Path)
    p.add_argument('--base-jar', required=True, type=Path)
    p.add_argument('--java-home', required=True, type=Path)
    p.add_argument('--pnpm', required=True, type=Path)
    p.add_argument('--generator', required=True, type=Path)
    p.add_argument('--output-dir', required=True, type=Path)
    p.add_argument('--offline', action='store_true')
    a = p.parse_args()
    for key in vars(a):
        if isinstance(getattr(a, key), Path): setattr(a, key, getattr(a, key).resolve())
    if hashlib.sha256(a.base_jar.read_bytes()).hexdigest() != BASE_SHA256:
        p.error('Base JAR must match the official v0.7.2 SHA256')
    if a.output_dir.exists(): p.error('Use a new output directory; previous evidence is preserved')
    a.output_dir.mkdir(parents=True)
    for name, target in FILES.items(): shutil.copy2(ROOT/'src'/name, a.upstream/target)
    frontend = a.upstream/'kafka-ui-react-app'
    for name in ('readOnlyHooks.spec.tsx', 'readOnlyPolicy.spec.tsx', 'brokerReadOnly.spec.tsx'):
        shutil.copy2(ROOT/'tests'/name, frontend/'src/lib/hooks/__tests__'/name)
    compile_dir = a.output_dir/'compile'
    compile_dir.mkdir()
    with zipfile.ZipFile(a.base_jar) as source:
        for entry in source.infolist():
            if entry.filename.startswith('BOOT-INF/lib/') and entry.filename.endswith('.jar'):
                (compile_dir/Path(entry.filename).name).write_bytes(source.read(entry))
            elif entry.filename.startswith('BOOT-INF/classes/') and entry.filename.endswith('.class'):
                source.extract(entry, compile_dir)
    classes = a.output_dir/'classes'
    cp = str(compile_dir/'*')+':'+str(compile_dir/'BOOT-INF/classes')
    run([a.java_home/'bin/javac', '--release', '17', '-cp', cp, '-d', classes,
         ROOT/'src/ReadOnlyGuardFilter.java'])
    run([a.java_home/'bin/java', '-Xmx512m', '-jar', a.generator, 'generate',
         '-g', 'typescript-fetch', '-i', a.upstream/'kafka-ui-contract/src/main/resources/swagger/kafka-ui-api.yaml',
         '-o', frontend/'src/generated-sources', '--additional-properties',
         'enumPropertyNaming=UPPERCASE,typescriptThreePlus=true,supportsES6=true,nullSafeAdditionalProps=true,withInterfaces=true',
         '--type-mappings', 'object=any'])
    install=[a.pnpm, 'install', '--frozen-lockfile', '--network-concurrency=4', '--child-concurrency=1']
    if a.offline: install.append('--offline')
    run(install, cwd=frontend)
    run([a.pnpm, 'exec', 'jest', '--runInBand', '--watch=false', '--reporters=default',
         '--runTestsByPath', 'src/lib/hooks/__tests__/readOnlyHooks.spec.tsx',
         'src/lib/hooks/__tests__/readOnlyPolicy.spec.tsx',
         'src/lib/hooks/__tests__/brokerReadOnly.spec.tsx'], cwd=frontend)
    run([a.pnpm, 'build'], cwd=frontend)
    output = a.output_dir/'kafka-ui-api-v0.7.2-readonly.1.jar'
    metadata = {'base_version':'v0.7.2', 'patch_version':'readonly.1', 'base_sha256':BASE_SHA256,
                'source_sha256':{name:hashlib.sha256((ROOT/'src'/name).read_bytes()).hexdigest() for name in FILES}}
    with zipfile.ZipFile(a.base_jar) as source, zipfile.ZipFile(output, 'w') as target:
        for entry in source.infolist():
            if not entry.filename.startswith('BOOT-INF/classes/static/'):
                target.writestr(entry, source.read(entry))
        for file in sorted((frontend/'build').rglob('*')):
            if file.is_file():
                target.write(file, 'BOOT-INF/classes/static/'+file.relative_to(frontend/'build').as_posix(),
                             compress_type=zipfile.ZIP_DEFLATED)
        for file in sorted(classes.rglob('*.class')):
            target.write(file, 'BOOT-INF/classes/'+file.relative_to(classes).as_posix(),
                         compress_type=zipfile.ZIP_DEFLATED)
        target.writestr('META-INF/readonly-patch.json', json.dumps(metadata, indent=2)+'\n')
        target.writestr('META-INF/readonly-LICENSE', (ROOT/'LICENSE').read_text())
        target.writestr('META-INF/readonly-NOTICE', (ROOT/'NOTICE').read_text())
        notices=[]
        store=frontend/'node_modules/.pnpm'
        for modules in sorted(store.glob('*/node_modules')):
            for package in sorted(modules.iterdir()):
                packages=sorted(package.iterdir()) if package.name.startswith('@') else [package]
                for dependency in packages:
                    if not dependency.is_dir(): continue
                    for notice in sorted(dependency.iterdir()):
                        if notice.is_file() and notice.name.upper().startswith(('LICENSE','LICENCE','COPYING','NOTICE')):
                            notices.append('\n--- '+dependency.name+'/'+notice.name+' ---\n'+notice.read_text(errors='replace'))
        target.writestr('META-INF/frontend-thirdparty-notices.txt', ''.join(notices))
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    (a.output_dir/'SHA256SUMS').write_text(digest+'  '+output.name+'\n')
    print(output.name, digest)

if __name__ == '__main__': main()
