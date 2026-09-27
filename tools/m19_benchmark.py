#!/usr/bin/env python3
"""Bounded SIFT capacity steps and one selected-envelope rehearsal on local MinIO."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import secrets
import struct

from minio_harness import container_scope, ready, run
from test_s3 import IMAGE

HASHES = {
    'siftsmall_base.fvecs': '1e90414a3254361aba48a0d58e461f7661fb135dabb3da985290e94de8f60fe0',
    'siftsmall_query.fvecs': '095af38bda2741be721447ba97e9286fa918ece946be92a9ae133276eddc1a10',
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path)
    parser.add_argument('--data', type=Path)
    parser.add_argument('--smoke', action='store_true')
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument('--m20', action='store_true', help='only the demonstrated 5,000-row boundary and final rehearsal')
    scope.add_argument('--probe-only', type=int, choices=[2000, 5000, 10000],
                       help='one bounded row step, without a final acceptance rehearsal')
    parser.add_argument('--diagnostic', action='store_true', help='unpaced 20-round real-data M20 attribution only')
    args = parser.parse_args()
    if args.diagnostic and (args.smoke or not args.m20):
        parser.error('--diagnostic requires --m20 and real data')
    args.output.mkdir(parents=True, exist_ok=False)
    data = args.data
    if args.smoke:
        data = args.output / 'fixture'; data.mkdir()
        for name, count in [('siftsmall_base.fvecs', 10000), ('siftsmall_query.fvecs', 100)]:
            with (data / name).open('wb') as f:
                for row in range(count):
                    f.write(struct.pack('<I128f', 128, *[((row * 17 + col * 11) % 256) for col in range(128)]))
    elif data is None:
        parser.error('--data must point to the verified SIFT small files')
    hashes = {name: hashlib.sha256((data / name).read_bytes()).hexdigest() for name in HASHES}
    if not args.smoke and hashes != HASHES:
        raise ValueError('SIFT source digest mismatch')
    env = {k: v for k, v in os.environ.items() if not k.startswith(('AWS_', 'GLIDER_S3_', 'GLIDER_M19_', 'MINIO_'))}
    run('cargo', 'build', '--release', '--locked', '--features', 's3', '--example', 'm19_capacity', env=env)
    source = hashlib.sha256()
    for path in sorted(Path('src').rglob('*.rs')) + [Path('Cargo.toml'), Path('Cargo.lock'), Path('examples/m19_capacity.rs'), Path(__file__).resolve().relative_to(Path.cwd().resolve())]:
        source.update(path.as_posix().encode()); source.update(path.read_bytes())
    name = 'glider-m19-' + secrets.token_hex(6)
    info = dict(version=1, git_revision=run('git','rev-parse','HEAD',capture=True).strip(),
                git_status=run('git','status','--short',capture=True).strip(), source_sha256=source.hexdigest(),
                platform=platform.platform(), machine=platform.machine(), cpu_count=os.cpu_count(),
                rust=run('rustc','--version',capture=True).strip(), minio_image=IMAGE, dataset_sha256=hashes,
                dataset='synthetic fixture' if args.smoke else 'SIFT small prefix; rotate137 updates',
                docker=run('docker','version','--format','{{.Server.Version}}',capture=True).strip(), smoke=args.smoke,
                cache_control='fresh serving process; uncontrolled OS/MinIO caches, power and competing load',
                backend_scope='loopback MinIO only; no remote or AWS capacity acceptance', m20=args.m20, diagnostic=args.diagnostic,
                probe_only=args.probe_only)
    (args.output/'run.json').write_text(json.dumps(info,indent=2)+'\n')
    with container_scope(name):
        env.update(MINIO_ROOT_USER='glider-'+secrets.token_hex(8), MINIO_ROOT_PASSWORD=secrets.token_hex(24))
        run('docker','run','-d','--name',name,'-p','127.0.0.1::9000','-e','MINIO_ROOT_USER','-e','MINIO_ROOT_PASSWORD',IMAGE,'server','/data',env=env,capture=True)
        port=run('docker','port',name,'9000/tcp',capture=True).strip().split(':')[-1]
        endpoint='http://127.0.0.1:'+port; ready(endpoint,name)
        run('docker','exec',name,'mc','mb','test/glider-test',capture=True)
        env.update(GLIDER_S3_ENDPOINT=endpoint,GLIDER_S3_BUCKET='glider-test',AWS_ACCESS_KEY_ID=env['MINIO_ROOT_USER'],AWS_SECRET_ACCESS_KEY=env['MINIO_ROOT_PASSWORD'])
        def phase(case, rows, rounds, operation):
            output=args.output/case; output.mkdir(exist_ok=True)
            env['GLIDER_M19_CASE']=case
            report=output/('prepare.json' if operation=='prepare' else 'serve.json')
            run('target/release/examples/m19_capacity',operation,str(rows),str(rounds),'smoke' if args.smoke or args.diagnostic else 'paced',str(data),str(report),env=env,timeout=180,stage='M19 '+case+' '+operation)
            return json.loads(report.read_text())
        supported=None; breach=None
        for rows in ([args.probe_only] if args.probe_only else [5000] if args.m20 else [2000] if args.smoke else [2000,5000,10000]):
            case='probe-'+str(rows); rounds=2 if args.smoke else 20
            phase(case,rows,rounds,'prepare'); result=phase(case,rows,rounds,'serve')
            if not args.smoke and not result['performance_accepted']:
                breach=rows; break
            supported=rows
        final=None
        if supported is not None and not args.probe_only:
            case='final-'+str(supported); rounds=2 if args.smoke else 50
            phase(case,supported,rounds,'prepare'); result=phase(case,supported,rounds,'serve')
            phase(case,supported,rounds,'verify')
            verification=json.loads((args.output/case/'verify.json').read_text())
            final=dict(rows=supported,performance_accepted=result['performance_accepted'],backup_restore_accepted=verification['backup_restore_accepted'])
        decision=dict(supported_probe_rows=supported,first_breach_rows=breach,final=final,smoke=args.smoke)
        (args.output/'decision.json').write_text(json.dumps(decision,indent=2)+'\n')
    print('M19 bounded study finished and MinIO removed:',json.dumps(decision))


if __name__=='__main__':
    main()
