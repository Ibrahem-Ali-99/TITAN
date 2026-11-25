"""Start the disposable protected API/web/ingress runtime for browser tests."""
# ruff: noqa: E501

from __future__ import annotations

import argparse
import json
import os
import secrets
import shutil
import ssl
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes

from accept_protected_orchestration import COMPOSE_FILES, ROOT, Runner, project_name
from keycloak_fixture import disposable_keycloak_fixture, request_json

BROWSER_OVERRIDE: Path | None = None


def extract_browser_evidence(text: str) -> dict[str, object] | None:
    decoder = json.JSONDecoder()
    for offset, character in enumerate(text):
        if character != "{":
            continue
        try:
            value, _ = decoder.raw_decode(text[offset:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and ("screen_results" in value or "role_check" in value):
            return value
    return None


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def http_error_302(self, request, fp, code, msg, headers):
        return fp


def verify_ingress(url: str, ca_file: Path) -> int:
    context = ssl.create_default_context(cafile=str(ca_file))
    opener = urllib.request.build_opener(_NoRedirect)
    request = urllib.request.Request(url)
    try:
        with opener.open(request, context=context, timeout=10) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


def compose(project: str, *args: str) -> list[str]:
    command = ["docker", "compose", "--project-name", project]
    files = (*COMPOSE_FILES, "docker-compose.staging-ingress.yml")
    for name in files:
        command.extend(("-f", name))
    if BROWSER_OVERRIDE is not None:
        command.extend(("-f", str(BROWSER_OVERRIDE)))
    return command + list(args)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--project-name", default=None)
    parser.add_argument("--browser-smoke", action="store_true")
    parser.add_argument("--container-browser", action="store_true")
    parser.add_argument("--rbac-smoke", action="store_true")
    args = parser.parse_args()
    project = args.project_name or project_name()
    https_port = 18000 + secrets.randbelow(1000)
    redirect_uri = (
        "https://staging.local:8443/auth/callback"
        if args.container_browser
        else f"https://staging.local:{https_port}/auth/callback"
    )
    runner = Runner(ROOT, project, args.timeout)
    evidence_path = ROOT / "benchmarks" / f"protected-browser-{project}.json"
    work = Path(tempfile.mkdtemp(prefix="titan-protected-browser-"))
    global BROWSER_OVERRIDE
    BROWSER_OVERRIDE = work / "browser-override.yml"
    BROWSER_OVERRIDE.write_text(
        'services:\n  api:\n    environment:\n      TITAN_MONITORING_DIAGNOSTICS: "1"\n    extra_hosts:\n      - host.docker.internal:host-gateway\n  web:\n    command: ["sh", "-c", "npm run build && npm run start -- --hostname 0.0.0.0 --port 3000"]\n    extra_hosts:\n      - host.docker.internal:host-gateway\n',
        encoding="utf-8",
    )
    try:
        runner.run("docker", ["docker", "info", "--format", "{{.ServerVersion}}"])
        with disposable_keycloak_fixture(
            args.timeout,
            issuer_host="keycloak",
            issuer_port=8080,
            redirect_uri=redirect_uri,
        ) as fixture:
            runner.bootstrap(work)
            tls_dir = work / "tls"
            runner.run(
                "ingress-tls",
                [
                    sys.executable,
                    "scripts/staging_ingress_tls.py",
                    "--source",
                    str(tls_dir),
                    "--volume",
                    f"{project}-ingress-tls",
                ],
            )
            runner.env.update(
                {
                    "TITAN_OIDC_ISSUER": fixture.issuer,
                    "TITAN_OIDC_CLIENT_ID": fixture.client_id,
                    "TITAN_OIDC_CLIENT_SECRET": fixture.client_secret_file.read_text().strip(),
                    "TITAN_OIDC_REDIRECT_URI": redirect_uri,
                    "TITAN_OIDC_AUDIENCE": "titan",
                    "TITAN_OIDC_SESSION_SECRET": os.urandom(32).hex(),
                    "TITAN_HTTPS_PORT": str(https_port),
                    "TITAN_MONITORING_DIAGNOSTICS": "1",
                }
            )
            files = [
                str((ROOT / name).resolve())
                for name in (*COMPOSE_FILES, "docker-compose.staging-ingress.yml")
            ]
            files.append(str(BROWSER_OVERRIDE))
            preflight = [
                sys.executable,
                "scripts/staging_preflight.py",
                "--secrets-dir",
                str(work / "secrets"),
                "--project-name",
                project,
                "--windows-acl-verified",
                "--postgres-tls",
                "--postgres-tls-dir",
                str(tls_dir),
                "--kafka-tls",
                "--kafka-tls-dir",
                str(work / "kafka"),
                "--clickhouse-tls",
                "--clickhouse-tls-dir",
                str(tls_dir),
                "--minio-tls",
                "--minio-tls-dir",
                str(tls_dir),
                "--ingress-tls",
                "--ingress-tls-dir",
                str(tls_dir),
            ]
            for name in files:
                preflight.extend(("--compose-file", name))
            runner.run("preflight-ingress", preflight)
            runner.run("api-build", compose(project, "build", "api"))
            runner.run("web-build", compose(project, "build", "web"))
            runner.run(
                "services",
                compose(
                    project,
                    "up",
                    "-d",
                    "--wait",
                    "postgres",
                    "kafka",
                    "migrate",
                    "api",
                    "web",
                    "ingress",
                ),
            )
            runner.run(
                "keycloak-network",
                [
                    "docker",
                    "network",
                    "connect",
                    "--alias",
                    "keycloak",
                    f"{project}_default",
                    fixture.container,
                ],
            )
            discovery = request_json(f"{fixture.discovery_url}/.well-known/openid-configuration")
            if discovery.get("issuer") != fixture.issuer:
                raise RuntimeError("OIDC discovery issuer does not match the configured issuer")
            runner.run(
                "api-ready",
                compose(
                    project,
                    "exec",
                    "-T",
                    "api",
                    "python",
                    "-c",
                    "import urllib.request; urllib.request.urlopen("
                    "'http://127.0.0.1:8000/health/ready', timeout=5)",
                ),
            )
            runner.run(
                "web-api-ready",
                compose(
                    project,
                    "exec",
                    "-T",
                    "web",
                    "node",
                    "-e",
                    "fetch('http://api:8000/health/ready', { headers: { authorization: `Bearer ${process.env.TITAN_READ_TOKEN}` }, signal: AbortSignal.timeout(5000) }).then(r => { console.log(r.status); process.exit(r.status === 200 ? 0 : 1) }).catch(e => { console.error(e.name); process.exit(1) })",
                ),
            )
            direct_probe = runner.run(
                "direct-api-probe",
                compose(
                    project,
                    "exec",
                    "-T",
                    "web",
                    "node",
                    "-e",
                    """(async()=>{const paths=['/api/v1/monitoring/overview','/health/ready'];
const out=[]; const started=Date.now();
for(let i=0;i<8;i++){for(const path of paths){const t=Date.now();try{const r=await fetch('http://api:8000'+path,{headers:{authorization:`Bearer ${process.env.TITAN_READ_TOKEN}`},signal:AbortSignal.timeout(10000)});out.push({path,status:r.status,duration_ms:Date.now()-t});await r.arrayBuffer()}catch(e){out.push({path,error:e.name,message:String(e.message||'').slice(0,120),duration_ms:Date.now()-t})}}}
console.log(JSON.stringify({attempts:out.length,elapsed_ms:Date.now()-started,results:out}));})().catch(e=>{console.error(e.name);process.exit(1)});""",
                ),
            )
            (work / "direct-api-probe.json").write_text(direct_probe, encoding="utf-8")
            local_api_state = runner.run(
                "api-local-state",
                compose(
                    project,
                    "exec",
                    "-T",
                    "api",
                    "sh",
                    "-c",
                    "python -c \"import json,time,urllib.request,os; t=time.monotonic(); out={};\ntry:\n r=urllib.request.urlopen('http://127.0.0.1:8000/health/ready', timeout=5); out.update(status=r.status)\nexcept Exception as e: out.update(error=type(e).__name__,message=str(e)[:120])\nout['duration_ms']=round((time.monotonic()-t)*1000); out['pid']=os.getpid(); out['proc_stat']=open('/proc/self/stat').read().split()[2]; print(json.dumps(out))\"",
                ),
            )
            (work / "api-local-state.txt").write_text(local_api_state, encoding="utf-8")
            runner.run(
                "monitoring-diagnostics",
                compose(project, "logs", "--no-color", "api"),
            )
            runner.run(
                "web-ready",
                compose(
                    project,
                    "exec",
                    "-T",
                    "web",
                    "node",
                    "-e",
                    "fetch('http://127.0.0.1:3000/', { redirect: 'manual' }).then(r => { console.log(r.status); process.exit((r.ok || (r.status >= 300 && r.status < 400)) ? 0 : 1) }).catch(e => { console.error(e.message); process.exit(1) })",
                ),
            )
            runner.run(
                "ingress-ready",
                [
                    sys.executable,
                    "-c",
                    "import sys,ssl,urllib.request; N=type('N',(urllib.request.HTTPRedirectHandler,),{x:(lambda s,r,f,c,m,h:f) for x in ('http_error_301','http_error_302','http_error_303','http_error_307','http_error_308')}); c=ssl.create_default_context(cafile=sys.argv[2]); o=urllib.request.build_opener(urllib.request.HTTPSHandler(context=c),N); r=o.open(urllib.request.Request(sys.argv[1]),timeout=10); print(r.status); sys.exit(0 if 300 <= r.status < 400 or r.status == 200 else 1)",
                    f"https://localhost:{https_port}/",
                    str(tls_dir / "ca.crt"),
                ],
            )
            if args.browser_smoke:
                ca_file = tls_dir / "ca.crt"
                certificate = x509.load_pem_x509_certificate(ca_file.read_bytes())
                thumbprint = certificate.fingerprint(hashes.SHA1()).hex().upper()
                if not args.container_browser:
                    runner.run(
                        "browser-ca-install",
                        ["certutil", "-user", "-addstore", "-f", "Root", str(ca_file)],
                    )
                try:
                    try:
                        screenshot = ROOT / "benchmarks" / f"protected-browser-{project}.png"
                        if args.container_browser:
                            evidence_path.touch()
                            screenshot.touch()
                            netlog_path = (
                                ROOT / "benchmarks" / f"protected-browser-{project}-netlog.json"
                            )
                            netlog_path.touch()
                            ingress_ip = runner.run(
                                "browser-network",
                                [
                                    "docker",
                                    "inspect",
                                    "--format",
                                    "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                                    f"{project}-ingress-1",
                                ],
                            ).strip()
                            role_runs = (
                                (
                                    ("viewer", fixture.password_files["viewer"]),
                                    ("analyst", fixture.password_files["analyst"]),
                                    ("operator", fixture.password_files["operator"]),
                                    ("admin", fixture.password_files["admin"]),
                                )
                                if args.rbac_smoke
                                else (("viewer", fixture.password_files["viewer"]),)
                            )
                            for role, password_file in role_runs:
                                role_evidence = evidence_path.with_name(
                                    f"{evidence_path.stem}-{role}{evidence_path.suffix}"
                                )
                                role_evidence.touch()
                                runner.run(
                                    f"browser-smoke-{role}",
                                    [
                                        "docker",
                                        "run",
                                        "--rm",
                                        "--network",
                                        f"{project}_default",
                                        "--add-host",
                                        f"staging.local:{ingress_ip}",
                                        "--add-host",
                                        "host.docker.internal:host-gateway",
                                        "-v",
                                        f"{ROOT / 'scripts' / 'browser_oidc_smoke.py'}:/run/browser_oidc_smoke.py:ro",
                                        "-v",
                                        f"{password_file}:/run/viewer-password:ro",
                                        "-v",
                                        f"{ca_file}:/run/staging-ca.crt:ro",
                                        "-v",
                                        f"{screenshot}:/run/browser.png",
                                        "-v",
                                        f"{role_evidence}:/run/browser.json",
                                        "-v",
                                        f"{netlog_path}:/run/netlog.json",
                                        "mcr.microsoft.com/playwright/python:v1.65.1-noble",
                                        "bash",
                                        "-lc",
                                        f"cp /run/staging-ca.crt /usr/local/share/ca-certificates/titan-staging.crt && update-ca-certificates >/dev/null && (command -v certutil >/dev/null || (apt-get update -qq && apt-get install -y -qq libnss3-tools)) && mkdir -p $HOME/.pki/nssdb && (certutil -d sql:$HOME/.pki/nssdb -L >/dev/null 2>&1 || certutil -d sql:$HOME/.pki/nssdb -N --empty-password) && certutil -d sql:$HOME/.pki/nssdb -A -n titan-staging-ca -t C,, -i /run/staging-ca.crt && python -c \"import socket,ssl,urllib.request; print(socket.gethostbyname('staging.local')); c=ssl.create_default_context(cafile='/run/staging-ca.crt'); r=urllib.request.urlopen('https://staging.local:8443/', context=c, timeout=10); print(r.status); print(urllib.request.urlopen('http://keycloak:8080/realms/titan-test/.well-known/openid-configuration', timeout=10).status)\" && python -m pip install --no-cache-dir playwright==1.65.1 >/dev/null && python /run/browser_oidc_smoke.py --base-url 'https://staging.local:8443' --issuer '{fixture.issuer}' --username {role} --staging-host {ingress_ip} --net-log /run/netlog.json --password-file /run/viewer-password --screenshot /run/browser.png --evidence /run/browser.json --boundary-probe {'--role-check --expected-role ' + role if args.rbac_smoke else ''} > /run/browser.json",
                                    ],
                                )
                        else:
                            browser_output = runner.run(
                                "browser-smoke",
                                [
                                    sys.executable,
                                    "scripts/browser_oidc_smoke.py",
                                    "--base-url",
                                    f"https://staging.local:{https_port}",
                                    "--issuer",
                                    fixture.issuer,
                                    "--password-file",
                                    str(fixture.password_files["viewer"]),
                                    "--screenshot",
                                    str(screenshot),
                                    "--evidence",
                                    str(evidence_path),
                                    "--boundary-probe",
                                ],
                            )
                            browser_evidence = extract_browser_evidence(
                                browser_output + "\n" + runner.evidence.read_text(encoding="utf-8")
                            )
                            if isinstance(browser_evidence, dict):
                                evidence_path.write_text(
                                    json.dumps(browser_evidence, indent=2) + "\n",
                                    encoding="utf-8",
                                )
                    except RuntimeError:
                        logs = runner.run(
                            "runtime-logs",
                            compose(project, "logs", "--no-color", "web", "api", "ingress"),
                        )
                        if evidence_path.exists():
                            try:
                                evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
                                evidence["direct_api_probe"] = json.loads(direct_probe)
                                evidence["proxy_exceptions"] = [
                                    line.split("[titan-proxy-error]", 1)[1].strip()
                                    for line in logs.splitlines()
                                    if "[titan-proxy-error]" in line
                                ]
                                evidence_path.write_text(
                                    json.dumps(evidence, indent=2) + "\n", encoding="utf-8"
                                )
                            except (OSError, ValueError):
                                pass
                        raise
                    else:
                        if evidence_path.exists():
                            try:
                                evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
                                evidence["direct_api_probe"] = json.loads(direct_probe)
                                evidence_path.write_text(
                                    json.dumps(evidence, indent=2) + "\n", encoding="utf-8"
                                )
                            except (OSError, ValueError):
                                pass
                finally:
                    if not args.container_browser:
                        runner.run(
                            "browser-ca-remove",
                            ["certutil", "-user", "-delstore", "Root", thumbprint],
                        )
            print(f"protected runtime ready: {project}")
            return 0
    finally:
        summary = "unavailable"
        if evidence_path.exists():
            try:
                summary = str(
                    json.loads(evidence_path.read_text(encoding="utf-8")).get(
                        "result", "incomplete"
                    )
                )
            except (OSError, ValueError):
                summary = "unreadable"
        print(f"browser evidence: {evidence_path} result={summary}")
        subprocess.run(
            compose(project, "down", "--volumes", "--remove-orphans"),
            cwd=ROOT,
            env=runner.env,
            capture_output=True,
            timeout=args.timeout,
            check=False,
        )
        runner.cleanup()
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
