"""Provision and validate disposable protected-staging certificates.

This tool deliberately uses the platform OpenSSL executable and ordinary PEM
files. It does not change service listeners or place private keys in Git.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import stat
import subprocess
import sys
import tempfile
from pathlib import Path

SERVICES = ("kafka", "postgres", "clickhouse", "minio")


def _run(*args: str, cwd: Path | None = None) -> str:
    result = subprocess.run(["openssl", *args], cwd=cwd, check=True, capture_output=True, text=True)
    return result.stdout


def _private(path: Path, *, windows_acl_verified: bool) -> None:
    if os.name == "nt" and not windows_acl_verified:
        raise ValueError("Windows ACL verification is required for private keys")
    if os.name != "nt":
        path.chmod(stat.S_IRUSR | stat.S_IWUSR)


def provision(root: Path, *, days: int, windows_acl_verified: bool) -> None:
    if days <= 0 or days > 825:
        raise ValueError("certificate lifetime must be between 1 and 825 days")
    if root.exists() and any(root.iterdir()):
        raise ValueError(f"refusing to overwrite non-empty TLS directory: {root}")
    root.mkdir(parents=True, exist_ok=True)
    ca_key = root / "ca.key"
    ca_crt = root / "ca.crt"
    _run("genrsa", "-out", str(ca_key), "3072")
    _private(ca_key, windows_acl_verified=windows_acl_verified)
    _run(
        "req",
        "-x509",
        "-new",
        "-key",
        str(ca_key),
        "-sha256",
        "-days",
        str(days),
        "-subj",
        "/CN=TITAN protected staging CA",
        "-out",
        str(ca_crt),
    )
    for service in SERVICES:
        key = root / f"{service}.key"
        csr = root / f"{service}.csr"
        crt = root / f"{service}.crt"
        ext = root / f"{service}.ext"
        ext.write_text(
            "subjectAltName = DNS:" + service + ",DNS:" + service + ".titan-staging\n"
            "extendedKeyUsage = serverAuth\n",
            encoding="ascii",
        )
        _run("genrsa", "-out", str(key), "2048")
        _private(key, windows_acl_verified=windows_acl_verified)
        _run("req", "-new", "-key", str(key), "-subj", f"/CN={service}", "-out", str(csr))
        _run(
            "x509",
            "-req",
            "-in",
            str(csr),
            "-CA",
            str(ca_crt),
            "-CAkey",
            str(ca_key),
            "-CAcreateserial",
            "-sha256",
            "-days",
            str(days),
            "-extfile",
            str(ext),
            "-out",
            str(crt),
        )
        csr.unlink()
        ext.unlink()
    for transient in root.glob("*.srl"):
        transient.unlink()


def _pubkey_digest(path: Path, *, certificate: bool) -> str:
    if certificate:
        pem = _run("x509", "-in", str(path), "-pubkey", "-noout")
        with tempfile.TemporaryDirectory() as temp:
            pub = Path(temp) / "pub.pem"
            pub.write_text(pem, encoding="ascii")
            der = subprocess.run(
                ["openssl", "pkey", "-pubin", "-in", str(pub), "-outform", "DER"],
                check=True,
                capture_output=True,
            ).stdout
    else:
        pem = _run("pkey", "-in", str(path), "-pubout")
        with tempfile.TemporaryDirectory() as temp:
            pub = Path(temp) / "pub.pem"
            pub.write_text(pem, encoding="ascii")
            der = subprocess.run(
                ["openssl", "pkey", "-pubin", "-in", str(pub), "-outform", "DER"],
                check=True,
                capture_output=True,
            ).stdout
    return hashlib.sha256(der).hexdigest()


def verify(root: Path, *, windows_acl_verified: bool) -> None:
    ca = root / "ca.crt"
    if not ca.is_file():
        raise ValueError("missing staging CA certificate")
    for service in SERVICES:
        crt = root / f"{service}.crt"
        key = root / f"{service}.key"
        if not crt.is_file() or not key.is_file():
            raise ValueError(f"missing certificate or key for {service}")
        _run("verify", "-CAfile", str(ca), "-verify_hostname", service, str(crt))
        _run("x509", "-in", str(crt), "-checkend", "0", "-noout")
        if _pubkey_digest(crt, certificate=True) != _pubkey_digest(key, certificate=False):
            raise ValueError(f"certificate/key mismatch for {service}")
        _private(key, windows_acl_verified=windows_acl_verified)


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("provision", "verify"):
        item = sub.add_parser(command)
        item.add_argument("--tls-dir", type=Path, required=True)
        item.add_argument("--windows-acl-verified", action="store_true")
        if command == "provision":
            item.add_argument("--days", type=int, default=30)
    args = parser.parse_args()
    try:
        if args.command == "provision":
            provision(args.tls_dir, days=args.days, windows_acl_verified=args.windows_acl_verified)
        else:
            verify(args.tls_dir, windows_acl_verified=args.windows_acl_verified)
    except (OSError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"staging TLS validation failed: {exc}", file=sys.stderr)
        return 1
    print(f"staging TLS {args.command} passed: CA chain, SAN, expiry, key match")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
