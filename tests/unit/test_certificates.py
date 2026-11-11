"""A certificate that fails verification, over real TLS on this machine: what httpx raises, and what the SDK does.

The certificates are made here with the `cryptography` package, in a temporary directory that is gone again before the
first test runs. Where the package is not installed, these tests are skipped.
"""

from __future__ import annotations

import asyncio
import datetime
import http.server
import ipaddress
import ssl
import tempfile
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest

from rask_sdk import AsyncRask, Rask, RaskConnectionError

KEY = "raskai_test_key_unit"

# OpenSSL's verification codes, which ssl.SSLCertVerificationError carries as verify_code.
FAILURES = {
    "self-signed": 18,
    "of an unknown issuer": 20,
    "expired": 10,
    "for another host name": 64,
}


class Job(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        body = b'{"object": "job", "id": "j", "status": "succeeded"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        pass


class Server(http.server.HTTPServer):
    """Counts the connections it is offered: every attempt of the SDK is one, however its handshake ends."""

    connections = 0

    def get_request(self) -> Any:
        self.connections += 1
        return super().get_request()

    def handle_error(self, request: Any, client_address: Any) -> None:
        pass


@dataclass
class Endpoint:
    server: Server
    verify: ssl.SSLContext | bool

    @property
    def base_url(self) -> str:
        return f"https://127.0.0.1:{self.server.server_address[1]}/v3"


def issue(
    directory: Path,
    name: str,
    *,
    hosts: list[Any] | None,
    starts: datetime.datetime,
    ends: datetime.datetime,
    issuer: tuple[Any, Any] | None = None,
) -> tuple[Path, Path, tuple[Any, Any]]:
    """A throwaway key and its certificate, as two files. No hosts makes an authority; no issuer signs it by itself."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, f"rask-sdk test {name}")])
    issuer_subject, issuer_key = issuer or (subject, key)
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer_subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(starts)
        .not_valid_after(ends)
        .add_extension(x509.BasicConstraints(ca=hosts is None, path_length=None), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(issuer_key.public_key()), critical=False)
    )
    if hosts is None:
        usage = x509.KeyUsage(
            digital_signature=False,
            content_commitment=False,
            key_encipherment=False,
            data_encipherment=False,
            key_agreement=False,
            key_cert_sign=True,
            crl_sign=True,
            encipher_only=False,
            decipher_only=False,
        )
        builder = builder.add_extension(usage, critical=True)
    else:
        builder = builder.add_extension(x509.SubjectAlternativeName(hosts), critical=False)
    certificate = directory / f"{name}.pem"
    certificate.write_bytes(builder.sign(issuer_key, hashes.SHA256()).public_bytes(serialization.Encoding.PEM))
    key_file = directory / f"{name}.key"
    key_file.write_bytes(
        key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    )
    return certificate, key_file, (subject, key)


def serve(certificate: Path, key_file: Path) -> Server:
    server = Server(("127.0.0.1", 0), Job)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificate, key_file)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True).start()
    return server


@pytest.fixture(scope="module")
def endpoints() -> Iterator[dict[str, Endpoint]]:
    x509 = pytest.importorskip("cryptography.x509", reason="The certificates are made with the cryptography package.")
    now = datetime.datetime.now(datetime.UTC)
    day = datetime.timedelta(days=1)
    here = [x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
    found: dict[str, Endpoint] = {}
    with tempfile.TemporaryDirectory() as temporary:
        directory = Path(temporary)
        authority, _, signer = issue(directory, "authority", hosts=None, starts=now - day, ends=now + day)
        trusting = ssl.create_default_context(cafile=str(authority))

        def endpoint(name: str, verify: ssl.SSLContext | bool, **how: Any) -> None:
            certificate, key_file, _ = issue(directory, name.replace(" ", "-"), **how)
            found[name] = Endpoint(serve(certificate, key_file), verify)

        endpoint("trusted", trusting, hosts=here, starts=now - day, ends=now + day, issuer=signer)
        # These two meet a client that has never heard of the authority: httpx's own default.
        endpoint("self-signed", True, hosts=here, starts=now - day, ends=now + day)
        endpoint("of an unknown issuer", True, hosts=here, starts=now - day, ends=now + day, issuer=signer)
        endpoint("expired", trusting, hosts=here, starts=now - 3 * day, ends=now - day, issuer=signer)
        elsewhere = [x509.DNSName("elsewhere.test")]
        endpoint("for another host name", trusting, hosts=elsewhere, starts=now - day, ends=now + day, issuer=signer)
    # The key files are gone by now: each context holds what it loaded.
    yield found
    for one in found.values():
        one.server.shutdown()
        one.server.server_close()


def on_client(kind: str, endpoint: Endpoint, use: Callable[[Any], Any]) -> Any:
    """What `use` makes of the sync or the async client, over the endpoint's TLS. Two retries, if it retries at all."""
    if kind == "sync":
        with httpx.Client(verify=endpoint.verify, trust_env=False) as http:
            return use(Rask(api_key=KEY, base_url=endpoint.base_url, http_client=http, max_retries=2))

    async def drive() -> Any:
        async with httpx.AsyncClient(verify=endpoint.verify, trust_env=False) as http:
            return await use(AsyncRask(api_key=KEY, base_url=endpoint.base_url, http_client=http, max_retries=2))

    return asyncio.run(drive())


def verification_error(failure: BaseException | None) -> ssl.SSLCertVerificationError:
    """The ssl error behind what httpx raised, wherever it is in the chain."""
    for _ in range(10):
        if failure is None:
            break
        if isinstance(failure, ssl.SSLCertVerificationError):
            return failure
        failure = failure.__cause__ or failure.__context__
    raise AssertionError("No ssl.SSLCertVerificationError behind the failure.")


CALLS = {
    "a call": lambda client: client.account.get_me(),
    "jobs.wait": lambda client: client.jobs.wait("j", timeout=3),
}


@pytest.mark.parametrize("kind", ["sync", "async"])
def test_a_certificate_the_client_trusts_is_served(endpoints: dict[str, Endpoint], kind: str) -> None:
    """The other tests fail on the certificate and on nothing else about these endpoints."""
    job = on_client(kind, endpoints["trusted"], CALLS["jobs.wait"])
    assert job.status == "succeeded"


@pytest.mark.parametrize("kind", ["sync", "async"])
@pytest.mark.parametrize("call", CALLS)
@pytest.mark.parametrize("certificate", FAILURES)
def test_a_certificate_that_fails_verification_ends_at_the_first_attempt(
    endpoints: dict[str, Endpoint], certificate: str, call: str, kind: str
) -> None:
    endpoint = endpoints[certificate]
    before = endpoint.server.connections
    with pytest.raises(RaskConnectionError) as caught:
        on_client(kind, endpoint, CALLS[call])
    assert endpoint.server.connections - before == 1
    # What httpx reports, the same on both clients: a ConnectError that says so, with the ssl error behind it.
    failure = caught.value.__cause__
    assert isinstance(failure, httpx.ConnectError)
    assert "CERTIFICATE_VERIFY_FAILED" in str(failure)
    assert verification_error(failure).verify_code == FAILURES[certificate]
