"""Shared fixtures for SAML SSO tests: self-signed cert, signed SAML responses.

Generates a self-signed X509 cert + RSA key (IdP signing credential) and builds
signed SAML Response XML fixtures so tests can drive the ACS callback without
any real IdP dependency.
"""

from __future__ import annotations

import base64
import datetime as dt

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

_RSA = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_SUBJECT = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "idp.test.example.com")])
_NOW = dt.datetime.now(dt.UTC)
_CERT_OBJ = (
    x509.CertificateBuilder()
    .subject_name(_SUBJECT)
    .issuer_name(_SUBJECT)
    .public_key(_RSA.public_key())
    .serial_number(x509.random_serial_number())
    .not_valid_before(_NOW - dt.timedelta(days=1))
    .not_valid_after(_NOW + dt.timedelta(days=365))
    .sign(_RSA, hashes.SHA256())
)
IDP_CERT_PEM: str = _CERT_OBJ.public_bytes(serialization.Encoding.PEM).decode("ascii")
IDP_KEY_PEM: str = _RSA.private_bytes(
    serialization.Encoding.PEM,
    serialization.PrivateFormat.TraditionalOpenSSL,
    serialization.NoEncryption(),
).decode("ascii")
IDP_ENTITY_ID: str = "https://idp.test.example.com"
SP_ENTITY_ID: str = "https://sp.test.example.com"
ACS_URL: str = "http://testserver.local/admin/callback"


def _format_cert_for_saml(pem: str) -> str:
    """Strip PEM headers/footers/newlines — python3-saml wants the raw base64 body."""
    lines = [ln.strip() for ln in pem.strip().splitlines() if ln.strip() and "-----" not in ln]
    return "".join(lines)


IDP_CERT_SAML: str = _format_cert_for_saml(IDP_CERT_PEM)

_ASSERTION_TEMPLATE = """\
<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol" \
ID="{response_id}" Version="2.0" IssueInstant="{issue_instant}" Destination="{acs_url}">
  <saml:Issuer xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion">{idp_entity_id}</saml:Issuer>
  <samlp:Status><samlp:StatusCode Value="urn:oasis:names:tc:SAML:2.0:status:Success"/></samlp:Status>
  <saml:Assertion xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion" \
ID="{assertion_id}" Version="2.0" IssueInstant="{issue_instant}">
    <saml:Issuer>{idp_entity_id}</saml:Issuer>
    <saml:Subject>
      <saml:NameID Format="urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified">{nameid}</saml:NameID>
      <saml:SubjectConfirmation Method="urn:oasis:names:tc:SAML:2.0:cm:bearer">
        <saml:SubjectConfirmationData NotOnOrAfter="{not_on_or_after}" Recipient="{acs_url}"/>
      </saml:SubjectConfirmation>
    </saml:Subject>
    <saml:Conditions NotBefore="{not_before}" NotOnOrAfter="{not_on_or_after}">
      <saml:AudienceRestriction><saml:Audience>{sp_entity_id}</saml:Audience></saml:AudienceRestriction>
    </saml:Conditions>
    <saml:AuthnStatement AuthnInstant="{issue_instant}" SessionIndex="{session_index}">
      <saml:AuthnContext>
        <saml:AuthnContextClassRef>urn:oasis:names:tc:SAML:2.0:ac:classes:PasswordProtectedTransport</saml:AuthnContextClassRef>
      </saml:AuthnContext>
    </saml:AuthnStatement>
{attribute_statement}  </saml:Assertion>
</samlp:Response>
"""


def _attr_xml(name: str, values: list[str]) -> str:
    items = "".join(f"      <saml:AttributeValue>{v}</saml:AttributeValue>" for v in values)
    return f'    <saml:Attribute Name="{name}">\n{items}\n    </saml:Attribute>\n'


def build_saml_response(
    *,
    nameid: str = "user-saml-1",
    attributes: dict[str, list[str]] | None = None,
    sign: bool = True,
) -> str:
    """Build a SAML Response (base64-encoded), optionally with a signed assertion."""
    from onelogin.saml2.utils import OneLogin_Saml2_Utils

    now = dt.datetime.now(dt.UTC)
    issue_instant = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    not_before = (now - dt.timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    not_on_or_after = (now + dt.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

    # python3-saml strict mode requires an AttributeStatement; always include
    # the email attribute plus any caller-supplied attributes.
    all_attrs: dict[str, list[str]] = {
        "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress": [
            "user@example.com",
        ],
    }
    if attributes:
        all_attrs.update(attributes)

    attr_stmt = "    <saml:AttributeStatement>\n"
    for name, values in all_attrs.items():
        attr_stmt += _attr_xml(name, values)
    attr_stmt += "    </saml:AttributeStatement>\n"

    xml = _ASSERTION_TEMPLATE.format(
        response_id="_response-" + OneLogin_Saml2_Utils.generate_unique_id(),
        assertion_id="_assertion-" + OneLogin_Saml2_Utils.generate_unique_id(),
        session_index="_session-" + OneLogin_Saml2_Utils.generate_unique_id(),
        issue_instant=issue_instant,
        not_before=not_before,
        not_on_or_after=not_on_or_after,
        acs_url=ACS_URL,
        idp_entity_id=IDP_ENTITY_ID,
        sp_entity_id=SP_ENTITY_ID,
        nameid=nameid,
        attribute_statement=attr_stmt,
    )

    if sign:
        # Sign the assertion node only (python3-saml validates assertion signatures).
        from onelogin.saml2.utils import OneLogin_Saml2_Utils
        from onelogin.saml2.xml_utils import OneLogin_Saml2_XML

        etree = OneLogin_Saml2_XML.to_etree(xml)
        assertion = OneLogin_Saml2_XML.query(etree, "//saml:Assertion")[0]
        signed_assertion = OneLogin_Saml2_Utils.add_sign(
            OneLogin_Saml2_XML.to_string(assertion),
            IDP_KEY_PEM,
            IDP_CERT_PEM,
        )
        # Replace the unsigned assertion with the signed one.
        signed_etree = OneLogin_Saml2_XML.to_etree(signed_assertion)
        old_assertion = OneLogin_Saml2_XML.query(etree, "//saml:Assertion")[0]
        parent = old_assertion.getparent()
        idx = list(parent).index(old_assertion)
        parent.remove(old_assertion)
        parent.insert(idx, signed_etree)
        xml = OneLogin_Saml2_XML.to_string(etree)

    raw = xml if isinstance(xml, bytes) else xml.encode("utf-8")
    return base64.b64encode(raw).decode("ascii")
