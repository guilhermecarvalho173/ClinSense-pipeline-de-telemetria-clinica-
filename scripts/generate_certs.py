import ipaddress
from datetime import datetime, timedelta, timezone
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa

def generate_mqtt_tls_certs():
    now = datetime.now(timezone.utc)

    # 1. Gerar Chave Privada e Certificado da CA (Autoridade Certificadora ClinSense)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_subject = ca_issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, "BR"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "Sao Paulo"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, "Sao Caetano do Sul"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "ClinSense Fisio Center"),
        x509.NameAttribute(NameOID.COMMON_NAME, "ClinSense-Root-CA"),
    ])
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_subject)
        .issuer_name(ca_issuer)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + timedelta(days=365))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256())
    )

    # 2. Gerar Certificado do Broker Mosquitto (localhost)
    broker_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    broker_subject = x509.Name([
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "ClinSense Fisio Center"),
        x509.NameAttribute(NameOID.COMMON_NAME, "localhost"),
    ])
    broker_cert = (
        x509.CertificateBuilder()
        .subject_name(broker_subject)
        .issuer_name(ca_issuer)
        .public_key(broker_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + timedelta(days=365))
        .add_extension(
            x509.SubjectAlternativeName([
                x509.DNSName("localhost"),
                x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
            ]),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )

    # 3. Gerar 1 Certificado Compartilhado para as 40 Pulseiras ESP32-S3
    shared_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    shared_subject = x509.Name([
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "ClinSense Fisio Center"),
        x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, "Wearables-Fleet-40"),
        x509.NameAttribute(NameOID.COMMON_NAME, "clinsense-esp32-shared-cert"),
    ])
    shared_cert = (
        x509.CertificateBuilder()
        .subject_name(shared_subject)
        .issuer_name(ca_issuer)
        .public_key(shared_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + timedelta(days=365))
        .sign(ca_key, hashes.SHA256())
    )

    files = {
        "certs/ca.crt": ca_cert.public_bytes(serialization.Encoding.PEM),
        "certs/ca.key": ca_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        ),
        "certs/broker.crt": broker_cert.public_bytes(serialization.Encoding.PEM),
        "certs/broker.key": broker_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        ),
        "certs/shared-device.crt": shared_cert.public_bytes(serialization.Encoding.PEM),
        "certs/shared-device.key": shared_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        ),
    }

    for path, content in files.items():
        with open(path, "wb") as f:
            f.write(content)
        print(f"[TLS] Gerado: {path}")

if __name__ == "__main__":
    generate_mqtt_tls_certs()