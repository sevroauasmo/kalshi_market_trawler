import base64
import time
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


def load_private_key(key_path: str):
    key_data = Path(key_path).read_bytes()
    return serialization.load_pem_private_key(key_data, password=None)


def sign_request(private_key, timestamp_ms: int, method: str, path: str) -> str:
    """Sign a Kalshi API request using RSA-PSS.

    The signature covers: timestamp + method + path (no query params).
    """
    message = f"{timestamp_ms}{method}{path}".encode()
    signature = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH,
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode()
