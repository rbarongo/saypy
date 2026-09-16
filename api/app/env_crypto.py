"""
Helpers for encrypting/decrypting sensitive .env values (DB passwords, JWT
secret, seed admin password) — same scheme used on the task_monitor project.

Encrypted values are stored in .env files as:

    DATABASE_URL=enc:<fernet-token>

and transparently decrypted wherever `app.settings.Settings` loads them (see
the `_decrypt_secret` validator there). A plain, non-"enc:"-prefixed value
passes through unchanged, so this is fully backward compatible with the
existing plaintext api/.env used for local (non-Docker) dev — nothing
breaks if you never touch this.

SECURITY MODEL — read before relying on this:
This only protects a secret if the decryption key is kept SEPARATE from the
.env file(s) that hold the encrypted value — different storage, different
access list, never committed together. The key lives in a single file,
".secrets.key", at the project root, which is gitignored and must be
provisioned on each machine/server out-of-band (scp/SSH, a password manager,
your CI's secret store, etc.) — never by copying it alongside a .env file.
If the key sits right next to the .env file it decrypts (e.g. both
committed, both copied together, both readable by the same people), this
only obfuscates the value rather than actually protecting it — anyone who
can read the ciphertext can also read the key.

For a small self-hosted deployment like this one, this is a meaningful step
up from plaintext secrets. It's still not a substitute for a real secrets
manager (Vault, AWS/GCP Secrets Manager, etc.) if you later need audit
logging, key rotation, or per-secret access control.
"""
import os
from pathlib import Path

ENC_PREFIX = "enc:"

# .../saypy-collection (repo root — two levels up from api/app/)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_DEFAULT_KEY_FILE = _PROJECT_ROOT / ".secrets.key"


def _load_key():
    """
    Load the Fernet key, checked in priority order:
      1. ENV_SECRETS_KEY — the raw key, set directly in the process/shell/CI
         environment (never written into a .env file).
      2. ENV_SECRETS_KEY_FILE — path to a file containing the key (defaults
         to "<project root>/.secrets.key" if that file exists).
    Returns None if no key is configured anywhere.
    """
    key = os.environ.get("ENV_SECRETS_KEY")
    if key:
        return key.strip().encode()

    key_file = os.environ.get("ENV_SECRETS_KEY_FILE")
    path = Path(key_file) if key_file else _DEFAULT_KEY_FILE
    if path.exists():
        content = path.read_text().strip()
        if content:
            return content.encode()

    return None


def decrypt_env_value(value):
    """Decrypt a value if it's enc:-prefixed; return it unchanged otherwise."""
    if value is None or not isinstance(value, str) or not value.startswith(ENC_PREFIX):
        return value

    token = value[len(ENC_PREFIX):]
    key = _load_key()
    if not key:
        raise RuntimeError(
            "Found an encrypted .env value (enc:...) but no decryption key is "
            "configured. Set ENV_SECRETS_KEY or ENV_SECRETS_KEY_FILE, or make sure "
            f"{_DEFAULT_KEY_FILE} exists."
        )

    from cryptography.fernet import Fernet, InvalidToken
    try:
        return Fernet(key).decrypt(token.encode()).decode()
    except InvalidToken:
        raise RuntimeError(
            "Failed to decrypt an .env value — the configured key does not match "
            "the one it was encrypted with, or the value is corrupted."
        )


def encrypt_env_value(key, plaintext):
    """Encrypt a plaintext string, returning the enc:-prefixed value to store in .env."""
    from cryptography.fernet import Fernet
    token = Fernet(key).encrypt(plaintext.encode()).decode()
    return ENC_PREFIX + token
