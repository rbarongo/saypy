#!/usr/bin/env python
"""
CLI helper for managing encrypted .env values (see app/env_crypto.py for the
full security model).

Usage:
    python manage_secrets.py genkey
        Generate a new Fernet key and print it. Save it to .secrets.key at the
        project root (or wherever ENV_SECRETS_KEY_FILE points), and NEVER commit
        that file or store it alongside the .env files it protects.

    python manage_secrets.py encrypt "<plaintext>"
        Encrypt a value using the currently configured key (ENV_SECRETS_KEY or
        ENV_SECRETS_KEY_FILE / .secrets.key). Prints an "enc:..." string — paste
        that as the value for the corresponding KEY= line in your .env file.

    python manage_secrets.py decrypt "enc:<token>"
        Decrypt an enc:-prefixed value using the currently configured key. Useful
        to sanity-check a value before deploying it.
"""
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]

    if command == "genkey":
        from cryptography.fernet import Fernet
        print(Fernet.generate_key().decode())
        return

    if command == "encrypt":
        if len(sys.argv) < 3:
            print('Usage: python manage_secrets.py encrypt "<plaintext>"')
            sys.exit(1)
        from app.env_crypto import _load_key, encrypt_env_value
        key = _load_key()
        if not key:
            print("No key configured. Run `python manage_secrets.py genkey` first, save the "
                  "output to .secrets.key at the project root, then re-run this command.")
            sys.exit(1)
        print(encrypt_env_value(key, sys.argv[2]))
        return

    if command == "decrypt":
        if len(sys.argv) < 3:
            print('Usage: python manage_secrets.py decrypt "enc:<token>"')
            sys.exit(1)
        from app.env_crypto import decrypt_env_value
        print(decrypt_env_value(sys.argv[2]))
        return

    print(f"Unknown command: {command}\n")
    print(__doc__)
    sys.exit(1)


if __name__ == "__main__":
    main()
