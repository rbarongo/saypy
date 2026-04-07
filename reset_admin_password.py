#!/usr/bin/env python3
"""
reset_admin_password.py — Emergency recovery tool for the saypy_admin account.

Run from the project root (the directory that contains the `backend/` folder):

    python reset_admin_password.py

The script will:
  1. Connect to the same database configured in backend/.env.
  2. Verify that the saypy_admin user exists.
  3. Prompt for a new password twice (hidden input).
  4. Enforce the app password policy (≥8 chars, 1 letter, 1 number, 1 special char).
  5. Atomically update the password hash and salt.

No web server needs to be running.
"""

import sys
import os
import getpass
import hashlib
import binascii

# ── ensure the backend package is importable ──────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, "backend", ".env"))
load_dotenv()

from sqlalchemy import text
from backend.db import engine, password_policy_error

TARGET_USERNAME = "saypy_admin"


def _hash_password(password: str, salt: bytes) -> str:
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 100_000)
    return binascii.hexlify(dk).decode("ascii")


def user_exists() -> bool:
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id FROM users WHERE username = :u"),
            {"u": TARGET_USERNAME},
        ).fetchone()
    return row is not None


def reset_password(new_password: str) -> None:
    salt = os.urandom(16)
    ph = _hash_password(new_password, salt)
    salt_hex = binascii.hexlify(salt).decode("ascii")
    with engine.begin() as conn:
        result = conn.execute(
            text(
                "UPDATE users SET password_hash = :ph, salt = :s "
                "WHERE username = :u"
            ),
            {"ph": ph, "s": salt_hex, "u": TARGET_USERNAME},
        )
        if result.rowcount == 0:
            raise RuntimeError(
                f"No rows updated — user '{TARGET_USERNAME}' not found in the database."
            )


def main():
    print("=" * 60)
    print("  Saypy — Admin Password Reset")
    print("=" * 60)

    # 1. Confirm the account exists before asking for a new password
    try:
        found = user_exists()
    except Exception as e:
        print(f"\n[ERROR] Could not connect to the database: {e}")
        sys.exit(1)

    if not found:
        print(f"\n[ERROR] User '{TARGET_USERNAME}' does not exist in the database.")
        print("        Create the account first via the application, then retry.")
        sys.exit(1)

    print(f"\nUser '{TARGET_USERNAME}' found.")
    print("\nPassword requirements: ≥8 characters, at least 1 letter,")
    print("  1 number, and 1 special character (e.g. @, !, #, ...).\n")

    # 2. Prompt for new password (up to 3 attempts)
    for attempt in range(1, 4):
        try:
            new_pw = getpass.getpass("Enter new password   : ")
            confirm = getpass.getpass("Confirm new password : ")
        except (KeyboardInterrupt, EOFError):
            print("\nAborted.")
            sys.exit(0)

        if new_pw != confirm:
            print(f"  [!] Passwords do not match. ({attempt}/3)\n")
            continue

        policy_msg = password_policy_error(new_pw)
        if policy_msg:
            print(f"  [!] {policy_msg} ({attempt}/3)\n")
            continue

        # 3. Apply the change
        try:
            reset_password(new_pw)
        except Exception as e:
            print(f"\n[ERROR] Failed to update password: {e}")
            sys.exit(1)

        print(f"\n[OK] Password for '{TARGET_USERNAME}' has been reset successfully.")
        sys.exit(0)

    print("\n[ERROR] Too many failed attempts. Password was NOT changed.")
    sys.exit(1)


if __name__ == "__main__":
    main()
