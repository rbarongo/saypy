"""Application configuration, loaded from api/.env.

Named `settings.py` (not `config.py`) deliberately — the repo-root .gitignore
has a blanket `config.py` rule left over from an unrelated project and would
silently exclude a file with that name from version control.
"""
import os
from functools import lru_cache

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.env_crypto import decrypt_env_value

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=os.path.join(BASE_DIR, ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = "postgresql+psycopg://postgres:postgres@localhost:5432/saypy_core"

    jwt_secret_key: str = "insecure-dev-secret-change-me"
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 480

    cors_origins: str = "*"

    seed_admin_username: str = "admin"
    seed_admin_password: str = "ChangeMe123!"
    seed_church_names: str = "Kibada,KCC,Mwera,Goroka"

    # Any of these three may be stored in .env as "enc:<fernet-token>" instead
    # of plaintext (see app/env_crypto.py / manage_secrets.py) — decrypted
    # here, once, so every other module just reads settings.* normally and
    # never has to know or care whether a value was encrypted at rest.
    @field_validator("database_url", "jwt_secret_key", "seed_admin_password", mode="before")
    @classmethod
    def _decrypt_secret(cls, v):
        return decrypt_env_value(v)

    # Where uploaded files (e.g. collection payment-evidence slips) are
    # written — never served from a public static mount; always streamed
    # back through an authenticated, scope-checked endpoint.
    upload_dir: str = os.path.join(BASE_DIR, "uploads")

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    @property
    def seed_church_name_list(self) -> list[str]:
        return [n.strip() for n in self.seed_church_names.split(",") if n.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
