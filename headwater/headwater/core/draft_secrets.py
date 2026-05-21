"""Encrypted local draft-secret storage for setup flows."""

from __future__ import annotations

import json
import os
from contextlib import suppress
from pathlib import Path

from headwater.core.config import get_settings


class DraftSecretDependencyError(RuntimeError):
    """Raised when encrypted draft support dependencies are unavailable."""


class DraftSecretStore:
    """Persist small secret payloads to encrypted files on local disk."""

    def __init__(self) -> None:
        self._settings = get_settings()

    def load(self, name: str) -> dict | None:
        path = self._draft_path(name)
        if not path.exists():
            return None
        payload = self._fernet().decrypt(path.read_bytes())
        data = json.loads(payload.decode("utf-8"))
        return data if isinstance(data, dict) else None

    def save(self, name: str, payload: dict) -> None:
        self._settings.ensure_dirs()
        self._settings.setup_drafts_path.mkdir(parents=True, exist_ok=True)
        path = self._draft_path(name)
        token = self._fernet().encrypt(json.dumps(payload).encode("utf-8"))
        path.write_bytes(token)
        _chmod_private(path)

    def delete(self, name: str) -> None:
        path = self._draft_path(name)
        if path.exists():
            path.unlink()

    def _draft_path(self, name: str) -> Path:
        safe_name = "".join(ch for ch in name if ch.isalnum() or ch in ("-", "_")).strip()
        return self._settings.setup_drafts_path / f"{safe_name or 'draft'}.enc"

    def _fernet(self):
        fernet_cls = _load_fernet()
        env_key = os.environ.get("HEADWATER_DRAFT_SECRET_KEY")
        if env_key:
            return fernet_cls(env_key.encode("utf-8"))

        key_path = self._settings.setup_draft_key_path
        self._settings.ensure_dirs()
        if key_path.exists():
            return fernet_cls(key_path.read_bytes())

        self._settings.setup_drafts_path.mkdir(parents=True, exist_ok=True)
        key = fernet_cls.generate_key()
        key_path.write_bytes(key)
        _chmod_private(key_path)
        return fernet_cls(key)


def _chmod_private(path: Path) -> None:
    with suppress(OSError):
        os.chmod(path, 0o600)


def _load_fernet():
    try:
        from cryptography.fernet import Fernet
    except ModuleNotFoundError as exc:
        raise DraftSecretDependencyError(
            "Encrypted draft-secret storage requires the 'cryptography' package. "
            "Install project dependencies and restart the API."
        ) from exc
    return Fernet
