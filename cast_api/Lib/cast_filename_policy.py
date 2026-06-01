"""Cast hub transfer filename allowlist and double-extension checks."""

from __future__ import annotations

import os
import re
from typing import Iterable, List, Optional, Sequence, Tuple

# Longest suffixes first (compound extensions before simple ones).
_DEFAULT_ALLOWED_SUFFIXES: Tuple[str, ...] = (
    ".nii.gz",
    ".tar.gz",
    ".dicom",
    ".jpeg",
    ".tiff",
    ".nrrd",
    ".nii",
    ".dcm",
    ".dic",
    ".png",
    ".jpg",
    ".tif",
    ".bmp",
    ".zip",
    ".tar",
    ".gz",
)

# Inner dotted segments that must never appear before the allowed outer suffix.
_DANGEROUS_INNER_PARTS = frozenset(
    {
        "exe",
        "com",
        "scr",
        "pif",
        "msi",
        "msp",
        "bat",
        "cmd",
        "ps1",
        "psm1",
        "vbs",
        "vbe",
        "js",
        "jse",
        "wsf",
        "wsh",
        "hta",
        "jar",
        "dll",
        "sys",
        "drv",
        "ocx",
        "sh",
        "bash",
        "zsh",
        "php",
        "asp",
        "aspx",
        "jsp",
        "py",
        "rb",
        "pl",
        "reg",
        "inf",
        "lnk",
        "app",
        "deb",
        "rpm",
        "dmg",
        "apk",
    }
)

_PATH_UNSAFE = re.compile(r"[/\\]|\.\.")


class FilenamePolicyError(ValueError):
    """Raised when a transfer filename fails hub policy."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "unsupported_file_extension",
        file_name: str = "",
    ) -> None:
        super().__init__(message)
        self.code = code
        self.file_name = file_name


def is_filename_policy_enabled() -> bool:
    """False when ``CAST_HUB_FILENAME_POLICY`` is ``off`` / ``0`` / ``false``."""
    raw = os.getenv("CAST_HUB_FILENAME_POLICY")
    if raw is None:
        return True
    return raw.strip().lower() not in ("0", "false", "no", "off")


def allowed_transfer_suffixes() -> Tuple[str, ...]:
    """Configured allowlist (longest suffixes first)."""
    raw = os.getenv("CAST_HUB_ALLOWED_EXTENSIONS", "").strip()
    if not raw:
        return _DEFAULT_ALLOWED_SUFFIXES
    parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
    normalized: List[str] = []
    for part in parts:
        if not part.startswith("."):
            part = "." + part
        normalized.append(part)
    return tuple(sorted(set(normalized), key=len, reverse=True))


def _normalize_basename(file_name: str) -> str:
    name = (file_name or "").strip()
    if not name:
        raise FilenamePolicyError(
            "transfer filename is empty",
            code="empty_file_name",
            file_name=file_name,
        )
    if "\x00" in name:
        raise FilenamePolicyError(
            "transfer filename contains null byte",
            code="invalid_file_name",
            file_name=file_name,
        )
    if _PATH_UNSAFE.search(name):
        raise FilenamePolicyError(
            "transfer filename must be a plain basename without path segments",
            code="invalid_file_name",
            file_name=file_name,
        )
    base = os.path.basename(name.replace("\\", "/"))
    if base.startswith("."):
        raise FilenamePolicyError(
            "transfer filename must not start with '.'",
            code="invalid_file_name",
            file_name=file_name,
        )
    if len(base) > 255:
        raise FilenamePolicyError(
            "transfer filename is too long",
            code="invalid_file_name",
            file_name=file_name,
        )
    return base


def _match_allowed_suffix(basename_lower: str, allowed: Sequence[str]) -> Optional[str]:
    for suffix in allowed:
        if basename_lower.endswith(suffix):
            stem = basename_lower[: -len(suffix)]
            if stem and stem != ".":
                return suffix
            raise FilenamePolicyError(
                f"transfer filename has no name before suffix {suffix!r}",
                code="invalid_file_name",
                file_name=basename_lower,
            )
    return None


def _check_double_extension(stem: str, *, file_name: str) -> None:
    if "." not in stem:
        return
    for part in stem.split("."):
        if not part:
            continue
        if part.lower() in _DANGEROUS_INNER_PARTS:
            raise FilenamePolicyError(
                f"transfer filename has disallowed inner extension .{part}",
                code="double_extension",
                file_name=file_name,
            )


def validate_transfer_filename(file_name: str) -> None:
    """Allowlist + double-extension check. Raises ``FilenamePolicyError``."""
    if not is_filename_policy_enabled():
        return
    base = _normalize_basename(file_name)
    lower = base.lower()
    allowed = allowed_transfer_suffixes()
    matched = _match_allowed_suffix(lower, allowed)
    if matched is None:
        raise FilenamePolicyError(
            f"transfer filename suffix not in allowlist: {base!r}",
            code="unsupported_file_extension",
            file_name=base,
        )
    stem = lower[: -len(matched)]
    _check_double_extension(stem, file_name=base)


def validate_transfer_filenames(file_names: Iterable[str]) -> None:
    for name in file_names:
        if name and str(name).strip():
            validate_transfer_filename(str(name))


def collect_transfer_file_names(notification: dict) -> List[str]:
    """Gather ``fileName`` values from binary publish / STOW ``context.files[]``."""
    names: List[str] = []
    event = notification.get("event") if isinstance(notification, dict) else None
    if not isinstance(event, dict):
        return names
    ctx = event.get("context")
    if isinstance(ctx, dict):
        files = ctx.get("files")
        if isinstance(files, list):
            for entry in files:
                if isinstance(entry, dict):
                    fn = str(entry.get("fileName") or "").strip()
                    if fn:
                        names.append(fn)
    return names


def enforce_transfer_filenames_for_notification(
    notification: dict,
    *,
    require_name: bool = False,
) -> None:
    if not is_filename_policy_enabled():
        return
    names = collect_transfer_file_names(notification)
    if require_name and not names:
        raise FilenamePolicyError(
            "binary transfer requires context.files[].fileName",
            code="missing_file_name",
        )
    validate_transfer_filenames(names)
