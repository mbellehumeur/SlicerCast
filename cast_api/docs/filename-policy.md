# Binary transfer filename policy

The Cast hub enforces an **allowlist** on `resource.fileName` (and
`context.files[].fileName` on negotiated DICOM transfers) when it accepts
**binary payload bytes** (multipart `file` part only; JSON publish with embedded
`resource.data` is rejected for binary-family events).

Metadata-only binary-family events (no bytes) are not checked.

## Default allowed suffixes

Longest match wins (so `study.nii.gz` uses `.nii.gz`, not `.gz`).

| Imaging | Archives / compression |
|---------|-------------------------|
| `.dcm`, `.dicom`, `.dic` | `.zip` |
| `.nii`, `.nii.gz`, `.nrrd` | `.tar`, `.tar.gz`, `.gz` |
| `.png`, `.jpg`, `.jpeg`, `.tif`, `.tiff`, `.bmp` | |

## Double extensions

After the outer allowlisted suffix is matched, any **earlier** dotted segment
must not be a dangerous type (executables, scripts, installers, etc.).

Examples:

| Filename | Result |
|----------|--------|
| `patient.dcm` | Allowed |
| `volume.nii.gz` | Allowed |
| `bundle.tar.gz` | Allowed |
| `study.dcm.exe` | Rejected (`double_extension`) |
| `malware.exe.dcm` | Rejected |
| `../etc/passwd.dcm` | Rejected (`invalid_file_name`) |

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `CAST_HUB_FILENAME_POLICY` | on | Set to `off` to disable checks (dev only). |
| `CAST_HUB_ALLOWED_EXTENSIONS` | (see table) | Comma-separated list, e.g. `.dcm,.nii.gz,.zip` |

## HTTP errors

Rejected publishes return **400** with JSON detail:

```json
{
  "message": "transfer filename suffix not in allowlist: 'report.pdf'",
  "code": "unsupported_file_extension",
  "fileName": "report.pdf"
}
```

Codes include `unsupported_file_extension`, `double_extension`,
`invalid_file_name`, `empty_file_name`, and `missing_file_name`.

## Implementation

Logic lives in `cast_filename_policy.py`. Enforcement runs in
`cast_api.py` before HTTP payload registration and on multipart publish.
