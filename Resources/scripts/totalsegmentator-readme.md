# TotalSegmentator Cast service provider

## Cast Interface setup

In **Service Providers**, add or edit a row:

| Field | Value |
|-------|--------|
| Product | `TOTALSEG` |
| Version | `1.0` |
| Description | e.g. Total Segmentator CT segmentation |
| Hub | `SLICER-HUB`, `VOLVIEW-HUB`, or your cloud hub |
| onMessage script | `CastInterface/Resources/scripts/total_segmentator.py` |

Click **Connect**. The subscriber name (`TOTALSEG-XXXXXX`) appears only in the **hub admin portal**. Hub events subscribed for `TOTALSEG`: `dicom-send`, `nifti-send`, `dicomtransfer-request`.

**Disconnect the AIBRAIN provider** while testing TotalSegmentator. If both are connected, AIBRAIN immediately publishes the demo `ai-results-mrbrain.dcm` on every `dicom-send` (the Cast module now skips that when multiple providers are connected, but using one provider avoids confusion).

Requires the **TotalSegmentator** Slicer extension (Python package `totalsegmentator`) and `rt_utils` for DICOM RT Struct output.

Inference runs in a **separate `PythonSlicer` process** (TotalSegmentator CLI), matching the Slicer extension. This avoids Windows nnU-Net multiprocessing failures inside the live Slicer GUI process.

## Input expectations

### `dicomtransfer-request` / negotiated `dicom-send`

- VolView sends **`dicomtransfer-request`** with a per-slice manifest (`dicomTransferId`, `files[]`).
- This script replies with **`dicomtransfer-response`** with **`REJECT` on every file** (negotiated transfer disabled; VolView sends no DICOM payloads).
- Legacy **`dicom-send`** without `dicomTransferId` and **`nifti-send`** are unchanged below.
- When ACCEPT is re-enabled later, negotiated flow will stage by `dicomTransferId` and run on `complete` (no 3s topic debounce).

### Legacy `dicom-send` (no `dicomTransferId`)

- Each Cast `dicom-send` carries one DICOM file (or one `.zip` of slices).
- The script **accumulates** files per `hub.topic` under a temp staging folder.
- Files are staged only; segmentation is **not** started automatically (use negotiated transfer + ``complete``).
- Send a **complete CT series** (many slices) or one zip per study; a single slice is unlikely to work.

### `nifti-send` (e.g. from VolView)

- One compressed NIfTI volume (`.nii.gz`) per message — whole study in one file.
- Segmentation runs when the NIfTI file is received.
- VolView publishes with `target.product.name` = `TOTALSEG`.

## Output

- Uses TotalSegmentator `output_type="dicom"` (DICOM **RT Struct**), typically `segmentations.dcm`.
- Publishes that file back on the **same hub topic** as a `dicom-send` event.

## Logs

Logger name: `CastInterface.TotalSegmentator` (Slicer Python console / log).
