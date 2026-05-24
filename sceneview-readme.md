# SCENEVIEW (removed hub short-circuit)

This document describes the **local MRML sceneview shortcut** that used to live in
the VolView Cast hub (`VolView/server/cast_api/cast_api.py`). That shortcut was
removed so scene layout/camera state is served only through the normal Cast
request/response flow from a connected 3D Slicer subscriber.

## Why it existed

During early integration, callers could `POST /api/hub/request` with
`subscriber: "3DSLICER"` and get viewport/camera JSON **without** a live Slicer
WebSocket subscription. The hub read `.mrml` files from a fixed folder and
returned a synthetic collated HTTP response.

## Trigger (removed)

- **Endpoint:** `POST /api/hub/request`
- **Body:** `subscriber` must be `"3DSLICER"` (case-insensitive)
- **`dataType`:** optional; when omitted, audit logged `sceneview-request`
  (`SCENEVIEW`)
- **No WebSocket:** `connected: false`, `requestId: null`, immediate return

## Data source

Folder next to the hub script:

```
VolView/server/cast_api/sceneview/*.mrml
```

Selection rules:

1. Glob `*.mrml` in that folder
2. Prefer files whose basename contains `"Scene"`
3. Otherwise use all `.mrml` files
4. Pick the file with the newest modification time

## MRML → JSON mapping

Parsed with `xml.etree.ElementTree`:

| MRML element | JSON |
|--------------|------|
| `<Camera>` | Indexed by `singletonTag` or `layoutLabel`; fields: `position`, `focalPoint`, `viewUp`, `viewAngle`, `parallelProjection`, `parallelScale` |
| `<View>` | `type: "View"`, plus `id`, `name`, `layoutLabel`, `layoutName`, `fieldOfView`; if layout tag matches a camera, nested `camera` object |
| `<Slice>` | `type: "Slice"`, plus `id`, `name`, `layoutLabel`, `layoutName`, `orientation`, `fieldOfView`, `dimensions`, `sliceToRAS` |

Success payload:

```json
{
  "source": "sceneview",
  "file": "MyScene.mrml",
  "viewports": [ { "type": "View", ... }, { "type": "Slice", ... } ]
}
```

When no file could be loaded:

```json
{
  "error": "No sceneview data found",
  "folder": "/path/to/cast_api/sceneview"
}
```

## HTTP response shape (synthetic)

Same collated envelope as a normal hub reply:

```json
{
  "ok": true,
  "requestId": null,
  "subscriber": "3DSLICER",
  "dataType": null,
  "responses": [
    {
      "id": "<uuid>",
      "subscriber": "3DSLICER",
      "actor": null,
      "productName": null,
      "data": { "source": "sceneview", ... }
    }
  ],
  "expected": ["3DSLICER"],
  "missing": [],
  "timedOut": false,
  "exists": true,
  "connected": false
}
```

## Audit log

`event_data` included:

- `"status": "local-sceneview"`
- `"response"`: the parsed sceneview payload (or error object)

The admin UI treated `local-sceneview` as responding subscriber `3DSLICER`.

## Replacement (CastInterface)

Use the standard Cast protocol:

| Item | Value |
|------|--------|
| `dataType` | `SCENEVIEW` (required on `POST /api/hub/request`) |
| Request event | `sceneview-request` |
| Response event | `sceneview-response` |
| Hub behavior | Fan-out to all connected subscriptions matching `(topic, actor[, productName])` |

Implement handling in CastInterface (e.g. provider `onMessage` script or
`service_provider_hub.py`) by:

1. Subscribing to the hub with `productName` such as `3DSLICER`
2. On `sceneview-request`, building the same `viewports` structure from the
   **live** Slicer scene (MRML scene view nodes) instead of a folder on the hub
3. Publishing `sceneview-response` with that payload in `data`

Event-name helpers must stay in sync with
`CastInterface/Lib/cast_client.py`, `VolView/src/io/cast/event-names.ts`, and
`vtk-js/Sources/IO/Core/CastClient/eventNames.js`.

## Reference implementation (removed from hub)

```python
def _load_sceneview_from_folder(folder_path: str) -> Optional[Dict[str, Any]]:
    """Load View, Slice, and Camera data from the most recent *Scene*.mrml or *.mrml in folder_path."""
    if not folder_path or not os.path.isdir(folder_path):
        return None
    pattern = os.path.join(folder_path, "*.mrml")
    files = glob.glob(pattern)
    if not files:
        return None
    scene_files = [f for f in files if "Scene" in os.path.basename(f)]
    candidates = scene_files if scene_files else files
    best = max(candidates, key=lambda f: os.path.getmtime(f))
    try:
        tree = ET.parse(best)
        root = tree.getroot()
    except ET.ParseError:
        return None

    cameras_by_tag: Dict[str, Dict[str, Any]] = {}
    for cam in root.findall("Camera"):
        tag = (cam.get("singletonTag") or cam.get("layoutLabel") or "").strip()
        cameras_by_tag[tag] = {
            "position": cam.get("position"),
            "focalPoint": cam.get("focalPoint"),
            "viewUp": cam.get("viewUp"),
            "viewAngle": cam.get("viewAngle"),
            "parallelProjection": cam.get("parallelProjection"),
            "parallelScale": cam.get("parallelScale"),
        }

    viewports: List[Dict[str, Any]] = []
    for view in root.findall("View"):
        tag = (
            view.get("layoutLabel")
            or view.get("layoutName")
            or view.get("singletonTag")
            or ""
        ).strip()
        v: Dict[str, Any] = {
            "type": "View",
            "id": view.get("id"),
            "name": view.get("name"),
            "layoutLabel": view.get("layoutLabel"),
            "layoutName": view.get("layoutName"),
            "fieldOfView": view.get("fieldOfView"),
        }
        if tag and tag in cameras_by_tag:
            v["camera"] = cameras_by_tag[tag]
        viewports.append(v)

    for sl in root.findall("Slice"):
        s: Dict[str, Any] = {
            "type": "Slice",
            "id": sl.get("id"),
            "name": sl.get("name"),
            "layoutLabel": sl.get("layoutLabel"),
            "layoutName": sl.get("layoutName"),
            "orientation": sl.get("orientation"),
            "fieldOfView": sl.get("fieldOfView"),
            "dimensions": sl.get("dimensions"),
            "sliceToRAS": sl.get("sliceToRAS"),
        }
        viewports.append(s)

    return {"source": "sceneview", "file": os.path.basename(best), "viewports": viewports}
```
