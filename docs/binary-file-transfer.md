# Cast binary file transfer

Animated walkthrough (24 s loop): [images/binary-file-transfer-animated.svg](images/binary-file-transfer-animated.svg).
Static diagram: [images/binary-file-transfer.svg](images/binary-file-transfer.svg).

Cast carries imaging files (DICOM, NIfTI, PNG, NRRD, and similar) through a
Cast hub without putting raw bytes on the WebSocket. The hub is a **router** with a
short-lived in-memory file store—not a PACS or long-term archive.

**Two steps for every file:**

1. **Notify** — Publisher sends metadata (and uploads bytes) over HTTP. The hub
   fans out one **text-only** WebSocket message listing `payloadId` per file.
2. **Download** — Each subscriber pulls bytes when ready via
   `GET /api/hub/payloads/{payloadId}` and attaches them locally to
   `event.context.files[]`.

Subscribers never auto-download inside the Cast client library; the application
(or Slicer `resource_server_hub.py`) calls `fetch_all_payloads` / `fetchAllPayloads`
before handling the event.

Authoritative hub code: `VolView/server/cast_api/cast_api.py`. Matching clients:
vtk-js `Sources/IO/Core/CastClient/`, VolView `src/io/cast-client.ts`, OHIF
`extensions/cast`, Slicer `CastInterface/Lib/cast_client.py`.

Filename rules when storing bytes: `VolView/server/cast_api/filename-policy.md`.

---

## Why split WebSocket and HTTP?

| Concern | Approach |
|---------|----------|
| Keep `/bind/{endpoint}` JSON-safe and small | WS carries metadata + `payloadId` only |
| Move hundreds of DICOM slices efficiently | One publish → one WS message → N parallel GETs |
| Let receivers control memory and timing | Download is explicit, not on every WS frame |
| Resource servers behind firewalls | Outbound WSS + HTTPS only to the hub |

---

## Binary-capable events

An event is binary-capable when `hub.event` (lowercased) equals or starts with:

`dicom-`, `dicom_`, `nifti-`, `nifti_`, `jpg-`, `jpg_`, `png-`, `png_`,
`nrrd-`, `nrrd_`

The prefix list is defined in hub, vtk-js, VolView server, and Slicer client code
and must stay in sync when you add a new type.

Common events:

- **`dicom-send`** — one or many `.dcm` files (VolView study/series send uses one
  batch for all slices in scope)
- **`nifti-send`** — one or more volumes as `context.files[]` (e.g. `.nii.gz`)

---

## Wire format: STOW batch only

All binary publishes use **one** HTTP shape. Legacy `multipart/form-data`
(`message` + `file`) and per-message `resource.payloadId` on the wire are
**removed**; the hub returns HTTP 400 for non-STOW multipart.

### Upload: `POST /api/hub/`

- **Content-Type:** `multipart/related; boundary=…; type="application/dicom"`
- **Authorization:** `Bearer <access_token>` required

| Part | Role |
|------|------|
| **1 — JSON** | `application/dicom+json`: full Cast publish envelope. Bytes must **not** appear in JSON. |
| **2 … N+1** | One body per file, same order as `event.context.files[]` |

### Manifest: `event.context.files[]`

Each entry describes one file before upload:

```json
{
  "fileName": "slice-001.dcm",
  "byteLength": 527198,
  "mimeType": "application/dicom",
  "dicomMetadata": { "00080018": { "vr": "UI", "Value": ["…"] } }
}
```

- **`fileName`** — required for storage (allowlisted suffix; see filename policy)
- **`byteLength`** — must match the following multipart part size
- **`mimeType`** — optional; part may use `application/dicom`,
  `application/octet-stream`, or this value
- **`dicomMetadata`** — optional DICOM JSON tags for resource-server filtering
- Publishers may set **`data`** in memory; the client strips it and sends bytes in
  the multipart parts only

A **single** file is still a STOW batch with **`files.length === 1`**.

### Hub response and fan-out

- HTTP: `{"status":"received"}`
- WebSocket (one message per publish): same Cast envelope, but each `files[]`
  entry gains hub-assigned **`payloadId`** and **`expiresAt`** (no inline bytes)

Example fan-out fragment:

```json
"context": {
  "files": [
    {
      "fileName": "slice-001.dcm",
      "byteLength": 527198,
      "payloadId": "rmRT3Zu3Ju1Riv7ZDX8lbvkVwWoLEonPXHSgjmUELyY",
      "expiresAt": "2026-05-30T12:00:00.000Z"
    }
  ]
}
```

Plain **JSON** `POST /api/hub/` (no multipart) remains for subscribe, typed
requests, and metadata-only notifications. Embedding file bytes in JSON for a
binary-family event is rejected (HTTP 400).

---

## Publisher flow (client)

```mermaid
sequenceDiagram
    participant App as Application
    participant Client as Cast client
    participant Hub as cast_api

    App->>Client: publish(event with context.files[].data)
    Client->>Client: coerce legacy resource.data to files[] if needed
    Client->>Client: strip data from JSON (metadata-only)
    Client->>Hub: POST multipart/related (JSON + N parts)
    Hub->>Hub: store each part, assign payloadId
    Hub-->>Client: {"status":"received"}
    Hub->>Hub: fan-out WS text to subscribers
```

**APIs:**

- **`publish()`** — if any `files[].data` (or legacy `context[].resource.data`) is
  present, coerce to `files[]` and STOW
- **`publishStowBatch()`** / **`publish_stow_batch()`** — direct STOW when the
  manifest is already built

VolView examples: `publishDicomStowSend()` (DICOM STOW), `publishNiftiSendStudy()`
(NIfTI as `files[]`). Slicer providers use the same client via
`cast_provider_runtime.build_*_publish_message()`.

---

## Subscriber flow (client)

```mermaid
sequenceDiagram
    participant Hub as cast_api
    participant WS as WebSocket
    participant Client as Cast client
    participant App as onMessage

    Hub->>WS: hub.event + files[].payloadId
    WS->>Client: message (no bytes)
    Client->>Client: fetch_all_payloads (parallel GETs)
    loop Each payloadId
        Client->>Hub: GET /api/hub/payloads/{id}
        Hub-->>Client: file bytes
    end
    Client->>Client: files[].data filled in place
    Client->>App: enriched message
```

**Slicer (`resource_server_hub.py`):** the hub asyncio loop calls
`fetch_all_payloads()` **before** dispatching to the provider `onMessage` script,
so TotalSegmentator and similar handlers see `context.files[].data` already set.

**Extract helpers:** `extract_all_dicom_send_payloads()` /
`extract_all_nifti_send_payloads()` read **`context.files[]` only** (no legacy
`resource` slot).

### Download behavior (Python subscriber)

| Setting | Default | Effect |
|---------|---------|--------|
| `CAST_CLIENT_HTTP_PAYLOAD_MAX_CONCURRENT` | 25 | Parallel GETs per batch |
| `CAST_CLIENT_HTTP_PAYLOAD_PROGRESS_INTERVAL` | 25 | Log `Download … completed=N/M …` |
| Transport | `http.client` + thread pool | Fresh TCP per file; one retry on connection reset |

Use **`127.0.0.1`** in local hub URLs when possible (`CastResourceServers.py`
already does for `VOLVIEW-HUB`).

vtk-js / browser: `fetchAllPayloads()` uses the same concurrency constant and
parallel `fetch`.

---

## Hub payload store

| Item | Detail |
|------|--------|
| **GET** | `/api/hub/payloads/{payloadId}` — streamed in 4 MiB chunks (configurable) |
| **TTL** | `CAST_HUB_HTTP_PAYLOAD_TTL_SECONDS` (default 300 s) |
| **Cap** | `CAST_HUB_HTTP_PAYLOAD_MAX_TOTAL_BYTES` (default 2 GiB) |
| **Overflow** | Metadata-only fan-out (no `payloadId`); message is not dropped |
| **GET auth** | Token optional today; upload requires Bearer |

Hub log `Served http payload … elapsed=0.00s` is time to read from memory inside
the hub, not subscriber download time.

---

## DICOM send today

VolView sends a study or series as **one** `dicom-send` STOW batch:

1. Build `context.files[]` (`build-dicom-stow-manifest.ts`).
2. Client uploads manifest + one `application/dicom` part per slice.
3. Receivers download all `payloadId`s, then run segmentation or other logic.

**Do not use** (removed from the protocol):

- `dicomtransfer-request` / `dicomtransfer-response`
- `dicomTransferId`
- `dicom-send` with `context.status: complete` as an end-of-transfer signal
- `multipart/form-data` (`message` + `file`)

---

## End-to-end example

VolView → local hub → TotalSegmentator on topic `USER-1`:

1. VolView STOW-publishes 295 slices in one `dicom-send`.
2. Hub stores slices; one WS notification to `TOTAL-SEGMENTATOR`.
3. Slicer logs `Cast payload batch start … files=295 concurrent=25`.
4. Progress: `Download … completed=25/295 …`, then 50, 75, …
5. `Cast payload batch done …` → `onMessage` stages input → TotalSegmentator runs.

---

## Configuration reference

| Variable | Default | Role |
|----------|---------|------|
| `CAST_HUB_HTTP_PAYLOAD_TTL_SECONDS` | 300 | Payload lifetime |
| `CAST_HUB_HTTP_PAYLOAD_MAX_TOTAL_BYTES` | 2 GiB | Store cap |
| `CAST_HUB_HTTP_PAYLOAD_SEND_CHUNK_BYTES` | 4 MiB | GET chunk size |
| `CAST_CLIENT_HTTP_PAYLOAD_MAX_CONCURRENT` | 25 | Parallel GETs |
| `CAST_CLIENT_HTTP_PAYLOAD_PROGRESS_INTERVAL` | 25 | Progress log cadence |
| `CAST_CLIENT_WS_SOCKET_RCVBUF_BYTES` | 4 MiB | Socket tuning (WS + HTTP) |

---

## Implementation map

| Component | Path |
|-----------|------|
| Hub | `VolView/server/cast_api/cast_api.py` |
| Hub docs | `VolView/server/cast_api/README.md` |
| Python client | `VolView/server/cast_api/cast_client.py`, `CastInterface/Lib/cast_client.py` |
| Browser client | `vtk-js/Sources/IO/Core/CastClient/` |
| VolView app | `VolView/src/io/cast-client.ts`, `build-dicom-stow-manifest.ts`, `build-nifti-send-context.ts` |
| OHIF | `Viewers/extensions/cast/src/services/CastService/` |
| Slicer hub loop | `CastInterface/Lib/resource_server_hub.py` |
| Provider scripts | `CastInterface/Lib/cast_provider_runtime.py`, `Resources/scripts/` |

Workspace contract: `ProjectWeek45/AGENTS.md` (Cast wire-shape section).
