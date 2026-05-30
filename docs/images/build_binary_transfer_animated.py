"""Build binary-file-transfer-animated.svg from binary-file-transfer.svg."""
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
SRC = HERE / "binary-file-transfer.svg"
OUT = HERE / "binary-file-transfer-animated.svg"

text = SRC.read_text(encoding="utf-8")
b64m = re.search(r"data:image/png;base64,([A-Za-z0-9+/=]+)", text)
if not b64m:
    raise SystemExit("embedded PNG not found in source SVG")
b64 = b64m.group(1)
DUR = "30s"

ANIM_STYLE = f"""
      #castHubGraphic {{ transform-origin: 600px 340px; animation: hubPulse {DUR} infinite; }}
      @keyframes hubPulse {{
        0%, 18% {{ transform: scale(1); }}
        22%, 32% {{ transform: scale(1.07); }}
        36%, 100% {{ transform: scale(1); }}
      }}
      #step1Caption {{ animation: step1Caption {DUR} infinite; }}
      @keyframes step1Caption {{
        0%, 3% {{ opacity: 0; }}
        5%, 20% {{ opacity: 1; }}
        22%, 100% {{ opacity: 0; }}
      }}
      #step2Caption {{ animation: step2Caption {DUR} infinite; }}
      @keyframes step2Caption {{
        0%, 20% {{ opacity: 0; }}
        22%, 32% {{ opacity: 1; }}
        34%, 100% {{ opacity: 0; }}
      }}
      #step3Caption {{ animation: step3Caption {DUR} infinite; }}
      @keyframes step3Caption {{
        0%, 32% {{ opacity: 0; }}
        34%, 42% {{ opacity: 1; }}
        44%, 100% {{ opacity: 0; }}
      }}
      #step4Caption {{ animation: step4Caption {DUR} infinite; }}
      @keyframes step4Caption {{
        0%, 42% {{ opacity: 0; }}
        44%, 72% {{ opacity: 1; }}
        74%, 100% {{ opacity: 0; }}
      }}
      #step5Caption {{ animation: step5Caption {DUR} infinite; }}
      @keyframes step5Caption {{
        0%, 72% {{ opacity: 0; }}
        74%, 88% {{ opacity: 1; }}
        90%, 100% {{ opacity: 0; }}
      }}
      #flowLinePost {{ animation: flowLinePost {DUR} infinite; }}
      @keyframes flowLinePost {{
        0%, 3% {{ stroke: #cbd5e1; opacity: 0.5; }}
        5%, 22% {{ stroke: #2457d6; opacity: 1; }}
        24%, 100% {{ stroke: #cbd5e1; opacity: 0.5; }}
      }}
      #flowLineWs {{ animation: flowLineWs {DUR} infinite; }}
      @keyframes flowLineWs {{
        0%, 32% {{ stroke: #cbd5e1; opacity: 0.5; }}
        34%, 44% {{ stroke: #2457d6; opacity: 1; }}
        46%, 100% {{ stroke: #cbd5e1; opacity: 0.5; }}
      }}
      #flowLineGet {{ animation: flowLineGet {DUR} infinite; }}
      @keyframes flowLineGet {{
        0%, 42% {{ stroke: #cbd5e1; opacity: 0.5; }}
        44%, 72% {{ stroke: #1f9d55; opacity: 1; }}
        74%, 100% {{ stroke: #cbd5e1; opacity: 0.5; }}
      }}
      .packetStowPost {{ opacity: 0; animation: packetStowPost {DUR} infinite; }}
      @keyframes packetStowPost {{
        0%, 3% {{ opacity: 0; transform: translate(220px, 320px); }}
        5% {{ opacity: 1; transform: translate(220px, 320px); }}
        18% {{ opacity: 1; transform: translate(600px, 320px); }}
        21%, 100% {{ opacity: 0; transform: translate(600px, 320px); }}
      }}
      #uploadFileStack {{
        transform-origin: 0 0;
        animation: uploadFileStack {DUR} infinite;
      }}
      @keyframes uploadFileStack {{
        0%, 3% {{ opacity: 0; transform: translate(182px, 248px); }}
        5% {{ opacity: 1; transform: translate(182px, 248px); }}
        18% {{ opacity: 1; transform: translate(562px, 248px); }}
        21%, 100% {{ opacity: 0; transform: translate(562px, 248px); }}
      }}
      #hubPayloadStack {{
        transform-origin: 600px 260px;
        animation: hubPayloadStack {DUR} infinite;
      }}
      @keyframes hubPayloadStack {{
        0%, 20% {{ opacity: 0; transform: scale(0.92); }}
        22%, 73% {{ opacity: 1; transform: scale(1); }}
        76% {{ opacity: 0.85; transform: scale(0.96); }}
        80% {{ opacity: 0.35; transform: scale(0.88); }}
        84%, 100% {{ opacity: 0; transform: scale(0.8); }}
      }}
      .packetWsNotify {{ opacity: 0; animation: packetWsNotify {DUR} infinite; }}
      @keyframes packetWsNotify {{
        0%, 32% {{ opacity: 0; transform: translate(600px, 360px); }}
        34% {{ opacity: 1; transform: translate(600px, 360px); }}
        42% {{ opacity: 1; transform: translate(980px, 360px); }}
        44%, 100% {{ opacity: 0; transform: translate(980px, 360px); }}
      }}
      .packetGet1 {{ opacity: 0; animation: packetGet1 {DUR} infinite; }}
      @keyframes packetGet1 {{
        0%, 44% {{ opacity: 0; transform: translate(600px, 300px); }}
        46% {{ opacity: 1; transform: translate(600px, 300px); }}
        53% {{ opacity: 1; transform: translate(900px, 480px); }}
        54%, 100% {{ opacity: 0; transform: translate(900px, 480px); }}
      }}
      .packetGet2 {{ opacity: 0; animation: packetGet2 {DUR} infinite; }}
      @keyframes packetGet2 {{
        0%, 50% {{ opacity: 0; transform: translate(600px, 340px); }}
        52% {{ opacity: 1; transform: translate(600px, 340px); }}
        59% {{ opacity: 1; transform: translate(940px, 480px); }}
        60%, 100% {{ opacity: 0; transform: translate(940px, 480px); }}
      }}
      .packetGet3 {{ opacity: 0; animation: packetGet3 {DUR} infinite; }}
      @keyframes packetGet3 {{
        0%, 56% {{ opacity: 0; transform: translate(600px, 380px); }}
        58% {{ opacity: 1; transform: translate(600px, 380px); }}
        65% {{ opacity: 1; transform: translate(980px, 480px); }}
        66%, 100% {{ opacity: 0; transform: translate(980px, 480px); }}
      }}
      #rsAccumTray {{ animation: rsAccumTray {DUR} infinite; }}
      @keyframes rsAccumTray {{
        0%, 53% {{ opacity: 0; }}
        55%, 100% {{ opacity: 1; }}
      }}
      #rsAccumFile1 {{
        transform-origin: center;
        animation: rsAccumFile1 {DUR} infinite;
      }}
      @keyframes rsAccumFile1 {{
        0%, 53% {{ opacity: 0; transform: translate(900px, 480px) scale(0.55); }}
        54% {{ opacity: 1; transform: translate(900px, 480px) scale(0.9); }}
        55% {{ opacity: 1; transform: translate(908px, 542px) scale(1); }}
        56%, 100% {{ opacity: 1; transform: translate(908px, 542px) scale(1); }}
      }}
      #rsAccumFile2 {{
        transform-origin: center;
        animation: rsAccumFile2 {DUR} infinite;
      }}
      @keyframes rsAccumFile2 {{
        0%, 59% {{ opacity: 0; transform: translate(940px, 480px) scale(0.55); }}
        60% {{ opacity: 1; transform: translate(940px, 480px) scale(0.9); }}
        61% {{ opacity: 1; transform: translate(952px, 542px) scale(1); }}
        62%, 100% {{ opacity: 1; transform: translate(952px, 542px) scale(1); }}
      }}
      #rsAccumFile3 {{
        transform-origin: center;
        animation: rsAccumFile3 {DUR} infinite;
      }}
      @keyframes rsAccumFile3 {{
        0%, 65% {{ opacity: 0; transform: translate(980px, 480px) scale(0.55); }}
        66% {{ opacity: 1; transform: translate(980px, 480px) scale(0.9); }}
        67% {{ opacity: 1; transform: translate(996px, 542px) scale(1); }}
        68%, 100% {{ opacity: 1; transform: translate(996px, 542px) scale(1); }}
      }}
      #rsAccumLabel {{ animation: rsAccumLabel {DUR} infinite; }}
      @keyframes rsAccumLabel {{
        0%, 66% {{ opacity: 0; }}
        68%, 100% {{ opacity: 1; }}
      }}
      #fileProgressCount {{ animation: fileProgressCount {DUR} infinite; }}
      @keyframes fileProgressCount {{
        0%, 53% {{ opacity: 0; }}
        55%, 58% {{ opacity: 1; }}
        60%, 100% {{ opacity: 0; }}
      }}
      #fileProgressMid {{ animation: fileProgressMid {DUR} infinite; }}
      @keyframes fileProgressMid {{
        0%, 59% {{ opacity: 0; }}
        61%, 64% {{ opacity: 1; }}
        66%, 100% {{ opacity: 0; }}
      }}
      #fileProgressDone {{ animation: fileProgressDone {DUR} infinite; }}
      @keyframes fileProgressDone {{
        0%, 65% {{ opacity: 0; }}
        67%, 72% {{ opacity: 1; }}
        74%, 100% {{ opacity: 0; }}
      }}
"""

FILE_STACK_INNER = """
      <rect x="0" y="12" width="28" height="36" rx="4" fill="#dbeafe" stroke="#2457d6" stroke-width="2"/>
      <rect x="24" y="6" width="28" height="36" rx="4" fill="#dbeafe" stroke="#2457d6" stroke-width="2"/>
      <rect x="48" y="0" width="28" height="36" rx="4" fill="#dbeafe" stroke="#2457d6" stroke-width="2"/>
"""

parts = [
    '<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" '
    'viewBox="0 0 1200 700" width="1200" height="700" role="img" '
    'aria-labelledby="bftTitle bftDesc">\n',
    '  <title id="bftTitle">Binary file transfer (animated)</title>\n',
    '  <desc id="bftDesc">Files move with POST from Image Display; hub stores payloadIds; '
    'WebSocket notify; GET file bytes; payload TTL expires and hub clears memory.</desc>\n',
    "  <defs>\n",
    '    <filter id="shadow" x="-20%" y="-20%" width="140%" height="140%">\n',
    '      <feDropShadow dx="0" dy="6" stdDeviation="8" flood-color="#0b1b5f" flood-opacity="0.14"/>\n',
    "    </filter>\n",
    '    <clipPath id="resourceServerClip">\n',
    '      <rect x="848" y="188" width="264" height="168" rx="10"/>\n',
    "    </clipPath>\n",
    "    <style>\n",
    "      .title { font: 700 38px Arial, Helvetica, sans-serif; fill: #06146b; }\n",
    "      .heading { font: 700 24px Arial, Helvetica, sans-serif; fill: #06146b; }\n",
    "      .label { font: 700 20px Arial, Helvetica, sans-serif; fill: #06146b; }\n",
    "      .small { font: 16px Arial, Helvetica, sans-serif; fill: #06146b; }\n",
    "      .panel { fill: #fff; stroke: #cfe0ff; stroke-width: 2; filter: url(#shadow); }\n",
    "      .flowLine { fill: none; stroke: #cbd5e1; stroke-width: 3; stroke-linecap: round; }\n",
    "      .flowLineActive { fill: none; stroke-width: 4; stroke-linecap: round; }\n",
    "      .packetText { font: 700 11px Arial, Helvetica, sans-serif; fill: #fff; }\n",
    "      .packetSub { font: 600 9px Arial, Helvetica, sans-serif; fill: #dbeafe; }\n",
    "      .captionBox { fill: #0f172a; opacity: 0.9; }\n",
    "      .captionText { font: 700 13px Arial, Helvetica, sans-serif; fill: #fff; }\n",
    ANIM_STYLE,
    "    </style>\n",
    "  </defs>\n",
    '  <rect width="1200" height="700" fill="#ffffff"/>\n',
    '  <text x="600" y="58" text-anchor="middle" class="title">Binary file transfer</text>\n',
    '  <g id="imageDisplay">\n',
    '    <rect class="panel" x="95" y="255" width="250" height="190" rx="18"/>\n',
    '    <rect x="185" y="278" width="70" height="58" rx="12" fill="#6528e0"/>\n',
    '    <path d="M200 322 L215 304 L227 317 L236 309 L248 322 Z" fill="#fff"/>\n',
    '    <circle cx="236" cy="296" r="5" fill="#fff"/>\n',
    '    <text x="220" y="364" text-anchor="middle" class="label">Image Display</text>\n',
    '    <text x="220" y="390" text-anchor="middle" class="small">View and interact</text>\n',
    '    <text x="220" y="410" text-anchor="middle" class="small">with medical images</text>\n',
    "  </g>\n",
    '  <g id="castHub">\n',
    '    <g id="castHubGraphic">\n',
    '      <circle cx="600" cy="340" r="18" fill="#0b55d9"/>\n',
    '      <circle cx="600" cy="278" r="9" fill="#0b55d9"/>\n',
    '      <circle cx="545" cy="310" r="12" fill="#0b55d9"/>\n',
    '      <circle cx="655" cy="310" r="12" fill="#0b55d9"/>\n',
    '      <circle cx="555" cy="385" r="12" fill="#0b55d9"/>\n',
    '      <circle cx="645" cy="385" r="12" fill="#0b55d9"/>\n',
    '      <line x1="600" y1="340" x2="600" y2="278" stroke="#0b55d9" stroke-width="8"/>\n',
    '      <line x1="600" y1="340" x2="545" y2="310" stroke="#0b55d9" stroke-width="8"/>\n',
    '      <line x1="600" y1="340" x2="655" y2="310" stroke="#0b55d9" stroke-width="8"/>\n',
    '      <line x1="600" y1="340" x2="555" y2="385" stroke="#0b55d9" stroke-width="8"/>\n',
    '      <line x1="600" y1="340" x2="645" y2="385" stroke="#0b55d9" stroke-width="8"/>\n',
    "    </g>\n",
    '    <text x="600" y="430" text-anchor="middle" class="heading">Cast Hub</text>\n',
    '    <text x="600" y="456" text-anchor="middle" class="small">HTTP payload store</text>\n',
    '    <g id="hubPayloadStack">\n',
    '      <rect x="562" y="248" width="28" height="36" rx="4" fill="#dbeafe" stroke="#2457d6" stroke-width="2"/>\n',
    '      <rect x="586" y="242" width="28" height="36" rx="4" fill="#dbeafe" stroke="#2457d6" stroke-width="2"/>\n',
    '      <rect x="610" y="236" width="28" height="36" rx="4" fill="#dbeafe" stroke="#2457d6" stroke-width="2"/>\n',
    '      <text x="600" y="228" text-anchor="middle" font-family="Arial" font-size="10" font-weight="700" fill="#2457d6">payloadId ×3</text>\n',
    '      <text x="600" y="292" text-anchor="middle" font-family="Arial" font-size="9" font-weight="700" fill="#94a3b8">in-memory store</text>\n',
    "    </g>\n",
    "  </g>\n",
    '  <g id="resourceServer">\n',
    '    <rect class="panel" x="835" y="165" width="290" height="350" rx="18"/>\n',
    f'    <image xlink:href="data:image/png;base64,{b64}" href="data:image/png;base64,{b64}" '
    'x="848" y="188" width="264" height="168" preserveAspectRatio="xMidYMid meet" '
    'clip-path="url(#resourceServerClip)"/>\n',
    '    <text x="980" y="380" text-anchor="middle" class="label">Resource Server</text>\n',
    '    <text x="980" y="406" text-anchor="middle" class="small">e.g. TotalSegmentator</text>\n',
    '    <text x="980" y="426" text-anchor="middle" class="small">GETs bytes when ready</text>\n',
    "  </g>\n",
    '  <path class="flowLine" d="M345 350 L545 350"/>\n',
    '  <path id="flowLinePost" class="flowLineActive" d="M345 350 L545 350" stroke="#2457d6"/>\n',
    '  <path class="flowLine" d="M655 350 L835 350"/>\n',
    '  <path id="flowLineWs" class="flowLineActive" d="M655 350 L835 350" stroke="#2457d6"/>\n',
    '  <path id="flowLineGet" class="flowLineActive" d="M655 340 L835 340" stroke="#1f9d55"/>\n',
    '  <g id="step1Caption">\n',
    '    <rect class="captionBox" x="120" y="82" width="960" height="58" rx="8"/>\n',
    '    <text x="600" y="102" text-anchor="middle" class="captionText">Step 1: Image Display publishes an event with Content-Type: multipart/related</text>\n',
    '    <text x="600" y="118" text-anchor="middle" class="captionText" font-size="11">and one binary part per file.</text>\n',
    '    <text x="600" y="134" text-anchor="middle" class="captionText" font-size="11">The context data contains the manifest of each file.</text>\n',
    "  </g>\n",
    '  <g id="step2Caption">\n',
    '    <rect class="captionBox" x="380" y="88" width="440" height="44" rx="8"/>\n',
    '    <text x="600" y="108" text-anchor="middle" class="captionText">Step 2: Hub stores bytes; adds payloadId per files[] entry</text>\n',
    '    <text x="600" y="126" text-anchor="middle" class="captionText" font-size="11">WebSocket will carry metadata only</text>\n',
    "  </g>\n",
    '  <g id="step3Caption">\n',
    '    <rect class="captionBox" x="330" y="88" width="540" height="44" rx="8"/>\n',
    '    <text x="600" y="108" text-anchor="middle" class="captionText">Step 3: Hub fans out dicom-send JSON (files[].payloadId)</text>\n',
    '    <text x="600" y="126" text-anchor="middle" class="captionText" font-size="11">Cast Hub → Resource Server over WebSocket</text>\n',
    "  </g>\n",
    '  <g id="step4Caption">\n',
    '    <rect class="captionBox" x="240" y="88" width="720" height="44" rx="8"/>\n',
    '    <text x="600" y="108" text-anchor="middle" class="captionText">Step 4: GET requests — Resource Server fetches the files it needs</text>\n',
    '    <text x="600" y="126" text-anchor="middle" class="captionText" font-size="11">All three files fetched — bytes accumulate in files[].data</text>\n',
    "  </g>\n",
    '  <g id="step5Caption">\n',
    '    <rect class="captionBox" x="300" y="88" width="600" height="44" rx="8"/>\n',
    '    <text x="600" y="108" text-anchor="middle" class="captionText">Step 5: Payload expires — files removed from hub memory</text>\n',
    '    <text x="600" y="126" text-anchor="middle" class="captionText" font-size="11">CAST_HUB_HTTP_PAYLOAD_TTL_SECONDS (default 300 s)</text>\n',
    "  </g>\n",
    f'  <g id="uploadFileStack">{FILE_STACK_INNER}\n',
    '    <text x="38" y="-6" text-anchor="middle" font-family="Arial" font-size="10" font-weight="700" fill="#2457d6">3 files</text>\n',
    "  </g>\n",
    '  <g class="packetStowPost">\n',
    '    <rect x="-92" y="-20" width="184" height="40" rx="18" fill="#2457d6"/>\n',
    '    <text x="0" y="-4" text-anchor="middle" class="packetSub">HTTP POST</text>\n',
    '    <text x="0" y="11" text-anchor="middle" class="packetText">multipart + files</text>\n',
    "  </g>\n",
    '  <g class="packetWsNotify">\n',
    '    <rect x="-78" y="-16" width="156" height="32" rx="16" fill="#2457d6"/>\n',
    '    <text x="0" y="-2" text-anchor="middle" class="packetSub">WebSocket</text>\n',
    '    <text x="0" y="12" text-anchor="middle" class="packetText">dicom-send JSON</text>\n',
    "  </g>\n",
    '  <g class="packetGet1">\n',
    '    <rect x="-82" y="-16" width="164" height="32" rx="16" fill="#1f9d55"/>\n',
    '    <rect x="-68" y="-8" width="14" height="18" rx="2" fill="#dbeafe" stroke="#fff" stroke-width="1"/>\n',
    '    <text x="-48" y="-2" text-anchor="start" class="packetSub">GET req</text>\n',
    '    <text x="-48" y="12" text-anchor="start" class="packetText">+ file</text>\n',
    "  </g>\n",
    '  <g class="packetGet2">\n',
    '    <rect x="-82" y="-16" width="164" height="32" rx="16" fill="#1f9d55"/>\n',
    '    <rect x="-68" y="-8" width="14" height="18" rx="2" fill="#dbeafe" stroke="#fff" stroke-width="1"/>\n',
    '    <text x="-48" y="-2" text-anchor="start" class="packetSub">GET req</text>\n',
    '    <text x="-48" y="12" text-anchor="start" class="packetText">+ file</text>\n',
    "  </g>\n",
    '  <g class="packetGet3">\n',
    '    <rect x="-82" y="-16" width="164" height="32" rx="16" fill="#1f9d55"/>\n',
    '    <rect x="-68" y="-8" width="14" height="18" rx="2" fill="#dbeafe" stroke="#fff" stroke-width="1"/>\n',
    '    <text x="-48" y="-2" text-anchor="start" class="packetSub">GET req</text>\n',
    '    <text x="-48" y="12" text-anchor="start" class="packetText">+ file</text>\n',
    "  </g>\n",
    '  <g id="rsAccumulatedFiles">\n',
    '    <rect id="rsAccumTray" x="858" y="524" width="244" height="58" rx="10" fill="#f0fdf4" stroke="#86efac" stroke-width="2"/>\n',
    '    <g id="rsAccumFile1">\n',
    '      <rect x="-14" y="-18" width="28" height="36" rx="4" fill="#bbf7d0" stroke="#16a34a" stroke-width="2"/>\n',
    "    </g>\n",
    '    <g id="rsAccumFile2">\n',
    '      <rect x="-14" y="-18" width="28" height="36" rx="4" fill="#bbf7d0" stroke="#16a34a" stroke-width="2"/>\n',
    "    </g>\n",
    '    <g id="rsAccumFile3">\n',
    '      <rect x="-14" y="-18" width="28" height="36" rx="4" fill="#bbf7d0" stroke="#16a34a" stroke-width="2"/>\n',
    "    </g>\n",
    '    <text id="fileProgressCount" x="980" y="548" text-anchor="middle" font-family="Arial" font-size="11" font-weight="700" fill="#166534">1 / 3 fetched</text>\n',
    '    <text id="fileProgressMid" x="980" y="548" text-anchor="middle" font-family="Arial" font-size="11" font-weight="700" fill="#166534">2 / 3 fetched</text>\n',
    '    <text id="fileProgressDone" x="980" y="548" text-anchor="middle" font-family="Arial" font-size="11" font-weight="700" fill="#166534">3 / 3 fetched</text>\n',
    '    <text id="rsAccumLabel" x="980" y="572" text-anchor="middle" font-family="Arial" font-size="11" font-weight="700" fill="#166534">files accumulate here (files[].data)</text>\n',
    "  </g>\n",
    "</svg>\n",
]

OUT.write_text("".join(parts), encoding="utf-8", newline="\n")
print("wrote", OUT, len("".join(parts)))
