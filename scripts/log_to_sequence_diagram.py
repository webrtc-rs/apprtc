#!/usr/bin/env python3
"""Render apprtc appweb/signaling/sfu INFO logs as one sequence diagram.

apprtc is three (or more) processes:

    client ──HTTP──> appweb ──gRPC──> signaling <──gRPC session──> sfu worker 1..N
    client ──────────WebSocket──────> signaling (register / SDP relay / controls)

Each binary logs one line per event as::

    YYYY/MM/DD HH:MM:SS.ffffff [LEVEL] file:line - message

and a browser `send`/`deliver` additionally logs the opaque SDP/candidate body on
the next line. This script parses those INFO lines from every log, merges them by
timestamp, and draws a sequence diagram whose lanes are, left to right, each
client (by id), AppWeb, Signaling, and each SFU worker (by instance id).

Two output formats:

  --format mermaid   (default) a ```mermaid sequenceDiagram — brief events.
  --format html      a self-contained interactive page: the same events, but
                     every offer/answer carries a clickable [+] that expands its
                     full SDP inline.

The signaling process treats a browser's `msg` as opaque, so the SDP/candidate is
recovered by parsing the logged body (`{"type":"offer","sdp":...}` etc.) here.

Usage:
    scripts/log_to_sequence_diagram.py \\
        --appweb /private/tmp/logs/appweb.log \\
        --signaling /private/tmp/logs/signaling.log \\
        --sfu /private/tmp/logs/sfu.log
    # multiple SFU workers, HTML out, one room only:
    scripts/log_to_sequence_diagram.py --appweb a.log --signaling s.log \\
        --sfu w1.log w2.log -f html -o seq.html --room 42
"""

import argparse
import html
import json
import re
import sys
from dataclasses import dataclass, field

# TS [LEVEL] file:line - message   (start of a log entry; the apprtc bin format)
LINE_RE = re.compile(
    r"^(?P<ts>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+) \[(?P<lvl>[A-Z]+)\] "
    r"(?P<src>\S+?):(?P<ln>\d+) - (?P<msg>.*)$"
)

# ── appweb.log ──────────────────────────────────────────────────────────────
APPWEB = [
    ("join", re.compile(r"^HTTP V2 join: room_id=(?P<room>\d+) client_id=(?P<client>\d+)")),
    ("leave", re.compile(r"^HTTP V2 leave: room_id=(?P<room>\d+) client_id=(?P<client>\d+)")),
]
JOIN_RESP = re.compile(
    r"^HTTP V2 join response: room_id=(?P<room>\d+) client_id=(?P<client>\d+) "
    r"result=(?P<result>\w+) mode=(?P<mode>\w+)"
)
GRPC_REQ = re.compile(
    r"^Signaling gRPC request: operation=(?P<op>\w+) request_id=(?P<rid>\d+) "
    r"room_id=(?P<room>\d+)(?: client_id=(?P<client>\d+))?"
)
GRPC_RESP = re.compile(
    r"^Signaling gRPC response: operation=(?P<op>\w+) request_id=(?P<rid>\d+) result=(?P<result>\w+)"
)

# ── signaling.log ───────────────────────────────────────────────────────────
REGISTER = re.compile(
    r"^V2 register: connection_id=\d+ room_id=(?P<room>\d+) client_id=(?P<client>\d+) epoch=(?P<epoch>\S+)"
)
SEND = re.compile(
    r"^V2 send: connection_id=\d+ room_id=(?P<room>\d+) client_id=(?P<client>\d+) epoch=(?P<epoch>\S+) bytes=\d+"
)
DELIVER = re.compile(
    r"^V2 deliver: connection_id=\d+ room_id=(?P<room>\d+) client_id=(?P<client>\d+) bytes=\d+"
)
SEND_DROP = re.compile(
    r"^V2 send dropped: connection_id=\d+ room_id=(?P<room>\d+) client_id=(?P<client>\d+) reason=(?P<reason>\S+)"
)
SFU_CMD = re.compile(
    r"^SFU command: instance_id=(?P<inst>\S+) connection_id=\d+ request_id=(?P<rid>\d+) "
    r"operation=(?P<op>\w+) room_id=(?P<room>\d+) client_id=(?P<client>\d+)"
)
SFU_RES = re.compile(
    r"^SFU command result: instance_id=(?P<inst>\S+) connection_id=\d+ request_id=(?P<rid>\d+) result=(?P<result>\w+)"
)
SFU_EVENT = re.compile(
    r"^SFU event: instance_id=(?P<inst>\S+) operation=signal room_id=(?P<room>\d+) client_id=(?P<client>\d+)"
)
CONTROL = re.compile(
    r"^V2 control: control=(?P<ctrl>\S+) connection_id=\d+ room_id=(?P<room>\d+) client_id=(?P<client>\d+)"
    r"(?P<rest>.*)$"
)
SESSION_OPEN = re.compile(
    r"^SFU gRPC session opened: instance_id=(?P<inst>\S+) connection_id=\d+"
)

# ── sfu.log (one file per worker) ───────────────────────────────────────────
SFU_INSTANCE = re.compile(r"instance_id=(?P<inst>sfu-\S+)")
SFU_REGISTERED = re.compile(r"^SFU gRPC registered: instance_id=(?P<inst>\S+)")
SFU_MEDIA = re.compile(r"^SFU media listening on (?P<addr>\S+)")
SFU_HEALTH = re.compile(
    r"^SFU health sent: request_id=\d+ state=(?P<state>\w+) rooms=(?P<rooms>\d+) clients=(?P<clients>\d+)"
)


@dataclass
class Event:
    ts: str
    src: str  # lane key the arrow starts from ("" for a note)
    dst: str  # lane key the arrow ends at ("" for a note)
    label: str
    dashed: bool = False  # response / server-initiated
    client: str = ""  # associated client id (colours + membership dividers)
    room: str = ""
    note_lane: str = ""  # lane key for a note (when src/dst are "")
    body: str = ""  # expandable SDP (offer/answer only)
    kind: str = ""  # semantic tag: register / leave / etc. for dividers


@dataclass
class Ctx:
    """Cross-line + cross-file correlation state."""
    grpc: dict = field(default_factory=dict)  # request_id -> (op, room, client)
    sfu_cmd: dict = field(default_factory=dict)  # request_id -> (op, inst, room, client)


def short_inst(inst):
    return inst[4:12] if inst.startswith("sfu-") else inst[:8]


def worker_key(inst):
    return "w" + re.sub(r"[^0-9a-zA-Z]", "", short_inst(inst))


def _body(lines, i):
    """Raw payload logged on the lines after entry `i` (until the next log entry)."""
    body = []
    for line in lines[i + 1 :]:
        if LINE_RE.match(line):
            break
        body.append(line)
    while body and not body[-1].strip():
        body.pop()
    return "\n".join(body).strip()


def _msg_kind(raw):
    """(concise type label, expandable body, requestid) from an opaque signaling `msg` body.

    Shared by the browser send/deliver hops and the signaling→worker `signal` command, so an SDP
    offer/answer/candidate is labelled the same wherever it appears.
    """
    try:
        obj = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return "msg", "", None
    if not isinstance(obj, dict):
        return "msg", "", None
    typ = obj.get("type", "msg")
    rid = obj.get("requestid")
    if typ in ("offer", "answer"):
        sdp = (obj.get("sdp") or "").replace("\\r\\n", "\n").replace("\\n", "\n").replace("\r\n", "\n")
        return typ, sdp, rid
    if typ == "candidate":
        cand = obj.get("candidate", "")
        m = re.search(r" typ (\w+)", cand)
        return (f"candidate {m.group(1)}" if m else "candidate"), cand, rid
    return typ, "", rid


def _sdp_event(ts, src, dst, dashed, room, client, raw):
    """Turn a logged `msg` body into an Event, labelled by its SDP/candidate type."""
    label, body, rid = _msg_kind(raw)
    if rid and label in ("offer", "answer"):
        label += f" ⟨req {rid}⟩"  # subscribe-negotiation correlation id
    return Event(ts, src, dst, label, dashed=dashed, client=client, room=room, body=body)


def parse(path, service, ctx, room_filter):
    with open(path, encoding="utf-8", errors="replace") as fh:
        lines = fh.read().splitlines()

    file_inst = None
    events = []

    def keep(room):
        return not room_filter or room == room_filter

    for i, line in enumerate(lines):
        m = LINE_RE.match(line)
        if not m:
            continue
        ts, msg = m["ts"], m["msg"]

        if service == "appweb":
            done = False
            for kind, rx in APPWEB:
                h = rx.match(msg)
                if h and keep(h["room"]):
                    c = h["client"]
                    events.append(Event(ts, f"c{c}", "appweb", kind, client=c, room=h["room"], kind=kind))
                    done = True
                    break
            if done:
                continue
            h = JOIN_RESP.match(msg)
            if h and keep(h["room"]):
                c = h["client"]
                lbl = f"join {h['mode']}" if h["result"] == "SUCCESS" else f"join {h['result']}"
                events.append(Event(ts, "appweb", f"c{c}", lbl, dashed=True, client=c, room=h["room"]))
                continue
            h = GRPC_REQ.match(msg)
            if h:
                ctx.grpc[h["rid"]] = (h["op"], h["room"], h["client"])
                if h["client"] and keep(h["room"]):
                    events.append(Event(ts, "appweb", "signaling", h["op"], client=h["client"], room=h["room"]))
                continue
            h = GRPC_RESP.match(msg)
            if h:
                op, room, client = ctx.grpc.get(h["rid"], (h["op"], "", ""))
                if client and keep(room):
                    lbl = f"{op} ✓" if h["result"] == "OK" else f"{op} ✗"
                    events.append(Event(ts, "signaling", "appweb", lbl, dashed=True, client=client, room=room))
                continue

        elif service == "signaling":
            h = REGISTER.match(msg)
            if h and keep(h["room"]):
                c = h["client"]
                events.append(Event(ts, f"c{c}", "signaling", "register", client=c, room=h["room"], kind="register"))
                continue
            h = SEND.match(msg)
            if h and keep(h["room"]):
                c = h["client"]
                events.append(_sdp_event(ts, f"c{c}", "signaling", False, h["room"], c, _body(lines, i)))
                continue
            h = DELIVER.match(msg)
            if h and keep(h["room"]):
                c = h["client"]
                events.append(_sdp_event(ts, "signaling", f"c{c}", True, h["room"], c, _body(lines, i)))
                continue
            h = SEND_DROP.match(msg)
            if h and keep(h["room"]):
                c = h["client"]
                events.append(Event(ts, "", "", f"send dropped ({h['reason']})", client=c, room=h["room"], note_lane=f"c{c}"))
                continue
            h = SFU_CMD.match(msg)
            if h and keep(h["room"]):
                w, op, client = worker_key(h["inst"]), h["op"], h["client"]
                if op == "signal":
                    # A signal forwards the browser's SDP/candidate to the worker: label it by type
                    # (e.g. "offer(client)") with the SDP under a [+], like a browser send.
                    kind, body, _ = _msg_kind(_body(lines, i))
                    ctx.sfu_cmd[h["rid"]] = (kind, h["inst"], h["room"], client)
                    events.append(Event(ts, "signaling", w, f"{kind}({client})",
                                        client=client, room=h["room"], kind=op, body=body))
                else:
                    lbl = f"{op}({client})" if op in ("join", "leave") else op
                    ctx.sfu_cmd[h["rid"]] = (op, h["inst"], h["room"], client)
                    events.append(Event(ts, "signaling", w, lbl, client=client, room=h["room"], kind=op))
                continue
            h = SFU_RES.match(msg)
            if h:
                op, inst, room, client = ctx.sfu_cmd.get(h["rid"], ("?", h["inst"], "", ""))
                # A signal ack carries no content (the worker's real reply is its SFU event); keep
                # only the meaningful membership acks and any failures.
                if h["result"] == "OK" and op not in ("join", "leave", "sync_room"):
                    continue
                if keep(room):
                    w = worker_key(inst)
                    mark = "✓" if h["result"] == "OK" else "✗"
                    events.append(Event(ts, w, "signaling", f"{op} {mark}", dashed=True, client=client, room=room))
                continue
            h = SFU_EVENT.match(msg)
            if h and keep(h["room"]):
                # Worker-originated SDP (publish answer or server-initiated re-offer) and its
                # candidates: the worker->signaling leg, before signaling relays it to the browser.
                w, client = worker_key(h["inst"]), h["client"]
                kind, body, _ = _msg_kind(_body(lines, i))
                events.append(Event(ts, w, "signaling", f"{kind}({client})", dashed=True,
                                    client=client, room=h["room"], body=body))
                continue
            h = CONTROL.match(msg)
            if h and keep(h["room"]):
                c = h["client"]
                extra = ""
                mi = re.search(r"is_initiator=(\w+)", h["rest"])
                if mi:
                    extra = " (offerer)" if mi.group(1) == "true" else ""
                mr = re.search(r"reason=(\S+)", h["rest"])
                if mr:
                    extra = f" ({mr.group(1)})"
                events.append(Event(ts, "signaling", f"c{c}", h["ctrl"] + extra, dashed=True, client=c, room=h["room"], kind=h["ctrl"]))
                continue
            h = SESSION_OPEN.match(msg)
            if h:
                w = worker_key(h["inst"])
                events.append(Event(ts, "", "", f"session opened · {short_inst(h['inst'])}", note_lane=w))
                continue

        elif service == "sfu":
            if file_inst is None:
                h = SFU_INSTANCE.search(msg)
                if h:
                    file_inst = h["inst"]
            w = worker_key(file_inst) if file_inst else None
            if w is None:
                continue
            h = SFU_REGISTERED.match(msg)
            if h:
                events.append(Event(ts, "", "", f"registered · {short_inst(file_inst)}", note_lane=w))
                continue
            h = SFU_MEDIA.match(msg)
            if h:
                events.append(Event(ts, "", "", f"media {h['addr']}", note_lane=w))
                continue
            h = SFU_HEALTH.match(msg)
            if h:
                events.append(Event(ts, "", "", f"health {h['state']} · rooms={h['rooms']} clients={h['clients']}", note_lane=w))
                continue

    return events


# ─────────────────────────── lanes / participants ───────────────────────────

def build_lanes(events):
    """Ordered lane keys (clients, appweb, signaling, workers) with display labels."""
    clients, workers = [], []
    have_appweb = have_signaling = False
    for ev in events:
        for lane in (ev.src, ev.dst, ev.note_lane):
            if not lane:
                continue
            if lane == "appweb":
                have_appweb = True
            elif lane == "signaling":
                have_signaling = True
            elif lane.startswith("c") and lane not in clients:
                clients.append(lane)
            elif lane.startswith("w") and lane not in workers:
                workers.append(lane)

    order = list(clients)
    label = {c: f"client {c[1:]}" for c in clients}
    if have_appweb:
        order.append("appweb")
        label["appweb"] = "AppWeb"
    if have_signaling:
        order.append("signaling")
        label["signaling"] = "Signaling"
    for w in workers:
        order.append(w)
        label[w] = f"SFU {w[1:]}"
    return order, label


PALETTE = [
    "#2563eb", "#dc2626", "#059669", "#d97706", "#7c3aed",
    "#0891b2", "#db2777", "#65a30d", "#ea580c", "#4f46e5",
]


def colors(lanes):
    out, ci = {}, 0
    for key in lanes:
        if key == "appweb":
            out[key] = "#0891b2"
        elif key == "signaling":
            out[key] = "#334155"
        else:
            out[key] = PALETTE[ci % len(PALETTE)]
            ci += 1
    return out


# ─────────────────────────────── mermaid ────────────────────────────────────

def render_mermaid(events, lanes, label, room_filter):
    out = ["```mermaid", "sequenceDiagram", "    autonumber"]
    for key in lanes:
        out.append(f"    participant {key} as {label[key]}")

    anchor = "signaling" if "signaling" in lanes else lanes[0]
    rooms = room_filter or ", ".join(sorted({ev.room for ev in events if ev.room}))
    span = f"{events[0].ts[11:]}–{events[-1].ts[11:]}" if events else "—"
    out.append(f"    note over {anchor}: room {rooms} · {len(events)} events · {span}")

    for ev in events:
        if ev.note_lane:
            out.append(f"    note over {ev.note_lane}: {ev.label}")
            continue
        if not ev.src or not ev.dst:
            continue
        arrow = "-->>" if ev.dashed else "->>"
        suffix = " 📄" if ev.body else ""
        out.append(f"    {ev.src} {arrow} {ev.dst}: {ev.label}{suffix}")
        if ev.kind == "register":
            out.append(f"    note over {ev.src},{anchor}: ▶ client {ev.client} joins")
        elif ev.kind == "leave" and ev.src.startswith("c"):
            out.append(f"    note over {ev.src},{anchor}: ⏹ client {ev.client} leaves")
        elif ev.kind == "sfu-upgrade":
            out.append(f"    note over {ev.dst},{anchor}: ⬆ SFU upgrade")
        elif ev.kind == "sfu-downgrade":
            out.append(f"    note over {ev.dst},{anchor}: ⬇ P2P downgrade")

    out.append("```")
    return "\n".join(out)


# ──────────────────────────────── html ──────────────────────────────────────

HTML_TEMPLATE = """<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>apprtc signaling — room {room}</title>
<style>
  :root {{ --line:#94a3b8; }}
  * {{ box-sizing: border-box; }}
  body {{ margin: 0; padding: 20px; font: 14px/1.4 system-ui, sans-serif; color:#0f172a; background:#f8fafc; }}
  h1 {{ font-size: 1.15rem; margin: 0 0 2px; }}
  .meta {{ color:#64748b; font-size: 12px; margin-bottom: 14px; }}
  .scroll {{ overflow-x: auto; }}
  .diagram {{ --cols: repeat({n}, minmax(160px, 1fr)); min-width: {minw}px; }}
  .headers {{ position: sticky; top: 0; z-index: 3; display: grid; grid-template-columns: var(--cols);
             background: #f8fafc; padding: 8px 0; border-bottom: 1px solid #e2e8f0; }}
  .lane {{ text-align:center; }}
  .lane b {{ display:inline-block; padding:4px 10px; border-radius:8px; font-size:12.5px; font-weight:700;
            background:#fff; border:1.5px solid var(--c, #cbd5e1); color: var(--c, #334155); }}
  .lane.hub b {{ background:#334155; color:#fff; border-color:#334155; }}
  .board {{ position: relative; }}
  .lifelines {{ position:absolute; inset:0; display:grid; grid-template-columns: var(--cols); z-index:0; }}
  .lifeline {{ justify-self:center; border-left:1.5px dashed var(--c, #cbd5e1); opacity:.55; }}
  .steps {{ position: relative; z-index:1; }}
  .step {{ display:grid; grid-template-columns: var(--cols); align-items:center; }}
  .arrow {{ display:flex; align-items:center; padding:5px 0; min-height:26px; }}
  .arrow .stalk {{ flex:1 1 auto; border-top:1.5px solid var(--c, var(--line)); }}
  .arrow.out .stalk {{ border-top-style: dashed; }}
  .arrow .head {{ flex:0 0 auto; width:0; height:0; border:5px solid transparent; }}
  .arrow .head.right {{ border-left-color: var(--c, var(--line)); }}
  .arrow .head.left {{ border-right-color: var(--c, var(--line)); }}
  .arrow .label {{ flex:0 0 auto; padding:1px 8px; white-space:nowrap; background:#f8fafc;
                  border-radius:6px; font-size:12.5px; box-shadow: inset 0 -2px 0 var(--c, var(--line)); }}
  .seq {{ color:#94a3b8; font-variant-numeric: tabular-nums; margin-right:6px; }}
  .toggle {{ cursor:pointer; user-select:none; border:1px solid #cbd5e1; background:#fff; color:#334155;
            border-radius:4px; padding:0 5px; font: inherit; font-size:11px; line-height:16px; margin-left:6px; }}
  .toggle:hover {{ background:#eef2ff; }}
  .sdp {{ grid-column: 1 / -1; display:none; }}
  .sdp.open {{ display:block; }}
  .sdp pre {{ margin:2px 0 10px; max-height:320px; overflow:auto; background:#0f172a; color:#e2e8f0;
             padding:10px 12px; border-radius:8px; font: 11.5px/1.45 ui-monospace, monospace; }}
  .note {{ text-align:center; margin:6px 0; }}
  .note span {{ display:inline-block; padding:2px 10px; border-radius:9999px; font-size:12px;
               white-space:nowrap; color: var(--c, #334155); border:1.5px solid var(--c, #cbd5e1);
               background: color-mix(in srgb, var(--c, #94a3b8) 14%, #fff); }}
  .controls {{ margin: 4px 0 12px; }}
  .controls button {{ font: inherit; font-size:12px; padding:3px 10px; border-radius:6px;
                     border:1px solid #cbd5e1; background:#fff; cursor:pointer; }}
</style></head>
<body>
<h1>apprtc signaling — room {room}</h1>
<div class="meta">{count} events · {span} · solid = request, dashed = response/server-push · [+] expands SDP</div>
<div class="controls"><button onclick="allSdp(true)">Expand all SDP</button>
<button onclick="allSdp(false)">Collapse all</button></div>
<div class="scroll"><div class="diagram">
  <div class="headers">{heads}</div>
  <div class="board">
    <div class="lifelines">{lifelines}</div>
    <div class="steps">{steps}</div>
  </div>
</div></div>
<script>
  document.querySelectorAll('.toggle').forEach(function (t) {{
    t.addEventListener('click', function () {{
      var box = document.getElementById(t.dataset.target);
      var open = box.classList.toggle('open');
      t.textContent = open ? '[−]' : '[+]';
    }});
  }});
  function allSdp(open) {{
    document.querySelectorAll('.sdp').forEach(function (b) {{ b.classList.toggle('open', open); }});
    document.querySelectorAll('.toggle').forEach(function (t) {{ t.textContent = open ? '[−]' : '[+]'; }});
  }}
</script>
</body></html>
"""


def render_html(events, lanes, label, room_filter):
    col = {key: i + 1 for i, key in enumerate(lanes)}
    color_of = colors(lanes)
    n = len(lanes)

    heads = []
    for key in lanes:
        cls = "lane hub" if key in ("appweb", "signaling") else "lane"
        heads.append(f'<div class="{cls}" style="--c:{color_of[key]}"><b>{html.escape(label[key])}</b></div>')
    lifelines = "".join(f'<div class="lifeline" style="--c:{color_of[k]}"></div>' for k in lanes)

    steps = []
    for seq, ev in enumerate(events, 1):
        if ev.note_lane:
            c = color_of.get(ev.note_lane, "#94a3b8")
            steps.append(
                f'<div class="step"><div class="note" style="grid-column:{col[ev.note_lane]};--c:{c}">'
                f"<span>{html.escape(ev.label)}</span></div></div>"
            )
            continue
        if not ev.src or not ev.dst:
            continue

        cs, cd = col[ev.src], col[ev.dst]
        lo, hi = min(cs, cd), max(cs, cd)
        head_side = "right" if cd > cs else "left"
        inset = 50.0 / (hi - lo + 1)
        arrow_color = color_of.get(f"c{ev.client}") or color_of.get(ev.src, "#94a3b8")

        label_html = f'<span class="seq">{seq}</span>{html.escape(ev.label)}'
        if ev.body:
            label_html += f'<button class="toggle" data-target="sdp{seq}">[+]</button>'

        head = f'<span class="head {head_side}"></span>'
        stalk = '<span class="stalk"></span>'
        inner = f'{stalk}<span class="label">{label_html}</span>{stalk}'
        arrow = f"{head}{inner}" if head_side == "left" else f"{inner}{head}"

        row = ['<div class="step">']
        row.append(
            f'<div class="arrow {"out" if ev.dashed else "in"}" '
            f'style="grid-column:{lo} / {hi + 1};padding-left:{inset:.3f}%;padding-right:{inset:.3f}%;'
            f'--c:{arrow_color}">{arrow}</div>'
        )
        if ev.body:
            row.append(f'<pre class="sdp" id="sdp{seq}">{html.escape(ev.body)}</pre>')
        row.append("</div>")
        steps.append("".join(row))

        if ev.kind == "register":
            _divider(steps, col[ev.src], color_of[ev.src], f"▶ client {ev.client} joins")
        elif ev.kind == "leave" and ev.src.startswith("c"):
            _divider(steps, col[ev.src], color_of[ev.src], f"⏹ client {ev.client} leaves")
        elif ev.kind == "sfu-upgrade":
            _divider(steps, col[ev.dst], color_of[ev.dst], "⬆ SFU upgrade")
        elif ev.kind == "sfu-downgrade":
            _divider(steps, col[ev.dst], color_of[ev.dst], "⬇ P2P downgrade")

    rooms = room_filter or ", ".join(sorted({ev.room for ev in events if ev.room}))
    span = f"{events[0].ts[11:]}–{events[-1].ts[11:]}" if events else "—"
    return HTML_TEMPLATE.format(
        room=html.escape(rooms),
        n=n,
        minw=n * 160,
        count=len(events),
        span=span,
        heads="".join(heads),
        lifelines=lifelines,
        steps="".join(steps),
    )


def _divider(steps, column, color, text):
    steps.append(
        f'<div class="step"><div class="note" style="grid-column:{column};--c:{color}">'
        f"<span>{html.escape(text)}</span></div></div>"
    )


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--appweb", help="path to appweb INFO log")
    ap.add_argument("--signaling", help="path to signaling INFO log")
    ap.add_argument("--sfu", nargs="+", default=[], help="one or more SFU worker INFO logs")
    ap.add_argument("-f", "--format", choices=["mermaid", "html"], default="mermaid")
    ap.add_argument("-o", "--out", help="write here (default: stdout)")
    ap.add_argument("--room", help="only include this room id")
    args = ap.parse_args()

    if not (args.appweb or args.signaling or args.sfu):
        ap.error("provide at least one of --appweb, --signaling, --sfu")

    ctx = Ctx()
    events = []
    # Parse appweb + signaling before the sfu logs so command request_ids are correlated.
    if args.appweb:
        events += parse(args.appweb, "appweb", ctx, args.room)
    if args.signaling:
        events += parse(args.signaling, "signaling", ctx, args.room)
    for path in args.sfu:
        events += parse(path, "sfu", ctx, args.room)

    events.sort(key=lambda e: e.ts)
    if not events:
        sys.exit("no events found (wrong logs, or --room filtered everything out)")

    lanes, label = build_lanes(events)
    if args.format == "html":
        diagram = render_html(events, lanes, label, args.room)
    else:
        diagram = render_mermaid(events, lanes, label, args.room)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(diagram + "\n")
        print(f"wrote {len(events)} events ({args.format}) to {args.out}", file=sys.stderr)
    else:
        print(diagram)


if __name__ == "__main__":
    main()
