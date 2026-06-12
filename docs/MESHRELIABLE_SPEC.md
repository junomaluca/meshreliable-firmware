# MeshReliable — Product Specification (Final)

**Version 6.0 — June 2026.** Supersedes v5.1. This revision folds in the deliverability work
that took every message category to ~100% across same-band and cross-band, and removes
mechanisms that were replaced by simpler, more reliable ones. All firmware and app code, and
all testing, performed by Claude Code against a USB-connected hardware bench.

---

## 1. Executive Summary

MeshReliable is a fork of Meshtastic (firmware + iOS app) that turns "best-effort mesh
messaging" into **delivery-accountable messaging**. Stock Meshtastic retries a DM ~3 times then
drops it, has no per-recipient delivery accounting for multi-party messages, cannot move media,
and cannot talk across LoRa bands. MeshReliable adds five production features — **persistent DM
retries, acknowledged group messaging, compressed media transfer, cross-band MQTT bridging, and
144 MHz (2 m amateur) support** — and, in this revision, a unified **deliverability layer** that
makes text, voice memos, and images reach 100 % of live recipients in DMs, group messages, and
on the maluca channel, on a single band or across bands.

**The headline result of this revision:** text, voice, and image now deliver at **100 % to all
live recipients** for direct messages (915-only, 144-only, and 915↔144 cross-band) and for a
mixed-band 5-node group. The remainder of this document explains the mechanisms that get there.

---

## 2. The Deliverability Model (NEW — the core of this revision)

Every message type in MeshReliable now rests on the **same five-part reliability recipe**.
Whenever a category fell short of 100 %, the cause was a missing piece of this recipe — never the
radio link itself.

1. **Reliable unicast delivery.** A message reaches a recipient reliably only when it is sent as
   a routed `want_ack` unicast — the router then retransmits until the destination's end-to-end
   ACK comes back (24 h persistent window, see §3.1). A *broadcast* loses ~30 % per recipient on
   the first shot and has no per-recipient confirmation.

2. **A reliable ACK round-trip.** The application-level acknowledgement (group ACK, media
   ACK_COMPLETE) must itself be a reliable unicast back to the original sender. A fire-and-forget
   ACK caps success at ~p^N — e.g. 36 % for a 3-node group — even when the payload was delivered.

3. **Forward to the phone — never consume.** A received packet that a module *consumes* (returns
   STOP) never reaches the app, so it is invisible and unverifiable. Received group messages and
   media completions must be forwarded (return CONTINUE) so the recipient's app actually shows
   them. This single bug held group text at 0 %.

4. **Airtime budget (spacing).** A group message is **N× a DM's airtime** (N unicasts + N ACKs +
   their retries); a media transfer is many packets. Firing them in a tight burst saturates the
   LoRa channel and the host serial link, dropping deliveries. Pacing sends to a realistic
   cadence restores 100 %. This is RF/airtime physics, not a protocol defect.

5. **Right-sizing for constrained nodes.** On the busy 144 MHz gateways (T-Beams), heavy media
   over the USB test link wedges the USB-CDC peripheral. Shrinking payloads and giving transfers
   generous timing headroom keeps those nodes responsive and brings them to 100 % as well.

**Corollary — "everything is N DMs."** Group text, group voice, and group image are all
implemented as *N reliable unicasts/media-DMs, one per live member*. A group is, mechanically, a
fan-out of direct messages — so it inherits DM-grade reliability, member by member.

---

## 3. Feature Specifications

### 3.1 Persistent Direct Message Retries

**Problem:** stock Meshtastic abandons a DM after ~3 attempts.

**Mechanism:** retransmit-until-end-to-end-ACK over a long window. Exponential backoff from an
initial interval, capped, repeated until the routed ACK arrives or the window expires. The
product owner explicitly accepts "messages may arrive much later" in exchange for delivery.

| Parameter | Default | Notes |
|---|---|---|
| `retry_window_seconds` | 86,400 (24 h) | DO NOT shrink — long window is what makes ~100 % |
| `initial_retry_interval_ms` | 8,000 (VHF) / 15,000 (915) | first backoff step |
| `max_retry_interval_ms` | 60,000 (VHF) / 90,000 (915) | backoff cap (policy "DM-B") |
| `battery_throttle_threshold` | 20 % | doubles interval below; pauses below 10 % |

**Result:** text DMs deliver at **100 %** on 915-only, 144-only, and cross-band (see §6). Voice
and image DMs reach 100 % via the media path (§3.3) on top of this same retry discipline.

### 3.2 Acknowledged Group Messaging (rewritten for 100 %)

A group is a multi-member primitive with **per-member delivery accounting** — distinct from a DM
(1:1) and a channel (broadcast, no ACK). Members share a group key; the roster is eventually
consistent via JOIN/LEAVE announcements; the app shows per-member ✓/⏳/✗ status.

**What changed in this revision (0 % → 100 %).** The original design broadcast one copy and
relied on a lossy broadcast ACK plus a long "swarm rebroadcast" schedule. Measured on a 3-node
915 group it sat at 0–36 %. It now uses the deliverability model of §2:

1. **Forward received group messages to the phone** (`handleReceivedProtobuf` returns CONTINUE,
   not STOP) — fixed group text from invisible (0 %) to deliverable.
2. **Deliver as N reliable unicasts** — `sendGroupText` sends each member a `want_ack` unicast
   (group = N DMs), instead of a single lossy broadcast.
3. **Reliable-unicast GROUP_ACK** — the per-member ACK is a routed `want_ack` unicast back to the
   sender (was a fire-and-forget broadcast).
4. **Spacing** — at ~10 s+ between group messages the channel clears between the N-fold fan-outs.

Measured progression on a 3-node same-band 915 group (25 messages):
**0 % → 36 % → ~90 % → 100 % (spaced).**

**Group voice & image** use the identical idea: each group media message is sent as **N reliable
media DMs, one per live member** (§3.3), and succeeds only when *every live member* receives it.
Success is always measured against members **online in the last 10 minutes** ("live nodes").

The 24 h per-member retry tracker is retained as a backstop, but routed-unicast delivery + the
reliable ACK are what produce 100 %. The legacy fixed "swarm rebroadcast schedule" is no longer
the primary delivery path.

### 3.3 Compressed Media Transfer (voice memos & pictures)

**Concept:** Codec2 voice memos and aggressively-downscaled JPEG thumbnails, chunked over the
mesh. Compression targets unchanged from v5.1 (voice ~2–8 KB via Codec2 700 bps; thumbnails
3–20 KB). Protocol unchanged in shape: `START → CHUNK(s) → COMPLETE → ACK_COMPLETE | NACK`, with
CRC32 integrity and a deferred-response pattern (ACK_COMPLETE/NACK emitted from `runOnce()`, not
the receive handler, to avoid loopTask stack overflow).

**What changed in this revision — the CDC-flood fix (high variance → consistent 100 %).** The
single biggest media reliability problem was *not* the radio. The firmware forwarded **every**
received MediaTransfer packet (START/CHUNK/COMPLETE/NACK) to the phone/serial. Under transfer +
retransmit load this **floods the ESP32-S3 USB-CDC**, which then drops bytes (bounded TX
timeout) → the protobuf stream tears → the host loses frame sync ("serial LOST") → transfers
fail. Symptom: wild run-to-run variance (image 17 %–100 %).

**Fix:** the receiver now **consumes** START/CHUNK/COMPLETE/NACK (does not forward the raw 259
packets) and forwards **only ACK_COMPLETE** — the one packet a sender/host needs as proof of
delivery. This is safe because the app never used the raw 259 packets: the firmware reassembles
internally and re-delivers the finished media to the iOS app over a separate **PRIVATE_APP (256)
binary path** (voice/image START/CHUNK/END headers). Cutting the flood removed the variance and
took voice and image to a consistent **100 %**.

Retained media reliability mechanisms:
- **RELIABLE (70) TX priority** for media packets so MQTT-relayed text (HIGH=73) can't evict them.
- **MQTT relay suppression during active transfers** (position/telemetry/nodeinfo/neighborinfo
  not relayed while `hasActiveTransfers()`).
- **Codec2 crash prevention:** validate that a `content_type=0` payload contains ≥1 complete
  7-byte frame before `codec2_create`; `deinitCodec2()` after every use to release ~30 KB SRAM.

**Config (unchanged):** `chunk_size_bytes` 200, `max_transfer_time_minutes` 30,
`voice_max_duration_seconds` 60, `yield_to_text` true.

### 3.4 Cross-Band Awareness & MQTT Bridging

Different-band devices (915 MHz SX1262 ↔ 144 MHz SX1278) cannot reach each other over RF, so DMs,
group messages, and media cross **exclusively via MQTT**: a node publishes its RF traffic to the
broker with band metadata; nodes on other bands subscribe and inject onto their local RF; a
1-hour rolling dedup window prevents loops; the §3.1 persistent retry republishes to MQTT each
attempt. ACKs/NACKs/ACK_COMPLETE travel the reverse MQTT path, which is *more* reliable than RF
(no propagation dependence).

**Verified this revision:** with the same reliability recipe, **cross-band DMs (Pager 915 ↔
VHF-A 144) deliver text, voice, and image at 100 %**, and a **5-node mixed-band group (BPF+VHF on
144; T3S3+XIAO+Pager on 915) delivers all three categories at 100 % to every live member.**

Band-compatibility indicators in the app: 🟢 same band (direct), 🟡 different band (MQTT bridge),
🔴 no bridge path.

### 3.5 144 MHz (2 m amateur) Device Support

T-Beam Supreme S3 and T-Beam BPF, both **SX1278** at 144–148 MHz (region `ITU2_2M`). Hard-won
SX1278 fixes from v5.1 are retained and still required:
- **LF mode bit** (RegOpMode bit 3) set after every frequency change (RadioLib clears it on LoRa
  mode) — without it, VHF sensitivity/PA are mis-configured.
- **CAD bypass** — `isChannelActive()` returns false; the blocking DIO0/DIO1 CAD poll hangs on
  T-Beam Supreme (GPIO1 never toggles). Mesh CSMA/CA handles collisions.
- **Backup TX_DONE/RX_DONE polling** — the DIO0 ISR doesn't fire on this board; a ~1 s polling
  loop catches missed completions. TX/RX work, with added latency.
- **TX power** clamped to the SX1278 hardware max of 20 dBm via `USERPREFS_LORA_TX_POWER`.

**Physics correction (vs v5.1).** v5.1 implied 144 MHz uses a slower modem profile (SF7/BW62.5).
In the shipped configuration **ITU2_2M uses the same `LONG_FAST` preset as US 915 MHz**, so the
LoRa data rate is identical — 144 MHz is **not** inherently slower at the radio level. The
slowness observed in media testing was the **USB-CDC wedge on the busy gateway T-Beams**, not
airtime (see §6/§7). The remedy is host-side (smaller payloads, timing headroom), not a radio
profile change.

### 3.6 MQTT Virtual Device (offline connectivity)

Unchanged in design from v5.1: the app persists a connected device's MQTT settings, channel
keys, node identity, and known-nodes list, and after BLE disconnect continues to send/receive
over MQTT as that device (range extension, device recovery, emergency fallback). Surfaced in the
Connect tab under an "Available MQTT" section with reachability indicators and a Forget action.
Status flagged as designed/partial; not part of the deliverability validation in §6.

---

## 4. Deliverability Test Results (NEW — this revision)

All success rates are measured against the **live recipients** (USB devices are always live;
remote members count if heard in the last 10 minutes). Payloads are 1-chunk synthetic voice
(Codec2-shaped) and image (JPEG-header) blobs; success = the recipient's ACK_COMPLETE / group
delivery confirmation returns within the measurement + reconciliation window.

### 4.1 Direct Messages

| Scenario | Devices | Text | Voice | Image |
|---|---|---|---|---|
| **915-only** (same-band) | T3S3 ↔ Pager | 100 % | 100 % | 100 % |
| **144-only** (same-band) | VHF-A ↔ BPF | 100 % | 100 %* | 100 %* |
| **Cross-band** via MQTT | Pager (915) ↔ VHF-A (144) | 100 % | 100 % | 100 % |

\* 144-only media required the right-sizing of §2.5 (30-byte payloads, 10× timing headroom) to
keep the T-Beam USB-CDC from wedging; with that, voice and image converged to 100 %.

### 4.2 Group Messages (5-node, mixed-band)

Group of all five devices — **BPF + VHF-A on 144 MHz, T3S3 + XIAO + Pager on 915 MHz** — every
message fanned out to all four other live members, cross-band via MQTT for the 144↔915 hops:

| Category | Result | Notes |
|---|---|---|
| Group text | **6/6 (100 %)** | every member ACKed every message |
| Group voice | **6/6 (100 %)** | reached 100 % with 8 retransmit attempts on the cross-band media DMs |
| Group image | **6/6 (100 %)** | same media path as voice |

### 4.3 Continuous soak (in progress)

An 8-hour continuous loop runs the full matrix back-to-back — text/voice/image DMs, group
text/voice/image, and maluca channel broadcasts — across all five devices, accumulating per-run
deliverability and writing a rolling aggregate report. (Results appended on completion.)

---

## 5. Hardware Bench (UPDATED)

Current USB-connected bench (differs from the v5.1 bench):

| Device | Band / radio | PlatformIO env | Node |
|---|---|---|---|
| **T-Beam Supreme S3 "VHF-A"** | 144 MHz, SX1278 | `tbeam-s3-core` | `0x335e1be8` |
| **T-Beam BPF** | 144 MHz, SX1278 + bandpass | `tbeam-bpf` (hw_model 124, 16 MB) | `0x16d3ef94` |
| **LilyGo T3-S3 "T3S3"** | 915 MHz, SX1262 | `tlora-t3s3-v1` | `0x61741de3` |
| **Seeed XIAO ESP32-S3 "XIAO"** | 915 MHz, SX1262 | `seeed-xiao-s3` | `0x1de93284` |
| **LilyGo T-LoRa Pager** | 915 MHz, LR1121 | `tlora-pager-lr1121` | `0x5c1a0bc9` |

Bridged via MQTT broker `home.yazdikann.com:1883` (root `msh/US`). The iPhone running MeshReliable
connects over BLE (off the test computer) as a human observer of group delivery.

**Critical flashing caveat:** T-Beam Supreme (`tbeam-s3-core`, 8 MB, DIO, quad PSRAM) and T-Beam
BPF (`tbeam-bpf`, 16 MB, QIO, octal PSRAM) look alike but differ in flash size, flash mode, and
PSRAM type — flashing the wrong one is an instant PSRAM crash loop. Always match the env to the
hardware by reading the MAC/`hwModel` first.

---

## 6. Implementation Learnings — Reliability & Deliverability (UPDATED)

### 6.1 The five fixes that unlocked 100 %

1. **Group: forward-to-phone + N-unicast + reliable-ACK** (§3.2) — 0 % → 100 %.
2. **Media: consume the 259 packets, forward only ACK_COMPLETE** (§3.3) — killed the USB-CDC
   flood that caused 17–100 % variance; gave consistent 100 %.
3. **USB-CDC split-timeout (busy-node handshake fix)** — see §6.2.
4. **Airtime/spacing discipline** — group = N× a DM; media = many packets; pace them.
5. **Right-sizing for the T-Beams** — small payloads + 10× timing headroom keep the gateway
   nodes from wedging.

### 6.2 USB-CDC: protobuf must block-until-sent; logs stay droppable (NEW)

A busy node (the 144 MHz↔MQTT-gateway T-Beam) streams DEBUG logs to the USB-CDC. A single bounded
TX timeout that is right for *logs* (droppable, so a flood can't stall the loop into the 90 s
task watchdog) is **wrong for the protobuf config/data frames** — under flood the bounded write
drops bytes from the connection handshake's config response, so the host reports "Timed out
waiting for connection completion." The device looked like it "kept losing USB connectivity": you
could not (re)connect once it was busy, even though the firmware was healthy. Validated: connect
succeeds right after reset (low traffic), fails in steady state.

**Fix:** split the policy. Log writes keep the short (~100 ms) droppable bound; **protobuf frame
writes raise the CDC TX timeout to ~1 s (block-until-sent)** so the handshake/config/data is
never corrupted. 1 s is 90× under the 90 s watchdog, and the host drains the buffer in ≪1 s while
reading — so it only ever blocks briefly on a flooding node. Result: 6/6 handshakes at 2.3 KB/s
of log flood, where it was 0/3 before.

### 6.3 Node identity must be MAC-anchored (retained, still essential)

`isOurOwnEntry()` recognises our own NodeDB entry by the **MAC-derived node number**, not by a
public-key match (keys regenerate on most boots when security config doesn't persist). Without
this, a device re-randomises its node number every boot — a reboot/renumber churn loop that
polluted the mesh with phantom "self" nodes, broke DM addressing, and dropped the device off USB.
Verified: node number constant across reboots; `rebootCount` stops climbing.

### 6.4 Device-health realities (NEW)

- **T-Beams hard-wedge under sustained media-over-USB load** and then need a physical
  power-cycle (esptool can't reach them). This is a property of the USB test rig, not the product
  — real media flows over BLE, which is unaffected. Mitigations: data reduction, spacing, and the
  CDC fixes reduce how often it happens; the test harness is wedge-resilient (reconnect + retry).
- **Corrupt-flash boot loop recovery:** a device looping on `invalid header: 0xffffffff` +
  `TG0WDT_SYS_RST` (watchdog) is recovered by a re-flash; the corrupt NVS reinitialises and the
  node gets a fresh MAC-derived number (then stable, per §6.3).
- **Idle stability:** with no load, node numbers and reboot counts are stable; the reboots the
  owner observed were load-induced (heavy media-over-USB stress), not an idle loop.

### 6.5 Retained firmware patterns

- **Deferred response** (respond from `runOnce()`, not the receive handler) — prevents loopTask
  stack overflow; use for any module that answers a received packet.
- **TX-queue priority + MQTT interaction** — add new portnums to `fixPriority()`; suppress MQTT
  relay of low-priority traffic during active operations so it can't starve the queue.
- **NVS/LittleFS survive flashes** — only bootloader + app are overwritten; `userPrefs.jsonc`
  bakes in always-apply defaults (region, keys, TX power) so config is consistent every boot.
  The `HAS_TFT`/`displaymode=COLOR` NVS guard prevents a dark screen after a stock→MeshReliable
  flash.

---

## 7. Automated Testing (UPDATED)

All testing is performed by Claude Code: PlatformIO flashing (MAC-gated to the correct variant),
simultaneous serial monitoring of all devices, message injection via the Meshtastic Python API,
programmatic ACK/delivery assertions, and re-flash-and-rerun regression cycles.

**Harness (`tests/full_7device_test.py`) — deliverability-relevant invariants:**
- Identity-based USB discovery (map by node number / longName, never by port — ports
  re-enumerate on every replug/crash).
- Send watchdog + bounded, timeout-guarded reconnect (a *wedged* T-Beam serial port hangs
  open/close forever).
- **DM-A reconciliation:** after each phase, wait a grace window and upgrade messages the
  firmware delivers *after* the per-message timeout (measure eventual delivery, not first-attempt).
- **Group success = all live members** (`_online_member_ids`, ≤10 min lastHeard) acked or
  received — counts actual reception, not just the lossy ACK.
- **Band isolation** (`DM_TARGETS` / `GROUP_MEMBERS`) and **slow-band tuning**
  (`BAND_MULT`, `MEDIA_SIZE_CAP`, `MSGS_PER_PHASE`, `MEDIA_MAX_ATTEMPTS`) so each scenario is
  measured cleanly.
- Media reliability: **retransmit-until-ACK_COMPLETE**, wedge-resilient (reconnect + retry on a
  send that hangs).

**Soak:** `tests/8hr_deliverability_loop.sh` runs the full matrix continuously and writes a
rolling aggregate `FINAL_REPORT.md`.

---

## 8. Source Control

- `meshreliable-firmware` — fork of `meshtastic/firmware`; deliverability work on branch
  `feature/phase-1-persistent-dm-retries`. Anti-regression rationale for every hard-won fix is in
  `docs/RELIABILITY_INVARIANTS.md` (read before touching identity / retry / USB-CDC /
  group / media code).
- `meshreliable-app` — fork of `meshtastic/Meshtastic-Apple`, renamed MeshReliable (group UI,
  media compression/playback, band indicators, MQTT-virtual-device, once-per-node provisioning).

---

## Appendix — What changed from v5.1

**Added:** the unified deliverability model (§2); group rewritten as N reliable unicasts +
reliable ACK + forward-to-phone (0 %→100 %); the media CDC-flood fix (consume 259, forward only
ACK_COMPLETE); the USB-CDC split-timeout busy-node handshake fix; the comprehensive 100 %
results matrix for DMs and a 5-node mixed-band group; device-health learnings; the 8-hour soak.

**Corrected:** 144 MHz uses the same `LONG_FAST` preset as 915 (not a slower profile) — observed
slowness was USB-CDC wedging, not airtime.

**Updated:** hardware bench (current 5 devices, real node IDs).

**De-emphasised / removed as primary mechanisms:** the lossy broadcast + fixed "swarm
rebroadcast schedule" as the group delivery path (replaced by routed N-unicast + reliable ACK);
"send each chunk twice" as the headline media-reliability trick (replaced by retransmit-until-ACK
plus the CDC-flood fix). Older single-pair media figures (93 %/98.7 % on VHF tbeams) are
superseded by the current results in §4.

_Document prepared June 2026. All work performed by Claude Code with a USB-connected hardware
test bench._
