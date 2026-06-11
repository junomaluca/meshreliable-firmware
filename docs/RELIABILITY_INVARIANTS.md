# MeshReliable — Critical Invariants (DO NOT REGRESS)

> **Why this file exists:** the changes below were hard-won across long debugging
> sessions and are easy to silently revert in a future edit. Each entry says **what
> the code must do**, **why** (the bug it prevents), and **what NOT to change it back
> to**. If you are about to "clean up" or "simplify" any of these, read the rationale
> first — you will very likely be reintroducing a real, observed failure.
>
> Verify before editing any device-identity / retry / USB code:
> `git log --oneline -- src/mesh/NodeDB.cpp src/mesh/ReliableRouter.cpp src/modules/GroupMessageModule.* src/modules/MediaTransferModule.h src/SerialConsole.cpp`

---

## 1. Node identity must be MAC-anchored (anti node-number churn)

**File:** `src/mesh/NodeDB.cpp` → `NodeDB::pickNewNodeNum()`
**Invariant:** `isOurOwnEntry()` must recognise our own NodeDB entry by the **stable
MAC-derived node number** (`n->num == macNum`), and the device must converge on
`macNum`. It must NOT decide "this entry isn't us" based on a **public-key mismatch
alone**.

**Why / bug prevented:** the security keys regenerate on most boots when the security
config doesn't persist. The old key-only self-check therefore treated the device's own
stale entry as "another node" on every boot and **re-randomised the node number** — a
reboot/renumber churn loop that:
- polluted the mesh with dozens of phantom "self" entries (e.g. 50+ "BPF A" nodes),
- broke DM addressing (you can't DM a node whose ID changes every few seconds),
- dropped the device off USB every few seconds.

Verified fix: node number stays constant across reboots; `rebootCount` stops climbing.

**DO NOT** revert `isOurOwnEntry` to "compare public keys only", and do not remove the
`n->num == macNum` short-circuit or the MAC-preferred candidate selection.

---

## 2. Licensed / HAM operator mode is forced OFF everywhere

**Files:** `src/mesh/NodeDB.cpp` (boot force-off + clears the NodeDB self-entry licensed
bit), `src/modules/AdminModule.cpp` (`handleSetOwner` forces `is_licensed=false`;
`handleSetHamMode` is a no-op early-return).
**Invariant:** `owner.is_licensed` is always false on this network.

**Why / bug prevented:** this is a **private, encrypted** network. Licensed/HAM mode
*strips channel encryption* (plaintext ham operation) and was getting stuck "on" (the
app couldn't disable it) because a stale NodeDB licensed bit was restored and
re-broadcast. Forcing it off — and clearing the NodeDB bit — keeps encryption intact.

**DO NOT** re-enable HAM mode handling or let `handleSetOwner` copy `o.is_licensed`.

---

## 3. DM persistent retry policy (reliability ≫ latency)

**File:** `src/mesh/ReliableRouter.cpp` → `getReliableConfig()`; engine in
`src/mesh/NextHopRouter.cpp` (`startPersistentRetransmission` / `doRetransmissions`).
**Invariant (defaults):** `retry_window_seconds = 86400` (24 h), exponential backoff
from `initial_retry_interval_ms` (8 s VHF / 15 s 915) capped at `max_retry_interval_ms`
= **60 s VHF / 90 s 915** (policy "DM-B"), stops on end-to-end ACK.

**Why:** in an ideal co-located layout, residual loss is LoRa PHY (collisions, ACK loss),
not topology — so ~100% delivery requires retransmit-until-ACK. Long window + bounded
cadence = reliability without saturating the band. The product owner explicitly accepts
"messages may arrive much later."

**DO NOT** shrink `retry_window_seconds` or raise the max interval back to 120 s/300 s.

---

## 4. Group-message reliability — 24 h per-member retry (policy "GRP-A")

**Group text reaches ~100% (DM-grade) via FOUR pieces — do not remove any:**
1. **Forward to phone** — `handleReceivedProtobuf` returns `false` (CONTINUE), not `true`
   (STOP). Returning STOP *consumes* the packet so received group messages never reach the
   app/phone (group text scored 0%, invisible in the app). MediaTransferModule returns false
   for the same reason.
2. **Deliver as N reliable unicasts** — `sendGroupText` sends each member a `want_ack`
   unicast (routed end-to-end ACK + 24h retry), i.e. literally "group = N DMs". A broadcast
   loses ~30% per member on first shot.
3. **Reliable-unicast GROUP_ACK** — `sendAck` unicasts (want_ack) the ACK back to the original
   sender, NOT a fire-and-forget broadcast; otherwise the sender can't confirm all-acked even
   when delivery succeeded (capped success at ~p^N — e.g. 36% for a 3-node group).
4. **Spacing matters** — a group message is N× a DM's airtime (N unicasts + N ACKs + retries),
   so firing them in a tight burst saturates the LoRa channel (~80% delivery). At realistic
   cadence (~10s+ between group messages) it is 100%. This is RF physics, not a protocol bug.

Measured progression (3-node same-band 915 group, 25 msgs): 0% → 36% → ~90% → **100%** (spaced).


**Files:** `src/modules/GroupMessageModule.h` / `.cpp`.
**Invariant:** group messages are a **per-member-ACKed** protocol (recipient manifest +
`GROUP_ACK` + `GROUP_ALL_ACKED`), NOT fire-and-forget. The retry schedule
(`REBROADCAST_INTERVALS`) is front-loaded then **repeats the last (long) interval with
jitter** until `TRACKING_TIMEOUT_MS = 86400000` (24 h). Only **un-ACKed** members are
retransmitted.

**Why / bug prevented:** the original policy gave up after 5 rebroadcasts / 10 minutes,
so a member that missed the burst never caught up. (Group messages are distinct from
plain **channel/maluca broadcasts**, which are genuinely unconfirmed.)

**DO NOT** revert to `MAX_REBROADCASTS = 5` / 10-minute timeout, and do not "simplify"
group messages into a plain channel broadcast.

---

## 5. Media (voice/image) long retry (policy "DM-C")

**File:** `src/modules/MediaTransferModule.h`.
**Invariant:** `MAX_COMPLETE_RESENDS = 120`, `COMPLETE_RESEND_INTERVAL_MS = 30000`,
`TRANSFER_TIMEOUT_MS = 3600000` (1 h). The sender keeps resending COMPLETE and the
receiver keeps proactively NACKing missing chunks for up to ~1 h.

**Why:** media used to give up after 3 COMPLETE resends (~30 s); under any loss that
abandoned the transfer. The long window lets voice/image approach ~100%.

**DO NOT** revert `MAX_COMPLETE_RESENDS` to 3 or shorten `TRANSFER_TIMEOUT_MS`.

---

## 5b. Media: CONSUME MediaTransfer(259) packets — do NOT forward them to the phone/serial

**File:** `src/modules/MediaTransferModule.cpp` → `handleReceivedProtobuf()` switch.
**Invariant:** `MEDIA_START`, `MEDIA_CHUNK`, `MEDIA_COMPLETE`, `MEDIA_NACK`, `MEDIA_CANCEL`
all `return true` (consume). ONLY `MEDIA_ACK_COMPLETE` returns `false` (forward — it is the
host's per-transfer delivery confirmation).

**Why / bug prevented:** forwarding every received MediaTransfer(259) packet to the phone
floods the USB-CDC; with `setTxTimeoutMs(100)` the CDC then **drops bytes** → corrupted
protobuf → the host loses frame sync ("serial LOST") → reconnect churn → media transfers
fail. Symptom: wild run-to-run variance (image 17 %–100 %). Consuming the in-transfer
packets removed the variance and took **voice to 100 % / image ~100 %** on the 915 mesh.

**The app does NOT use the 259 packets** — so consuming them is safe. Received media is
reassembled internally; on COMPLETE the `completionCallback` (set by `VoiceMemoModule`)
re-forwards the finished media to the iOS app over **PRIVATE_APP (256)** using the app's
own header format (`voiceMemoChunk=0x02` / `imageChunk=0x05`, see `MeshReliableService`).
The 259 forward was only ever for test monitoring, which the host doesn't need (it scores
via ACK_COMPLETE). NOTE: this supersedes the older "MediaTransferModule returns false"
remark in §4.1 — that was about the group module; media now consumes.

**DO NOT** change these cases back to `return false` / `break` (re-introduces the CDC
flood and the high-variance media failures). Real media uses BLE+PRIVATE_APP, unaffected.

**Test harness** (`tests/full_7device_test.py`): media must **retransmit the whole transfer
until ACK_COMPLETE** (the harness injects raw packets, so the firmware sender's retransmit
never runs), be **wedge-resilient** (reconnect + retry on a send that raises), and **space**
transfers (`MEDIA_SEND_DELAY`). `_reconnect` must null-check `getMyNodeInfo()`.

---

## 6. USB-CDC writes must be non-blocking (anti media-load watchdog reset)

**File:** `src/SerialConsole.cpp` (guarded for `ESP32S2/S3/C3/C6`).
**Invariant:** `Port.setTxTimeoutMs(100)` after `Port.begin()` — a small, NON-ZERO bound.

**Why / bug prevented:** on ESP32-S3 the console is HWCDC. Under heavy serial output
(media-transfer packet forwarding) a **blocking** CDC write stalls the loop task long
enough to trip the task watchdog → chip reset → USB re-enumerates (host sees
"Device not configured" mid-transfer). Non-blocking writes drop a few console bytes
instead of resetting.

**DO NOT** remove the call, and **DO NOT set it to 0** — 0 (fully non-blocking) drops bytes during the connect-time config dump, corrupting the protobuf stream so the phone/CLI handshake never completes (observed: `Error parsing FromRadio`, every device unresponsive to --info). Keep a small non-zero value (~100 ms).

---

## 7b. BLE config-save: never deinit BLE mid-transaction (MQTT/Serial)

**File:** `src/modules/AdminModule.cpp` → `handleSetModuleConfig()`, the `mqtt` and
`serial` cases.
**Invariant:** the `disableBluetooth()` calls in those two cases MUST be guarded by
`if (!hasOpenEditTransaction)`.

**Why / bug prevented:** clients (iOS app, official web client, python CLI) edit config
in a `begin_edit → set → commit_edit` transaction. `saveChanges()` defers the disk write
until `commit_edit_settings`. `disableBluetooth()` does `nimbleBluetooth->deinit()` —
it tears down the link. Calling it *during* the MQTT/Serial set drops the BLE connection
before the client's `commit` arrives, so the change is **never written to flash** —
silently lost. Serial/USB is unaffected (that's why USB config persisted but BLE/app/web
did not). In a transaction the `commit_edit_settings` handler disables BT *after* the
commit, which is the correct place. Affects EVERY variant (shared AdminModule).

**DO NOT** remove the `!hasOpenEditTransaction` guard from either case (re-introduces
"settings don't save over BLE"). This is upstream behavior (commit `beb268ff25`); the
guard is a MeshReliable fix.

---

## 7. Test harness invariants (`tests/full_7device_test.py`)

This harness took ~15 runs to get reliable on flaky hardware. Keep:
- **Identity-based USB discovery** — map devices by owner `longName`, never by a
  hardcoded port (ports re-enumerate on every replug/crash; node numbers churn).
  Match flash/test targets by **deviceId**, never port number.
- **Send watchdog** (`guard_iface_sends`, 20 s) — a wedged pager USB-TX must not stall.
- **Bounded + timeout-guarded reconnect** (`MAX_RECONNECTS`, `run_with_timeout`) — a
  *wedged* (vs cleanly dropped) tbeam serial port hangs `SerialInterface()`/`close()`
  forever; the guards prevent an infinite reconnect stall.
- **Probe-retry** — these radios often time out on the FIRST connect then answer on the
  second; a single probe drops them (esp. the Pager).
- **DM-A reconciliation** — after each phase, wait a grace window and upgrade messages
  the firmware retries delivered *after* the per-message timeout (and count iOS-app
  reception, since the Pager gets broadcasts over BLE even when its USB `_on_rx` is
  disconnected). Without this the test measures first-attempt, not eventual, delivery.

**Known hardware facts (not bugs to "fix"):** the tbeam VHF-band devices (Supreme/VHF,
BPF) chronically wedge under media-over-USB load and need physical power-cycles; the
Pager's LR1121 transmits weakly to 915 SX1262 peers (delivers cross-band via MQTT). The
real media path is **BLE**, not USB-CDC.

---

## App-side invariants (meshreliable-app)
See `meshreliable-app/AGENTS.md`. Summary:
- **`UserConfig.swift`** — do NOT re-add `.foregroundColor(.gray)` to the Short Name
  `TextField` (it makes an editable field look permanently disabled).
- **`UserMessageList.swift`** — the per-user message predicate MUST keep
  `&& $0.toUser != nil`, or channel/broadcast messages leak into DM threads.
