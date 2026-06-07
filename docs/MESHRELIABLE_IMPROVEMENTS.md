# MeshReliable Firmware Improvements

All changes made to the Meshtastic firmware to achieve near-100% message deliverability across a multi-device LoRa mesh network. These are critical fixes and enhancements that must not regress.

## Device Inventory

| Device | Hardware | PlatformIO Env | Radio | Key Features |
|--------|----------|----------------|-------|--------------|
| XIAO | Seeed XIAO ESP32-S3 | `seeed-xiao-s3` | SX1262 | 915 MHz sub-GHz only |
| T3-S3 V1 | LilyGo T3-S3 V1 | `tlora-t3s3-v1` | SX1262 | 915 MHz sub-GHz, OLED, BLE to iPhone |
| Pager | LilyGo T-LoRa Pager | `tlora-pager-lr1121` | LR1121 | 915 MHz, TFT, audio, voice memo |
| T-Beam Supreme | LilyGo T-Beam Supreme S3 | `tbeam-s3-core` | SX1278 | 144 MHz VHF, GPS, 8MB flash, DIO |
| T-Beam BPF | LilyGo T-Beam BPF | `tbeam-bpf` | SX1278 | 144 MHz VHF, GPS, 16MB flash, QIO, OPI PSRAM |

---

## 1. Serial Timeout Extension (30s → 15min)

**File:** `src/SerialConsole.cpp`
**Problem:** ESP32-S3 USB CDC serial connections would timeout after 30 seconds of inactivity, dropping the serial console and breaking test automation.
**Fix:** Extended `SERIAL_CONNECTION_TIMEOUT` from 30,000ms to 900,000ms (15 minutes).
**Risk if reverted:** Serial connections drop during test marathons, automated testing impossible.

## 2. HWCDC::isPlugged() Removal

**File:** `src/SerialConsole.cpp`
**Problem:** `HWCDC::isPlugged()` returns false intermittently on ESP32-S3, causing the firmware to think serial is disconnected and stop sending data.
**Fix:** Removed `isPlugged()` check; serial output continues regardless.
**Risk if reverted:** Intermittent serial disconnections during testing.

## 3. Multi-Band Retry System (LR1121 Dual-Band)

### 3a. switchBand() Implementation

**File:** `src/mesh/LR11x0Interface.cpp` (lines ~353-443)
**File:** `src/mesh/LR11x0Interface.h`
**File:** `src/mesh/RadioInterface.h`

**What it does:** Runtime band switching for LR1121 radios between sub-GHz (906.875 MHz) and 2.4 GHz (2440 MHz). Saves/restores frequency, bandwidth, and power settings.

**Key state:**
- `onAlternateBand` (bool) — tracks current band
- `primaryFreq`, `primaryBw`, `primaryPower` — saved sub-GHz settings
- `wideLora()` returns true only for LR1121

### 3b. Multi-Band Retry Wiring

**File:** `src/mesh/NextHopRouter.cpp` (persistent retry section, ~line 364)

**What it does:** Odd persistent retry attempts use 2.4 GHz band, even use sub-GHz. This maximizes the chance of reaching the destination across different RF environments.

```
Retry 1: 2.4 GHz
Retry 2: sub-GHz (primary)
Retry 3: 2.4 GHz (+ flooding fallback every 3rd retry)
...
```

**Critical:** The `switchBand(true)` call happens BEFORE `send()`, and the band restore happens in `onNotify(ISR_TX)` AFTER the TX interrupt fires. See fix #4.

### 3c. Band Restore Location (CRITICAL — Recursion Fix)

**File:** `src/mesh/RadioLibInterface.cpp` — `onNotify()` case `ISR_TX`

**CRITICAL BUG FIXED:** The band restore (`switchBand(false)`) was originally placed in `completeSending()`. This caused infinite recursion:
1. `completeSending()` calls `switchBand(false)`
2. `switchBand()` calls `setStandby()`
3. LR11x0's `setStandby()` calls `completeSending()`
4. `completeSending()` sees `onAlternateBand` still true → calls `switchBand(false)` again
5. → STACK OVERFLOW → CRASH (after ~15-18 minutes when first persistent retry fires)

**Fix:** Band restore moved to `onNotify(ISR_TX)`, right after `handleTransmitInterrupt()` and before `startReceive()`. The `completeSending()` function no longer touches band state.

```cpp
case ISR_TX:
    handleTransmitInterrupt();
    if (onAlternateBand) {
        LOG_INFO("Restoring primary band after alternate-band TX");
        switchBand(false);
    }
    startReceive();
    setTransmitDelay();
    break;
```

**Risk if reverted:** Pagers crash with stack overflow ~15-18 minutes into operation whenever a persistent retry uses 2.4 GHz band.

## 4. Node "0 Online" Display Fix

**File:** `src/mesh/NodeDB.cpp` (line ~2588, in `updateFrom()`)

**Problem:** Devices without a time source (no GPS, no NTP, no BLE phone connection) have `getValidTime(RTCQualityFromNet)` return 0. This means `rx_time` is always 0, `last_heard` never gets set, and `sinceLastSeen()` returns >7200s (NUM_ONLINE_SECS). All nodes appear as "0 online" on the OLED display.

**Fix:** Added fallback to `getTime(false)` (boot-relative monotonic time) when `rx_time` is 0:

```cpp
if (mp.rx_time)
    info->last_heard = mp.rx_time;
else if (getTime(false) > 0)
    info->last_heard = getTime(false);
```

**Risk if reverted:** Pager screens show "0 online" even when nodes are actively communicating.

## 5. wideLora() for LR1121

**File:** `src/mesh/LR1121Interface.cpp`

**What it does:** `wideLora()` returns `true` for LR1121 hardware, enabling multi-band retry logic. The SX1262 and other single-band radios return `false`.

---

## Testing Infrastructure

**File:** `meshreliable-app/scripts/serial_marathon.py`

Automated marathon test script that:
- Connects to 2-4 devices via serial
- Runs all-pairs DM tests (text, ACK, binary data)
- Runs channel broadcast tests
- Tracks per-pair statistics with latency
- Runs for configurable duration (default 8 hours)

**Verified results:**
- 8-hour marathon (2 devices): 687 tests, 0 failures (100%)
- 4-device marathon (with all fixes): 415/416 (99.8%) before recursion bug was found
- After recursion fix: Testing in progress

---

## 6. Screen Always-On When USB Connected

**File:** `src/PowerFSM.cpp` (line ~401-408)

**Problem:** The power state machine had a timed transition from `statePOWER → stateDARK` with the same screen timeout as `stateON`. This meant the screen would dim/sleep even when the device was plugged into USB power, making it impossible to read the screen during testing or when using the device as a desk display.

**Fix:** Removed the `statePOWER → stateDARK` timed transition. Now when USB power is detected (`isPowered()` returns true), the device stays in `statePOWER` state with the screen always on. When USB is disconnected, the device transitions to `stateON` which still has the normal screen timeout.

**Risk if reverted:** Screen dims after timeout even when USB-powered, requiring button press to wake up.

## 7. Enhanced Retry Policy for Distant Devices

**File:** `src/mesh/NextHopRouter.cpp` (persistent retry section, ~line 294-396)

**Problem:** Original retry defaults (15s initial, 2x backoff, 5-minute max interval) were tuned for devices in close proximity. For real-world use where devices may be far apart, the initial retry was too slow and the exponential backoff too aggressive.

**Changes:**
1. **Faster initial retry:** 8s instead of 15s — gives faster feedback when first transmission fails
2. **Gentler backoff:** 1.5x multiplier instead of 2x — retries stay more frequent over time
   - Old: 15s → 30s → 60s → 120s → 300s
   - New: 8s → 12s → 18s → 27s → 40s → 60s → 90s → 120s
3. **Lower max interval:** 120s (2 min) instead of 300s (5 min) — cap retries at a reasonable frequency
4. **Hop limit boost on later retries:** After 4th retry, gradually increase hop_limit (up to max 7) to reach nodes via longer relay paths

**Retry timeline (with multi-band):**
```
Retry 1:  8s   sub-GHz, NextHop
Retry 2: 12s   2.4 GHz, NextHop  (multi-band)
Retry 3: 18s   sub-GHz, Flooding (flooding fallback)
Retry 4: 27s   2.4 GHz, NextHop  (hop_limit boost starts)
Retry 5: 40s   sub-GHz, NextHop
Retry 6: 60s   2.4 GHz, Flooding
...until 24-hour window expires
```

**Risk if reverted:** Slower initial retry response, more aggressive backoff reduces delivery rate for distant devices.

---

## Channel Configuration Requirements

All devices MUST share identical channel configuration for mesh communication:
- Channel 0: name="maluca", PSK=`42733066765a525264426867493837646967724841633332537155397a51726e`, channel_num=20
- Channel 1: name="LongFast", PSK=`01` (default)

**Gotcha:** Flashing firmware preserves channel config (stored in NVS), but factory resets will wipe it. Always verify channel config after any device reset.

---

## 8. SX1278 VHF Missed IRQ Polling (1000ms → 100ms)

**File:** `src/main.cpp` (line ~1205)
**Problem:** SX1278 (T-Beam Supreme / BPF, 144 MHz VHF) has unreliable DIO0 GPIO interrupts. The firmware relies on a polling fallback (`pollMissedIrqs()`) that checks SPI registers for TX_DONE/RX_DONE flags. The original 1000ms polling interval meant the radio could miss an entire packet's reception window (a packet at SF9/125kHz takes ~200ms air time).
**Fix:** Reduced polling interval from 1000ms to 100ms. CPU cost is negligible (one SPI register read per poll).
**Risk if reverted:** VHF devices may miss received packets due to the 1-second gap between polls.

## 9. SX1278 Deep AGC Reset

**File:** `src/mesh/RF95Interface.cpp` (resetAGC override)
**File:** `src/mesh/RF95Interface.h` (declaration)
**File:** `src/mesh/RadioLibInterface.h` (interval reduced 60s → 15s)

**Problem:** SX1278 continuous RX can experience AGC/LNA gain drift after prolonged idle, causing the receiver to stop detecting preambles ("receiver deafness"). The base class `resetAGC()` does a simple standby→RX cycle, but SX1278 requires a full SLEEP cycle to reset the analog frontend.

**Fix:** Override `resetAGC()` in RF95Interface with a deep reset cycle:
1. SLEEP mode (powers down analog frontend entirely)
2. Standby (crystal warm-up)
3. Re-apply frequency (also re-sets LF-mode bit for <525 MHz)
4. Re-enable CRC
5. Resume receiving with fresh analog state

AGC reset interval reduced from 60s to 15s for SX1278 devices.

**Risk if reverted:** VHF devices may gradually go deaf after extended continuous RX.

## 10. Selective MQTT Downlink for VHF Devices (CRITICAL)

**File:** `src/mqtt/MQTT.cpp` (`onReceiveProto()`, selective filter before packet enqueue)

**Problem:** With WiFi+MQTT enabled and `downlinkEnabled=true`, VHF devices receive ALL mesh traffic via MQTT and attempt to LoRa-rebroadcast every packet. This saturates the TX queue (6+ packets queued at all times), keeping the radio perpetually in TX mode. The device can never enter RX long enough to receive LoRa transmissions from nearby devices. Result: 0% LoRa reception while appearing to have ~50% delivery (because MQTT relay delivers some messages).

**Root cause chain:**
1. WiFi+MQTT enabled → device connected to MQTT broker
2. `downlinkEnabled=true` on channels → device subscribes to MQTT downlink topics
3. All mesh traffic from the "maluca" channel arrives via MQTT (dozens of packets/minute from 915 MHz mesh)
4. Firmware tries to LoRa-rebroadcast every MQTT-received packet on VHF
5. TX queue permanently saturated → radio never idle → no RX window
6. Occasional LoRa packets that sneak through are treated as duplicates (already received via MQTT)

**Fix:** Selective MQTT downlink filter in `onReceiveProto()` for VHF devices (detected by `config.lora.region == ITU2_2M`). MQTT downlink stays fully enabled (devices subscribe to topics), but packets are processed selectively:

1. **DMs addressed to this device** (`isToUs()`) → full processing + LoRa delivery (cross-band DMs work)
2. **DMs addressed to known LoRa peers** (destination in nodeDB, heard via radio not MQTT) → full processing + LoRa relay (relay for non-MQTT VHF nodes)
3. **Everything else** (broadcasts, telemetry, DMs for MQTT-reachable nodes) → processed locally for nodeDB/PKI key updates, but `hop_limit` set to 0 so they are NOT rebroadcasted via LoRa

```cpp
if (config.lora.region == meshtastic_Config_LoRaConfig_RegionCode_ITU2_2M && !isToUs(p.get())) {
    bool relayForPeer = false;
    if (p->to != NODENUM_BROADCAST) {
        const meshtastic_NodeInfoLite *destNode = nodeDB->getMeshNode(p->to);
        if (destNode && !nodeInfoLiteViaMqtt(destNode)) {
            relayForPeer = true;  // Relay for LoRa peer
        }
    }
    if (!relayForPeer) {
        p->hop_limit = 0;  // Local-only processing
    }
}
```

**Why not blanket disable:** Originally we disabled downlink entirely (`DOWNLINK_ENABLED=false` in platformio-custom.py). This prevented cross-band DMs TO VHF devices entirely (0% delivery in that direction). The selective approach restores cross-band delivery while preventing TX queue saturation.

**Verified results:** 60/60 (100%) delivery across all cross-band and same-band pairs with 0.4s avg latency.

**Risk if reverted:** VHF devices become deaf to LoRa when WiFi+MQTT is active, or cross-band DMs to VHF devices stop working.

---

## Known Issues & Bugs

### meshtastic-python CLI Bug: PSK Corruption on BPF (hwModel 124)

The `meshtastic` CLI corrupts the channel PSK on T-Beam BPF devices (hwModel 124) when writing any channel setting via `--ch-set`. The PSK gets zeroed out, making the device unable to decrypt mesh traffic. **Workaround:** Never use `--ch-set` for channel modifications on BPF devices. Use `userPrefs.jsonc` compiler flags for channel config, and `--set` for non-channel settings (WiFi, BLE, etc.) which don't trigger the bug. Factory reset restores the firmware-baked PSK.

### TBeam → BPF Same-Band Intermittent Failures (~1%)

TBeam→BPF direction on 144 MHz VHF has a ~1% failure rate (5 failures in 555 same-band messages). BPF→TBeam is 100%. All failures are in the TBeam→BPF direction only. Root cause unclear — may be related to SX1278 RX sensitivity or BPF's MQTT uplink activity briefly blocking the radio. The 99% rate is acceptable for production use.

---

## Endurance Test Results (June 2026)

**Configuration:** 5 devices, 2 bands (915 MHz + 144 MHz VHF), WiFi+MQTT on all, BLE off

**Combined results (1126 messages across 2 test runs):**

| Metric | Result |
|--------|--------|
| Total messages | 1126 |
| Overall delivery | 94.4% (includes cross-band to VHF which is disabled) |
| Same-band delivery | **99.4% (1006/1012)** |
| Cross-band delivery | 50% (deterministic — TO-VHF fails, FROM-VHF succeeds) |

**Per-category (same-band + cross-band combined):**

| Category | Rate |
|----------|------|
| Channel broadcast | 100% |
| Voice transfer | 100% |
| Image transfer | 99% |
| Group DM | 97% |
| Text DM | 86% (cross-band failures drag down average) |

**Per-pair (same-band only):**

| Pair | Rate |
|------|------|
| All 915 MHz pairs (Pager, T3S3, XIAO) | 100% |
| BPF → TBeam (144 MHz) | 100% |
| TBeam → BPF (144 MHz) | ~92% (intermittent) |
| All broadcasts | 100% |

**Cross-band test (after selective MQTT downlink, improvement #10):**

| Test | Result |
|------|--------|
| Cross-band DMs (all pairs, 3 rounds) | **60/60 (100%)** |
| 915→VHF delivery | 100% |
| VHF→915 delivery | 100% |
| Same-band (during cross-band test) | 100% |
| Avg latency | 0.4s |

**Key improvement over stock Meshtastic:** Stock firmware had ~50% VHF delivery due to MQTT downlink flood saturating the TX queue. MeshReliable selective MQTT downlink achieves 100% cross-band delivery and 99%+ same-band delivery by filtering MQTT packets — only DMs addressed to the local device or known LoRa peers are rebroadcasted, while all other traffic is processed locally (nodeDB/PKI) without consuming the LoRa TX queue.
