"""
E2E Test Harness — conftest.py

Fixtures for end-to-end testing with:
  - Device B (XIAO #2): USB serial via meshtastic CLI
  - Device C (T3-S3): USB serial via meshtastic CLI
  - iPhone app: HTTP telemetry via iproxy tunnel (localhost:8765)
    - BLE-connected to Device A (XIAO #1)

Requires:
  - iproxy 8765 8765 running (USB tunnel to iPhone)
  - iPhone app running with MESHRELIABLE_DEBUG=1
  - Device B and C connected via USB
"""

import os
import sys
import time
import subprocess
import json
import pytest
import requests

# ─── Configuration ────────────────────────────────────────────────────────

DEVICE_B_PORT = os.environ.get("DEVICE_B", "/dev/cu.usbmodem21201")
DEVICE_C_PORT = os.environ.get("DEVICE_C", "/dev/cu.usbmodemB8F862D9F8881")
MESHTASTIC_BIN = os.environ.get("MESHTASTIC_BIN", "/Users/patrick/Library/Python/3.14/bin/meshtastic")
IPHONE_API_BASE = os.environ.get("IPHONE_API", "http://localhost:8765")

# Timeout for waiting on telemetry events (seconds)
DEFAULT_TIMEOUT = 30


# ─── iPhone API Client ────────────────────────────────────────────────────

class iPhoneAPI:
    """HTTP client for the DebugHTTPServer on the iPhone app."""

    def __init__(self, base_url=IPHONE_API_BASE):
        self.base_url = base_url

    def status(self):
        """Get app connection status."""
        r = requests.get(f"{self.base_url}/status", timeout=5)
        r.raise_for_status()
        return r.json()

    def telemetry(self, since=None):
        """Get telemetry events, optionally filtered by ISO8601 timestamp."""
        params = {}
        if since:
            params["since"] = since
        r = requests.get(f"{self.base_url}/telemetry", params=params, timeout=5)
        r.raise_for_status()
        return r.json()

    def groups(self):
        """Get active groups with member lists."""
        r = requests.get(f"{self.base_url}/groups", timeout=5)
        r.raise_for_status()
        return r.json()

    def send_message(self, to_node: int, text: str):
        """Trigger DM send from iPhone app."""
        r = requests.post(
            f"{self.base_url}/action/send-message",
            json={"to": to_node, "text": text},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    def send_group_message(self, group_id: int, text: str):
        """Trigger group message send from iPhone app."""
        r = requests.post(
            f"{self.base_url}/action/send-group-message",
            json={"groupId": group_id, "text": text},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    def record_voice_memo(self):
        """Trigger voice memo recording on iPhone."""
        r = requests.post(
            f"{self.base_url}/action/record-voice-memo",
            json={},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    def messages(self, limit: int = 50, userNum: int = None, channel: int = None):
        """Get recent messages from SwiftData."""
        params = {"limit": limit}
        if userNum is not None:
            params["userNum"] = userNum
        if channel is not None:
            params["channel"] = channel
        r = requests.get(f"{self.base_url}/messages", params=params, timeout=5)
        r.raise_for_status()
        return r.json()

    def images(self, userNum: int = None):
        """Get recent image messages."""
        params = {}
        if userNum is not None:
            params["userNum"] = userNum
        r = requests.get(f"{self.base_url}/images", params=params, timeout=5)
        r.raise_for_status()
        return r.json()

    def transfers(self):
        """Get active transfers (incoming/outgoing)."""
        r = requests.get(f"{self.base_url}/transfers", timeout=5)
        r.raise_for_status()
        return r.json()

    def connection(self):
        """Get detailed BLE connection info."""
        r = requests.get(f"{self.base_url}/connection", timeout=5)
        r.raise_for_status()
        return r.json()

    def nodes(self):
        """Get all known mesh nodes."""
        r = requests.get(f"{self.base_url}/nodes", timeout=5)
        r.raise_for_status()
        return r.json()

    def send_channel_message(self, text: str, channel: int = 0):
        """Send a broadcast message on a channel."""
        r = requests.post(
            f"{self.base_url}/action/send-channel-message",
            json={"text": text, "channel": channel},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    def send_test_image(self, to_node: int, label: str = "TEST"):
        """Send a synthetic test image to a node."""
        r = requests.post(
            f"{self.base_url}/action/send-test-image",
            json={"to": to_node, "label": label},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    def send_voice_memo(self, to_node: int, duration_ms: int = 500):
        """Send a synthetic voice memo to a node."""
        r = requests.post(
            f"{self.base_url}/action/send-voice-memo",
            json={"to": to_node, "durationMs": duration_ms},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    def navigate(self, tab: str, userNum: int = None, channel: int = None):
        """Navigate the iOS app to a specific tab/view."""
        body = {"tab": tab}
        if userNum is not None:
            body["userNum"] = userNum
        if channel is not None:
            body["channel"] = channel
        r = requests.post(
            f"{self.base_url}/action/navigate",
            json=body,
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    def ble_reconnect(self):
        """Force BLE disconnect and reconnect."""
        r = requests.post(
            f"{self.base_url}/action/ble-reconnect",
            json={},
            timeout=15,
        )
        r.raise_for_status()
        return r.json()

    def clear_transfers(self):
        """Clear all pending/outgoing transfers."""
        r = requests.post(
            f"{self.base_url}/action/clear-transfers",
            json={},
            timeout=5,
        )
        r.raise_for_status()
        return r.json()

    def ble_debug(self):
        """Get BLE debug counters."""
        r = requests.get(f"{self.base_url}/ble-debug", timeout=5)
        r.raise_for_status()
        return r.json()

    def screenshot(self, save_path: str = None):
        """Capture a screenshot. Returns PNG bytes or saves to file."""
        r = requests.get(f"{self.base_url}/screenshot", timeout=10)
        r.raise_for_status()
        if save_path:
            with open(save_path, 'wb') as f:
                f.write(r.content)
        return r.content

    def create_group(self, member_nums: list):
        """Create a new group with the given member node numbers."""
        r = requests.post(
            f"{self.base_url}/action/create-group",
            json={"members": member_nums},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    def send_image(self, to_node: int, image_base64: str):
        """Send a custom image (base64-encoded JPEG) to a node."""
        r = requests.post(
            f"{self.base_url}/action/send-image",
            json={"to": to_node, "imageBase64": image_base64},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    # ── Polling helpers ───────────────────────────────────────────────

    def wait_for_message(self, text_contains: str = None, from_user: int = None,
                         timeout: int = DEFAULT_TIMEOUT):
        """Poll /messages until a matching message appears."""
        start = time.time()
        seen_ids = set()
        while time.time() - start < timeout:
            msgs = self.messages(limit=20, userNum=from_user)
            for msg in msgs:
                mid = msg.get("messageId")
                if mid in seen_ids:
                    continue
                text = msg.get("text", "")
                if text_contains and text_contains not in text:
                    continue
                if from_user and msg.get("fromUserNum") != from_user:
                    continue
                return msg
            seen_ids.update(m.get("messageId") for m in msgs)
            time.sleep(0.5)
        raise TimeoutError(
            f"No message matching text='{text_contains}' from={from_user} within {timeout}s"
        )

    def wait_for_image(self, from_user: int = None, timeout: int = DEFAULT_TIMEOUT):
        """Poll /messages until an image message appears."""
        start = time.time()
        seen_ids = set()
        while time.time() - start < timeout:
            msgs = self.messages(limit=20, userNum=from_user)
            for msg in msgs:
                mid = msg.get("messageId")
                if mid in seen_ids:
                    continue
                if msg.get("hasImage"):
                    if from_user and msg.get("fromUserNum") != from_user:
                        continue
                    return msg
            seen_ids.update(m.get("messageId") for m in msgs)
            time.sleep(0.5)
        raise TimeoutError(
            f"No image message from={from_user} within {timeout}s"
        )

    def wait_for_event(self, event_type: str, timeout: int = DEFAULT_TIMEOUT, since=None):
        """Poll telemetry until an event of the given type appears."""
        start = time.time()
        while time.time() - start < timeout:
            events = self.telemetry(since=since)
            for ev in events:
                if ev.get("type") == event_type:
                    return ev
            time.sleep(0.5)
        raise TimeoutError(f"No '{event_type}' event within {timeout}s")

    def wait_for_events(self, event_type: str, count: int, timeout: int = DEFAULT_TIMEOUT, since=None):
        """Poll telemetry until N events of the given type appear."""
        start = time.time()
        while time.time() - start < timeout:
            events = self.telemetry(since=since)
            matching = [e for e in events if e.get("type") == event_type]
            if len(matching) >= count:
                return matching[:count]
            time.sleep(0.5)
        raise TimeoutError(f"Only got {len(matching)}/{count} '{event_type}' events within {timeout}s")


# ─── Meshtastic Device Helper ────────────────────────────────────────────

class MeshtasticDevice:
    """Wrapper around meshtastic CLI for sending commands to a USB-connected device."""

    def __init__(self, port: str, name: str = ""):
        self.port = port
        self.name = name
        self._node_id = None

    @property
    def node_id(self):
        if self._node_id is None:
            self._node_id = self._get_node_id()
        return self._node_id

    def _get_node_id(self):
        """Get the node number from the device."""
        result = self._run(["--info"])
        # Parse node ID from info output
        for line in result.stdout.split("\n"):
            if "\"num\":" in line:
                import re
                match = re.search(r'"num":\s*(\d+)', line)
                if match:
                    return int(match.group(1))
        raise RuntimeError(f"Could not determine node ID for {self.name} on {self.port}")

    def send_text(self, text: str, dest: int = None, channel: int = 0):
        """Send a text message."""
        args = ["--sendtext", text, "--ch-index", str(channel)]
        if dest is not None:
            args += ["--dest", str(dest)]
        return self._run(args)

    def send_bytes(self, data: bytes, portnum: int, dest: int = None, channel: int = 0):
        """Send raw bytes on a specific portnum."""
        hex_str = data.hex()
        args = ["--port", self.port, "--sendtext", f"--dest", str(dest or 0xFFFFFFFF)]
        # Use --send-bytes with portnum
        args = [
            "--sendtext", hex_str,
            "--dest", str(dest or 0xFFFFFFFF),
            "--ch-index", str(channel),
        ]
        return self._run(args)

    def _run(self, args, timeout=30):
        """Run meshtastic CLI command."""
        cmd = [MESHTASTIC_BIN, "--port", self.port] + args
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            return result
        except subprocess.TimeoutExpired:
            raise TimeoutError(f"Meshtastic CLI timeout: {' '.join(cmd)}")


# ─── iproxy Management ────────────────────────────────────────────────────

def ensure_iproxy():
    """Start iproxy if not already running."""
    result = subprocess.run(["pgrep", "-f", "iproxy 8765"], capture_output=True)
    if result.returncode != 0:
        subprocess.Popen(
            ["iproxy", "8765", "8765"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        time.sleep(1)


# ─── Fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def iphone_api():
    """iPhone debug HTTP server client."""
    ensure_iproxy()
    api = iPhoneAPI()
    # Verify connectivity
    try:
        status = api.status()
        if status.get("status") != "connected":
            pytest.skip(f"iPhone app not connected (status: {status.get('status')})")
    except (requests.ConnectionError, requests.Timeout):
        pytest.skip("Cannot reach iPhone debug server at localhost:8765")
    return api


@pytest.fixture(scope="session")
def device_b():
    """Device B (XIAO #2) connected via USB serial."""
    if not os.path.exists(DEVICE_B_PORT):
        pytest.skip(f"Device B not found at {DEVICE_B_PORT}")
    dev = MeshtasticDevice(DEVICE_B_PORT, name="B")
    return dev


@pytest.fixture(scope="session")
def device_c():
    """Device C (T3-S3) connected via USB serial."""
    if not os.path.exists(DEVICE_C_PORT):
        pytest.skip(f"Device C not found at {DEVICE_C_PORT}")
    dev = MeshtasticDevice(DEVICE_C_PORT, name="C")
    return dev


@pytest.fixture(scope="session")
def device_a_num(iphone_api):
    """Node number of Device A (iPhone's connected node)."""
    status = iphone_api.status()
    num = status.get("deviceNum", 0)
    if num == 0:
        pytest.skip("Device A node number not available")
    return num


@pytest.fixture
def timestamp_marker():
    """Returns current ISO8601 timestamp for filtering telemetry since this point."""
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()
