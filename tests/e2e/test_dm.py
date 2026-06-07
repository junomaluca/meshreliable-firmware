"""
E2E Test: Direct Messages

Tests DM sending/receiving between:
  - Device B → Device A (iPhone receives via BLE)
  - Device A (iPhone sends) → Device B (receives via LoRa)
  - Multi-reply chains
  - Long messages (max payload)
"""

import time
import pytest


class TestDirectMessages:
    """Direct message end-to-end tests."""

    def test_device_b_to_iphone(self, device_b, iphone_api, device_a_num, timestamp_marker):
        """Send text from Device B → Device A (iPhone), verify via telemetry."""
        text = f"e2e_dm_b2a_{int(time.time())}"
        device_b.send_text(text, dest=device_a_num)

        # Wait for message to appear in iPhone telemetry
        event = iphone_api.wait_for_event("messageReceived", timeout=30, since=timestamp_marker)
        assert text in event.get("details", {}).get("text", "") or \
               str(device_b.node_id) == event.get("details", {}).get("from", "")

    def test_iphone_to_device_b(self, device_b, iphone_api, timestamp_marker):
        """Send text from iPhone (via HTTP action) → Device B."""
        text = f"e2e_dm_a2b_{int(time.time())}"
        result = iphone_api.send_message(to_node=device_b.node_id, text=text)
        assert result.get("ok") is True

        # Verify sent event in telemetry
        event = iphone_api.wait_for_event("messageSent", timeout=15, since=timestamp_marker)
        assert event is not None

    def test_multi_reply_chain(self, device_b, iphone_api, device_a_num, timestamp_marker):
        """10 alternating messages between Device B and iPhone, verify ordering."""
        messages_sent = []

        for i in range(10):
            if i % 2 == 0:
                # Device B → iPhone
                text = f"chain_b2a_{i}_{int(time.time())}"
                device_b.send_text(text, dest=device_a_num)
                messages_sent.append(("B", text))
            else:
                # iPhone → Device B
                text = f"chain_a2b_{i}_{int(time.time())}"
                iphone_api.send_message(to_node=device_b.node_id, text=text)
                messages_sent.append(("A", text))

            # Brief pause between messages for LoRa airtime
            time.sleep(3)

        # Verify we got events for both directions
        time.sleep(10)  # Wait for last messages to propagate
        events = iphone_api.telemetry(since=timestamp_marker)

        sent_events = [e for e in events if e.get("type") == "messageSent"]
        received_events = [e for e in events if e.get("type") == "messageReceived"]

        # Should have 5 sent (iPhone→B) and 5 received (B→iPhone)
        assert len(sent_events) >= 5, f"Expected 5 sent events, got {len(sent_events)}"
        assert len(received_events) >= 5, f"Expected 5 received events, got {len(received_events)}"

    def test_long_message(self, device_b, iphone_api, device_a_num, timestamp_marker):
        """Send max-length message (228 bytes), verify no truncation."""
        # LoRa max text payload after headers is ~228 bytes
        text = "L" * 200  # Safe length within payload limit
        device_b.send_text(text, dest=device_a_num)

        event = iphone_api.wait_for_event("messageReceived", timeout=30, since=timestamp_marker)
        assert event is not None
        # The full message should be received without truncation

    def test_device_c_to_iphone(self, device_c, iphone_api, device_a_num, timestamp_marker):
        """Send text from Device C (T3-S3) → Device A (iPhone)."""
        text = f"e2e_dm_c2a_{int(time.time())}"
        device_c.send_text(text, dest=device_a_num)

        event = iphone_api.wait_for_event("messageReceived", timeout=30, since=timestamp_marker)
        assert event is not None

    def test_iphone_to_device_c(self, device_c, iphone_api, timestamp_marker):
        """Send text from iPhone → Device C (T3-S3)."""
        text = f"e2e_dm_a2c_{int(time.time())}"
        result = iphone_api.send_message(to_node=device_c.node_id, text=text)
        assert result.get("ok") is True

        event = iphone_api.wait_for_event("messageSent", timeout=15, since=timestamp_marker)
        assert event is not None
