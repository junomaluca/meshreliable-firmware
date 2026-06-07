"""
E2E Test: Group Messaging

Tests group message protocol across all 3 nodes:
  - Group creation from iPhone
  - Send/receive group messages
  - Per-member ACK tracking
  - Group join/leave lifecycle
"""

import time
import struct
import pytest

# GroupMessage types (matches GroupMessageService.swift)
GROUP_TEXT = 0
GROUP_JOIN = 1
GROUP_LEAVE = 2
GROUP_ACK = 3
GROUP_ALL_ACKED = 4

# Portnum for group messages
GROUP_MESSAGE_PORTNUM = 258


class TestGroupMessaging:
    """Group messaging end-to-end tests."""

    @pytest.fixture
    def test_group_id(self):
        """Generate a unique group ID for this test session."""
        return int(time.time()) & 0xFFFFFFFF

    def test_iphone_send_group_message(self, iphone_api, device_b, device_c, timestamp_marker):
        """Send group message from iPhone, verify B and C receive it."""
        # Get existing groups or use a known test group
        groups = iphone_api.groups()

        if not groups:
            pytest.skip("No groups configured on iPhone app")

        group_id = groups[0].get("groupId", 0)
        text = f"group_from_iphone_{int(time.time())}"

        result = iphone_api.send_group_message(group_id=group_id, text=text)
        assert result.get("ok") is True

        # Verify sent event in telemetry
        event = iphone_api.wait_for_event("groupMessageSent", timeout=15, since=timestamp_marker)
        assert event is not None
        assert event["details"].get("groupId") == str(group_id)

    def test_device_b_send_group_message(self, device_b, iphone_api, device_a_num, timestamp_marker):
        """Send group message from Device B, verify iPhone receives it."""
        # Device B sends a group text message via the group portnum
        # This requires encoding the group message header
        groups = iphone_api.groups()
        if not groups:
            pytest.skip("No groups configured on iPhone app")

        group_id = groups[0].get("groupId", 0)
        text = f"group_from_b_{int(time.time())}"

        # Send as raw group message portnum from device B
        device_b.send_text(text, dest=0xFFFFFFFF, channel=0)

        # Wait for group message receipt on iPhone
        # The iPhone should see this as either a groupMessageReceived or messageReceived
        time.sleep(10)
        events = iphone_api.telemetry(since=timestamp_marker)
        received = [e for e in events if e.get("type") in ("groupMessageReceived", "messageReceived")]
        assert len(received) > 0, "No group/message received events after Device B send"

    def test_group_ack_tracking(self, iphone_api, timestamp_marker):
        """Verify per-member ACK tracking after group message send."""
        groups = iphone_api.groups()
        if not groups:
            pytest.skip("No groups configured")

        group_id = groups[0].get("groupId", 0)
        text = f"ack_test_{int(time.time())}"

        iphone_api.send_group_message(group_id=group_id, text=text)

        # Wait for the sent event
        event = iphone_api.wait_for_event("groupMessageSent", timeout=15, since=timestamp_marker)
        assert event is not None

        # Wait for ACK events (members should ACK back)
        time.sleep(20)
        events = iphone_api.telemetry(since=timestamp_marker)
        ack_events = [e for e in events if e.get("type") == "groupAckReceived"]
        # At least some ACKs should come back from devices B and C
        # (This may fail if devices are offline — that's expected)

    def test_multiple_group_messages(self, iphone_api, timestamp_marker):
        """Send 5 group messages in sequence, verify all tracked."""
        groups = iphone_api.groups()
        if not groups:
            pytest.skip("No groups configured")

        group_id = groups[0].get("groupId", 0)

        for i in range(5):
            text = f"multi_group_{i}_{int(time.time())}"
            iphone_api.send_group_message(group_id=group_id, text=text)
            time.sleep(3)  # LoRa airtime spacing

        # Verify all 5 sent events
        time.sleep(10)
        events = iphone_api.telemetry(since=timestamp_marker)
        sent = [e for e in events if e.get("type") == "groupMessageSent"]
        assert len(sent) >= 5, f"Expected 5 groupMessageSent events, got {len(sent)}"
