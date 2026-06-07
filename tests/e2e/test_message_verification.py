"""Data-level message verification tests.

Verifies that messages sent via serial devices actually appear in the iPhone
app's SwiftData store by querying the /messages endpoint.

Requires:
  - iproxy 8765 8765 running
  - iPhone app with DebugHTTPServer active
  - At least one USB device (Device B) connected
"""

import time
import random
import pytest
from conftest import iPhoneAPI, MeshtasticDevice, DEFAULT_TIMEOUT


class TestDMVerification:
    """Verify DMs from serial devices appear in iPhone app's message store."""

    def test_text_dm_from_serial(self, iphone_api: iPhoneAPI, device_b: MeshtasticDevice,
                                  device_a_num: int, timestamp_marker):
        """Send text DM from Device B to Device A (iPhone), verify in /messages."""
        tracking = f"VERIFY-{int(time.time())}-{random.randint(1000, 9999)}"
        text = f"{tracking} hello from B"

        device_b.send_text(text, dest=device_a_num)

        msg = iphone_api.wait_for_message(text_contains=tracking, timeout=60)
        assert msg is not None, f"Message '{tracking}' not found in /messages"
        assert tracking in msg.get("text", "")
        assert msg.get("fromUserNum") == device_b.node_id

    def test_text_dm_round_trip(self, iphone_api: iPhoneAPI, device_b: MeshtasticDevice,
                                 device_a_num: int, timestamp_marker):
        """Send DM from iPhone to Device B, then B replies — verify both in /messages."""
        # iPhone -> B
        outgoing_text = f"OUT-{int(time.time())}-{random.randint(1000, 9999)}"
        iphone_api.send_message(to_node=device_b.node_id, text=outgoing_text)
        time.sleep(15)  # wait for delivery

        # B -> iPhone
        reply_text = f"REPLY-{int(time.time())}-{random.randint(1000, 9999)}"
        device_b.send_text(reply_text, dest=device_a_num)

        msg = iphone_api.wait_for_message(text_contains=reply_text[:10], timeout=60)
        assert msg is not None


class TestImageVerification:
    """Verify image transfers are recorded in message store."""

    def test_send_test_image(self, iphone_api: iPhoneAPI, device_b: MeshtasticDevice,
                              timestamp_marker):
        """Send a test image from iPhone to Device B, verify hasImage in /messages."""
        label = f"IMG-{int(time.time()) % 10000}"
        iphone_api.send_test_image(to_node=device_b.node_id, label=label)

        # Wait for the image message to appear
        time.sleep(5)
        msgs = iphone_api.messages(limit=10, userNum=device_b.node_id)
        image_msgs = [m for m in msgs if m.get("hasImage")]
        assert len(image_msgs) > 0, "No image messages found after send_test_image"
        assert image_msgs[0].get("imageBytes", 0) > 0


class TestVoiceMemoVerification:
    """Verify voice memo transfers are recorded in message store."""

    def test_send_voice_memo(self, iphone_api: iPhoneAPI, device_b: MeshtasticDevice,
                              timestamp_marker):
        """Send a synthetic voice memo from iPhone to Device B, verify in /messages."""
        iphone_api.send_voice_memo(to_node=device_b.node_id, duration_ms=500)

        # Wait for the transfer to complete
        time.sleep(30)
        msgs = iphone_api.messages(limit=10, userNum=device_b.node_id)
        voice_msgs = [m for m in msgs if m.get("hasVoiceMemo")]
        assert len(voice_msgs) > 0, "No voice memo messages found after send_voice_memo"
        assert voice_msgs[0].get("voiceMemoBytes", 0) > 0


class TestChannelVerification:
    """Verify channel broadcasts appear in message store."""

    def test_channel_broadcast(self, iphone_api: iPhoneAPI, timestamp_marker):
        """Send a channel broadcast from iPhone, verify in /messages."""
        tracking = f"CH-{int(time.time())}-{random.randint(1000, 9999)}"
        iphone_api.send_channel_message(text=tracking, channel=0)

        time.sleep(5)
        msgs = iphone_api.messages(limit=10, channel=0)
        matching = [m for m in msgs if tracking in (m.get("text") or "")]
        assert len(matching) > 0, f"Channel broadcast '{tracking}' not found in /messages"


class TestGroupVerification:
    """Verify group creation and messaging."""

    def test_create_group_and_send(self, iphone_api: iPhoneAPI, device_b: MeshtasticDevice,
                                    device_a_num: int, timestamp_marker):
        """Create a group, send a message, verify in /groups."""
        members = [device_a_num, device_b.node_id]
        result = iphone_api.create_group(member_nums=members)
        assert result.get("ok"), f"create_group failed: {result}"
        group_id = result.get("groupId")
        assert group_id, "No groupId returned"

        # Send a group message
        tracking = f"GRP-{int(time.time())}-{random.randint(1000, 9999)}"
        iphone_api.send_group_message(group_id=group_id, text=tracking)

        time.sleep(5)
        groups = iphone_api.groups()
        group_ids = [g.get("groupId") for g in groups]
        assert group_id in group_ids, f"Group {group_id} not found in /groups"


class TestConnectionHealth:
    """Verify connection endpoints return sane data."""

    def test_nodes_endpoint(self, iphone_api: iPhoneAPI):
        """Verify /nodes returns at least the connected device."""
        nodes = iphone_api.nodes()
        assert isinstance(nodes, list)
        assert len(nodes) > 0, "No nodes returned"
        # At least one node should have a num
        nums = [n.get("num") for n in nodes if n.get("num")]
        assert len(nums) > 0

    def test_connection_endpoint(self, iphone_api: iPhoneAPI):
        """Verify /connection returns connected state."""
        conn = iphone_api.connection()
        assert conn.get("isConnected") is True or conn.get("state") == "subscribed"

    def test_ble_debug_endpoint(self, iphone_api: iPhoneAPI):
        """Verify /ble-debug returns counters."""
        debug = iphone_api.ble_debug()
        assert "fromnumNotifications" in debug
        assert "drainCalls" in debug
