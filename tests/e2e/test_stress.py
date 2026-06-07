"""
E2E Test: Stress & Load Testing

Tests system stability under load:
  - 50 rapid messages from Device B → iPhone
  - Concurrent DM + group message streams
  - Voice memo during active text messaging
  - 10-minute sustained operation
"""

import time
import threading
import pytest


class TestStress:
    """Load and stability tests."""

    def test_rapid_fire_50_messages(self, device_b, iphone_api, device_a_num, timestamp_marker):
        """50 messages in rapid succession from B → A, measure delivery rate."""
        sent_count = 50
        for i in range(sent_count):
            text = f"rapid_{i:03d}_{int(time.time())}"
            device_b.send_text(text, dest=device_a_num)
            time.sleep(0.5)  # Minimum spacing for LoRa

        # Wait for propagation
        time.sleep(60)

        events = iphone_api.telemetry(since=timestamp_marker)
        received = [e for e in events if e.get("type") == "messageReceived"]
        delivery_rate = len(received) / sent_count * 100

        # We expect at least 80% delivery rate
        assert delivery_rate >= 80, \
            f"Delivery rate too low: {delivery_rate:.1f}% ({len(received)}/{sent_count})"

    def test_concurrent_dm_and_group(self, device_b, iphone_api, device_a_num, timestamp_marker):
        """Concurrent DM + group message streams."""
        groups = iphone_api.groups()
        if not groups:
            pytest.skip("No groups configured")
        group_id = groups[0].get("groupId", 0)

        # Send 10 DMs and 10 group messages interleaved
        for i in range(10):
            # DM from B
            device_b.send_text(f"concurrent_dm_{i}", dest=device_a_num)
            time.sleep(1)
            # Group message from iPhone
            iphone_api.send_group_message(group_id=group_id, text=f"concurrent_group_{i}")
            time.sleep(2)

        # Wait and verify both streams delivered
        time.sleep(30)
        events = iphone_api.telemetry(since=timestamp_marker)

        dm_received = [e for e in events if e.get("type") == "messageReceived"]
        group_sent = [e for e in events if e.get("type") == "groupMessageSent"]

        assert len(dm_received) >= 7, f"DM delivery too low: {len(dm_received)}/10"
        assert len(group_sent) >= 10, f"Group sends missing: {len(group_sent)}/10"

    def test_voice_memo_during_text(self, device_b, iphone_api, device_a_num, timestamp_marker):
        """Voice memo recording while text messages are flowing."""
        # Start text message stream in background
        def send_texts():
            for i in range(10):
                device_b.send_text(f"bg_text_{i}", dest=device_a_num)
                time.sleep(2)

        text_thread = threading.Thread(target=send_texts, daemon=True)
        text_thread.start()

        # Trigger voice memo
        time.sleep(3)
        iphone_api.record_voice_memo()

        # Wait for everything to settle
        text_thread.join(timeout=30)
        time.sleep(15)

        events = iphone_api.telemetry(since=timestamp_marker)
        text_received = [e for e in events if e.get("type") == "messageReceived"]
        voice_events = [e for e in events if e.get("type") in ("voiceMemoRecordStart", "voiceMemoSent")]

        # Text messages should still deliver during voice memo
        assert len(text_received) >= 5, f"Text delivery degraded during voice: {len(text_received)}/10"
        # Voice memo should have at least started
        assert len(voice_events) >= 1, "Voice memo events missing"

    @pytest.mark.slow
    def test_sustained_10_minute(self, device_b, iphone_api, device_a_num, timestamp_marker):
        """10-minute sustained operation: periodic messages, monitor for drops."""
        duration = 600  # 10 minutes
        interval = 15   # Send every 15 seconds
        start = time.time()
        sent = 0

        while time.time() - start < duration:
            text = f"sustained_{sent:04d}"
            device_b.send_text(text, dest=device_a_num)
            sent += 1
            time.sleep(interval)

        # Final wait
        time.sleep(30)

        events = iphone_api.telemetry(since=timestamp_marker)
        received = [e for e in events if e.get("type") == "messageReceived"]
        delivery_rate = len(received) / sent * 100 if sent > 0 else 0

        assert delivery_rate >= 90, \
            f"Sustained delivery rate: {delivery_rate:.1f}% ({len(received)}/{sent})"

    def test_latency_measurement(self, device_b, iphone_api, device_a_num, timestamp_marker):
        """Measure average message latency from send to telemetry receipt."""
        latencies = []

        for i in range(5):
            send_time = time.time()
            text = f"latency_{i}_{int(send_time)}"
            device_b.send_text(text, dest=device_a_num)

            try:
                event = iphone_api.wait_for_event("messageReceived", timeout=30, since=timestamp_marker)
                receive_time = time.time()
                latency = receive_time - send_time
                latencies.append(latency)
            except TimeoutError:
                pass

            time.sleep(5)

        if latencies:
            avg_latency = sum(latencies) / len(latencies)
            max_latency = max(latencies)
            # Average latency should be under 15 seconds for LoRa LONG_FAST
            assert avg_latency < 15, f"Average latency too high: {avg_latency:.1f}s"
            assert max_latency < 30, f"Max latency too high: {max_latency:.1f}s"
        else:
            pytest.fail("No messages received for latency measurement")
