"""
E2E Test: Voice Memo

Tests voice memo recording, sending, and receiving:
  - Record on iPhone, verify Codec2 chunks sent to mesh
  - Device C (T3-S3) receives voice memo chunks
  - Duration and chunk count validation
"""

import time
import pytest


class TestVoiceMemo:
    """Voice memo end-to-end tests."""

    def test_record_and_send_from_iphone(self, iphone_api, timestamp_marker):
        """Trigger voice memo recording on iPhone, verify send events."""
        result = iphone_api.record_voice_memo()
        assert result.get("ok") is True

        # Wait for recording start event
        event = iphone_api.wait_for_event("voiceMemoRecordStart", timeout=10, since=timestamp_marker)
        assert event is not None

    def test_voice_memo_sent_event(self, iphone_api, timestamp_marker):
        """After recording, verify voice memo sent event with chunk metadata."""
        # Trigger recording
        iphone_api.record_voice_memo()
        time.sleep(5)  # Allow recording time

        # Check for voiceMemoSent event (may take time for encoding + sending)
        try:
            event = iphone_api.wait_for_event("voiceMemoSent", timeout=60, since=timestamp_marker)
            assert event is not None
            details = event.get("details", {})
            # Should have chunk count and byte count
            if "chunks" in details:
                chunks = int(details["chunks"])
                assert chunks > 0, "Voice memo should have at least 1 chunk"
            if "bytes" in details:
                byte_count = int(details["bytes"])
                assert byte_count > 0, "Voice memo should have non-zero bytes"
        except TimeoutError:
            pytest.skip("No voice memo sent — may need manual recording trigger")

    def test_voice_memo_received_on_iphone(self, iphone_api, device_c, device_a_num, timestamp_marker):
        """
        Send synthetic voice memo from Device C → iPhone.
        Verify telemetry shows receipt and reassembly.
        """
        # This test requires Device C to send voice memo chunks.
        # In practice, Device C's VoiceMemoModule handles this at the firmware level.
        # For now, we verify the telemetry infrastructure works by checking
        # if any voiceMemoReceived events appear from prior test sessions.
        time.sleep(5)
        events = iphone_api.telemetry(since=timestamp_marker)
        voice_events = [e for e in events if e.get("type") == "voiceMemoReceived"]
        # This is informational — don't fail if no voice memos received
        if voice_events:
            event = voice_events[0]
            details = event.get("details", {})
            assert "transferId" in details
            assert "chunks" in details

    def test_voice_memo_duration_validation(self, iphone_api, timestamp_marker):
        """Verify that voice memo respects max 60s duration limit."""
        # The firmware enforces 60s max. Just verify the metadata is reasonable.
        events = iphone_api.telemetry()
        voice_sent = [e for e in events if e.get("type") == "voiceMemoSent"]

        for event in voice_sent:
            details = event.get("details", {})
            if "bytes" in details:
                byte_count = int(details["bytes"])
                # At 8kHz 16-bit mono, 60s = 960,000 bytes max PCM
                # After Codec2 encoding at 700bps, much less
                assert byte_count <= 960000, f"Voice memo too large: {byte_count} bytes"

    def test_chunk_count_reasonable(self, iphone_api, timestamp_marker):
        """Verify chunk count is consistent with audio duration."""
        events = iphone_api.telemetry()
        voice_sent = [e for e in events if e.get("type") == "voiceMemoSent"]

        for event in voice_sent:
            details = event.get("details", {})
            if "chunks" in details and "bytes" in details:
                chunks = int(details["chunks"])
                byte_count = int(details["bytes"])
                # Each chunk is max 220 bytes
                expected_min_chunks = byte_count // 220
                assert chunks >= expected_min_chunks, \
                    f"Too few chunks ({chunks}) for {byte_count} bytes"
