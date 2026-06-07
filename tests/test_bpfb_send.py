#!/usr/bin/env python3
"""Quick test: can BPF-B send when it's the only open interface?"""
import time
import meshtastic.serial_interface

BPFB_PORT = '/dev/cu.usbmodem21201'
VHFA_ID = 0x335e1be8

def test_single():
    """Open only BPF-B, try to send."""
    print("Opening BPF-B as sole interface...")
    try:
        iface = meshtastic.serial_interface.SerialInterface(BPFB_PORT)
        time.sleep(3)
        node = iface.getMyNodeInfo()
        print(f"  Connected: 0x{node.get('num', 0):08x}")

        # Try sending text
        print("  Sending text to VHF-A...")
        iface.sendText("BPF-B solo test", destinationId=VHFA_ID, wantAck=True)
        print("  -> Send succeeded!")
        time.sleep(5)

        # Try sending data (media transfer portnum)
        print("  Sending data to VHF-A...")
        iface.sendData(b'\x08\x01\x10\x01', destinationId=VHFA_ID, portNum=259,
                       wantAck=False, wantResponse=False)
        print("  -> Data send succeeded!")
        time.sleep(5)

        iface.close()
        print("Done - BPF-B can send when solo!")
        return True
    except Exception as e:
        print(f"  FAILED: {e}")
        try: iface.close()
        except: pass
        return False

if __name__ == "__main__":
    # Wait to make sure the stress test isn't holding the port
    print("NOTE: Make sure no other process has the BPF-B port open!")
    print(f"Port: {BPFB_PORT}")
    test_single()
