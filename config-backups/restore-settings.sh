#!/bin/bash
#
# Restore all default MeshReliable settings to a device via Meshtastic CLI.
# Usage: ./restore-settings.sh [--port /dev/cu.usbmodemXXXX]
#
# After firmware flash or factory reset, run this to apply all config.
#

set -e

PORT_ARG=""
if [ "$1" = "--port" ] && [ -n "$2" ]; then
    PORT_ARG="--port $2"
fi

echo "=== MeshReliable Settings Restore ==="
echo ""

# LoRa Radio
echo ">> Configuring LoRa radio..."
meshtastic $PORT_ARG \
    --set lora.region US \
    --set lora.modem_preset LONG_FAST \
    --set lora.ok_to_mqtt true \
    --set lora.tx_enabled true \
    --set lora.hop_limit 7 \
    --set lora.channel_num 20

sleep 2

# Channel 0 — maluca (Primary)
echo ">> Configuring Channel 0 (maluca)..."
meshtastic $PORT_ARG \
    --ch-index 0 \
    --ch-set name maluca \
    --ch-set psk "QnMwZnZaUlJkQmhnSTg3ZGlnckhBYzMyU3FVOXpRcm4=" \
    --ch-set uplink_enabled true \
    --ch-set downlink_enabled true \
    --ch-set module_settings.position_precision 32

sleep 2

# Channel 1 — LongFast (Secondary)
echo ">> Configuring Channel 1 (LongFast)..."
meshtastic $PORT_ARG \
    --ch-index 1 \
    --ch-set name LongFast \
    --ch-set psk "AQW==" \
    --ch-set uplink_enabled true \
    --ch-set downlink_enabled true \
    --ch-set module_settings.position_precision 0

sleep 2

# Bluetooth
echo ">> Configuring Bluetooth..."
meshtastic $PORT_ARG \
    --set bluetooth.enabled true

sleep 1

# Time Zone
echo ">> Setting time zone..."
meshtastic $PORT_ARG \
    --set device.tzdef "PST8PDT,M3.2.0/2:00:00,M11.1.0/2:00:00"

sleep 1

# Position
echo ">> Configuring position..."
meshtastic $PORT_ARG \
    --set position.position_broadcast_secs 3600 \
    --set position.position_flags 1

sleep 1

# Canned Messages
echo ">> Configuring canned messages..."
meshtastic $PORT_ARG \
    --set canned_message.enabled true \
    --set canned_message.send_bell true \
    --set canned_message.messages "Are you ok?|I am ok|I am lost|I need help|I want to leave|Where are you?|Meet at the meeting point|I need to drink/eat|Yes|No|OK"

sleep 1

# External Notifications
echo ">> Configuring external notifications..."
meshtastic $PORT_ARG \
    --set external_notification.enabled true \
    --set external_notification.alert_bell true \
    --set external_notification.alert_message true \
    --set external_notification.nag_timeout 1

sleep 1

# MQTT
echo ">> Configuring MQTT..."
meshtastic $PORT_ARG \
    --set mqtt.enabled true \
    --set mqtt.proxy_to_client_enabled true \
    --set mqtt.encryption_enabled true \
    --set mqtt.map_reporting_enabled true \
    --set mqtt.map_report_settings.publish_interval_secs 3600 \
    --set mqtt.root "msh/US" \
    --set mqtt.address "home.yazdikann.com:1883" \
    --set mqtt.username "admin" \
    --set mqtt.password "admin" \
    --set mqtt.tls_enabled false

sleep 1

# Store and Forward
echo ">> Configuring Store and Forward..."
meshtastic $PORT_ARG \
    --set store_forward.enabled true \
    --set store_forward.heartbeat true \
    --set store_forward.records 100 \
    --set store_forward.history_return_max 100 \
    --set store_forward.history_return_window 72000

sleep 1

echo ""
echo "=== Settings restored successfully ==="
echo "Device will reboot to apply changes."
