# MeshReliable Default Device Settings (All Nodes)

After any firmware flash or factory reset, these values must be restored.

## LoRa Radio
- Region: United States (915 MHz)
- Modem preset: Long Fast
- OK to MQTT: enabled
- LoRa transmit: enabled
- Number of hops: 7
- Frequency slot: 20

## Channel 0 — maluca (Primary)
- Channel name: `maluca`
- Encryption key: `QnMwZnZaUlJkQmhnSTg3ZGlnckhBYzMyU3FVOXpRcm4=` (256-bit)
- Channel role: primary
- Position enabled: yes
- Precise location: enabled
- MQTT uplink: enabled
- MQTT downlink: enabled

## Channel 1 — LongFast (Secondary)
- Channel name: `LongFast`
- Encryption key: `AQW==` (default / 8-bit)
- Channel role: secondary
- Allow position requests: no
- MQTT uplink: enabled
- MQTT downlink: enabled
- Alerts/notifications: **muted**

## Bluetooth
- Bluetooth: enabled

## Time Zone
- Time zone: US Pacific (California)
- POSIX TZ string: `PST8PDT,M3.2.0/2:00:00,M11.1.0/2:00:00`

## Position
- Position broadcast interval: 3600 seconds (1 hour)
- Position flags — Timestamp: on
- Position flags — All others (altitude, heading, speed, etc.): off

## Canned Messages
- Message list: `Are you ok?|I am ok|I am lost|I need help|I want to leave|Where are you?|Meet at the meeting point|I need to drink/eat|Yes|No|OK`
- Send bell with canned message: enabled

## External Notifications
- External notifications: enabled
- Alert on bell: enabled
- Alert on message: enabled
- Nag timeout: 1 second

## MQTT
- MQTT: enabled
- MQTT client proxy: enabled
- Encryption: enabled
- Map reporting: enabled
- Consent to publish map data: enabled
- Map publish interval: 3600 seconds (1 hour)
- Root topic: `msh/US`
- MQTT server address: `home.yazdikann.com:1883`
- MQTT username: `admin`
- MQTT password: `admin`
- TLS: disabled

## Store and Forward
- Store and Forward module: enabled
- Heartbeat: enabled
- Records: 100
- Max history return: 100
- History return window: 72000 seconds (20 hours)
