#include "configuration.h"

#ifdef LILYGO_TBEAM_BPF

#include "Arduino.h"

void earlyInitVariant()
{
    // Power on LoRa module LDO
    pinMode(LORA_EN, OUTPUT);
    digitalWrite(LORA_EN, HIGH);

    // Enable BPF control
    pinMode(BPF_CTRL, OUTPUT);
    digitalWrite(BPF_CTRL, HIGH);

    // Deselect SPI devices to avoid bus contention
    pinMode(LORA_CS, OUTPUT);
    digitalWrite(LORA_CS, HIGH);
    pinMode(SDCARD_CS, OUTPUT);
    digitalWrite(SDCARD_CS, HIGH);

    // Allow LDO to stabilize before SPI probing
    delay(100);
}

#endif
