#ifndef Pins_Arduino_h
#define Pins_Arduino_h

#include <stdint.h>

// LilyGo T-LoRa Pager pin definitions
// Source: https://github.com/espressif/arduino-esp32/blob/master/variants/lilygo_tlora_pager/pins_arduino.h

#define USB_VID 0x303a
#define USB_PID 0x82D4
#define USB_MANUFACTURER "LILYGO"
#define USB_PRODUCT "T-LoRa-Pager"

// UART
static const uint8_t TX = 43;
static const uint8_t RX = 44;

// I2C
static const uint8_t SDA = 3;
static const uint8_t SCL = 2;

// SPI (shared: LoRa, display, SD card, NFC)
static const uint8_t SS = 36;
static const uint8_t MOSI = 34;
static const uint8_t MISO = 33;
static const uint8_t SCK = 35;

#endif /* Pins_Arduino_h */
