#ifndef Pins_Arduino_h
#define Pins_Arduino_h

#include <stdint.h>

#define USB_VID 0x303a
#define USB_PID 0x1001

static const uint8_t TX = 43;
static const uint8_t RX = 44;

// Single I2C bus shared by PMU (AXP2101) and display (SSD1306)
static const uint8_t SDA = 8;
static const uint8_t SCL = 9;

// Default SPI mapped to LoRa radio (shared with SD card)
static const uint8_t SS = 1;
static const uint8_t MOSI = 11;
static const uint8_t MISO = 13;
static const uint8_t SCK = 12;

#endif /* Pins_Arduino_h */
