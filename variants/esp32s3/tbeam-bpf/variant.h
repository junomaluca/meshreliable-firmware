// LilyGo T-Beam BPF (ESP32-S3 + SX1278 @ 144-148 MHz VHF)
// Single I2C bus shared by PMU (AXP2101) and display (SH1106 OLED)

// Hardware has 2M support only (144-148 MHz amateur band)
#define HAS_HAM_2M 1
#define HAS_HAM_2M_ONLY 1

// Display - SH1106 OLED (128x64)
#define USE_SH1106
#define OLED_WIDTH 128
#define OLED_HEIGHT 64

// I2C
#define I2C_SDA 8
#define I2C_SCL 9

// Buttons
#define BUTTON_PIN 0     // Boot button
#define BUTTON_PIN_SECONDARY 3 // User button

// PMU - AXP2101
#define HAS_AXP2101
#define PMU_IRQ 4

// GPS - Quectel L76K (UART)
#define HAS_GPS 1
#define GPS_RX_PIN 5
#define GPS_TX_PIN 6
#define GPS_1PPS_PIN 7
#define GPS_BAUDRATE 9600

// LoRa SX1278 (RF95 only — no SX1262/SX1268/LR1121)
#define USE_RF95

#define LORA_SCK 12
#define LORA_MISO 13
#define LORA_MOSI 11
#define LORA_CS 1
#define LORA_RESET 18
#define LORA_DIO0 14  // SX1278 DIO0 (RxDone/TxDone IRQ)
#define LORA_DIO1 21  // SX1278 DIO1
#define LORA_DIO2 -1  // Not connected

// Explicit RF95 pin mapping
#define RF95_IRQ LORA_DIO0
#define RF95_DIO1 LORA_DIO1
#define RF95_RESET LORA_RESET

// LoRa power control
#define LORA_EN 16    // LDO enable for LoRa module (P-MOSFET gate)
#define BPF_CTRL 39   // BPF control / LNA enable
#define RF95_RXEN 39  // LNA enable — HIGH during RX, LOW during TX (same pin as BPF_CTRL)

// SX1278 rated to 20 dBm; previous 10 dBm cap was overly conservative
#define RF95_MAX_POWER 20

// SD card on shared SPI bus (same pins as LoRa)
#define HAS_SDCARD
#define SDCARD_CS 10
#define SPI_SCK 12
#define SPI_MISO 13
#define SPI_MOSI 11

// 32768 Hz crystal present
#define HAS_32768HZ 1
