// LilyGo T-LoRa Pager — MeshReliable variant
// Hardware: ESP32-S3 + ST7796 TFT LCD (480x222) + LR1121/SX1262 LoRa + XL9555 GPIO expander
// Pin mapping: https://github.com/Xinyuan-LilyGO/LilyGoLib/blob/master/docs/hardware/lilygo-t-lora-pager.md

// ST7796 TFT LCD (2.33" IPS, 480x222, SPI shared with LoRa)
#define TFT_CS 38
#define ST7796_CS TFT_CS
#define ST7796_RS 37    // DC
#define ST7796_SDA MOSI // MOSI (GPIO 34)
#define ST7796_SCK SCK  // GPIO 35
#define ST7796_RESET -1
#define ST7796_MISO MISO // GPIO 33
#define ST7796_BUSY -1
#define ST7796_BL 42
#define ST7796_SPI_HOST SPI2_HOST
#define TFT_BL 42
#define SPI_FREQUENCY 75000000
#define SPI_READ_FREQUENCY 16000000
#define TFT_HEIGHT 480
#define TFT_WIDTH 222
#define TFT_OFFSET_X 49
#define TFT_OFFSET_Y 0
#define TFT_OFFSET_ROTATION 3
#define SCREEN_ROTATE
#define SCREEN_TRANSITION_FRAMERATE 30
#define BRIGHTNESS_DEFAULT 130
#define USE_TFTDISPLAY 1

// I2C bus (shared: ES8311, XL9555, BHI260AP, PCF85063A, BQ25896, BQ27220, TCA8418, DRV2605)
#define I2C_SDA SDA // GPIO 3
#define I2C_SCL SCL // GPIO 2

// Keyboard (TCA8418)
#define HAS_PHYSICAL_KEYBOARD 1
#define I2C_NO_RESCAN
#define KB_BL_PIN 46
#define KB_INT 6

// Rotary encoder
#define ROTARY_A (40)
#define ROTARY_B (41)
#define ROTARY_PRESS (7)

// Haptic motor driver
#define HAS_DRV2605 1

// Power management
#define USE_POWERSAVE
#define SLEEP_TIME 120

#define BATTERY_PIN 1
#define ADC_MULTIPLIER 2.11
#define ADC_CHANNEL ADC_CHANNEL_0

// Battery charger BQ25896
#define HAS_PPM 1
#define XPOWERS_CHIP_BQ25896

// Battery gauge BQ27220
#define HAS_BQ27220 1
#define BQ27220_I2C_SDA SDA
#define BQ27220_I2C_SCL SCL
#define BQ27220_DESIGN_CAPACITY 1500

#define BUTTON_PIN 0

// GPS (UBlox MIA-M10Q)
#define HAS_GPS 1
#define GPS_BAUDRATE 38400
#define GPS_RX_PIN 4
#define GPS_TX_PIN 12
#define PIN_GPS_PPS 13

// RTC (PCF85063A)
#define PCF85063_RTC 0x51

// SD card (shared SPI bus)
#define HAS_SDCARD
#define SDCARD_USE_SPI1
#define SPI_MOSI MOSI
#define SPI_SCK SCK
#define SPI_MISO MISO
#define SPI_CS 21
#define SDCARD_CS SPI_CS
#define SD_SPI_FREQUENCY 75000000U

// Audio codec ES8311 (I2S)
#define HAS_I2S
#define DAC_I2S_BCK 11
#define DAC_I2S_WS 18
#define DAC_I2S_DOUT 45
#define DAC_I2S_DIN 17
#define DAC_I2S_MCLK 10

// Gyroscope BHI260AP
#define HAS_BHI260AP

// NFC ST25R3916
#define NFC_INT 5
#define NFC_CS 39

// External expansion chip XL9555
#define USE_XL9555
#define EXPANDS_DRV_EN (0)
#define EXPANDS_AMP_EN (1)
#define EXPANDS_KB_RST (2)
#define EXPANDS_LORA_EN (3)
#define EXPANDS_GPS_EN (4)
#define EXPANDS_NFC_EN (5)
#define EXPANDS_GPS_RST (7)
#define EXPANDS_KB_EN (8)
#define EXPANDS_GPIO_EN (9)
#define EXPANDS_SD_DET (10)
#define EXPANDS_SD_PULLEN (11)
#define EXPANDS_SD_EN (12)

// LoRa — LR1121 radio (skip SX126x probes to prevent misidentification)
#define SKIP_SX126X_PROBE
#define USE_SX1262
#define USE_SX1268
#define USE_LR1121

// Disable 2.4 GHz multi-band retry — the LR1121's band switch from 2.4 GHz back to sub-GHz
// fails with SPI error -707, leaving the radio permanently stuck and unable to receive.
#define DISABLE_WIDELORA_MULTIBAND

// Pager LoRa SPI pins
#define LORA_SCK 35
#define LORA_MISO 33
#define LORA_MOSI 34
#define LORA_CS 36
#define LORA_RESET 47

#define LORA_DIO0 -1
#define LORA_DIO1 14   // SX1262 IRQ / LR1121 IRQ
#define LORA_DIO2 48   // SX1262 BUSY / LR1121 BUSY

// SX126X pin aliases
#define SX126X_CS LORA_CS
#define SX126X_DIO1 LORA_DIO1
#define SX126X_BUSY LORA_DIO2
#define SX126X_RESET LORA_RESET
#define SX126X_DIO2_AS_RF_SWITCH
#define SX126X_DIO3_TCXO_VOLTAGE 3.0

// LR1121 pin aliases
#define LR1121_IRQ_PIN LORA_DIO1
#define LR1121_NRESET_PIN LORA_RESET
#define LR1121_BUSY_PIN LORA_DIO2
#define LR1121_SPI_NSS_PIN LORA_CS
#define LR1121_SPI_SCK_PIN LORA_SCK
#define LR1121_SPI_MOSI_PIN LORA_MOSI
#define LR1121_SPI_MISO_PIN LORA_MISO
#define LR11X0_DIO3_TCXO_VOLTAGE 3.0
#define LR11X0_DIO_AS_RF_SWITCH
