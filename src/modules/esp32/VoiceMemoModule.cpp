#include "configuration.h"
#ifdef ARCH_ESP32

#include "VoiceMemoModule.h"
#include "modules/MediaTransferModule.h"
#include "MeshService.h"
#include "MessageStore.h"
#include "NodeDB.h"
#include "gps/RTC.h"

#ifdef HAS_VOICE_MEMO
#include <driver/i2s.h>
#include "input/ButtonThread.h"

#ifdef VOICE_MEMO_ES8311
// ES8311 codec (T-LoRa Pager): single I2S port through codec for both mic and speaker
#include "AudioBoard.h"
extern AudioBoard board;
#ifdef USE_XL9555
#include "ExtensionIOXL9555.hpp"
extern ExtensionIOXL9555 io;
#endif
#define ES8311_I2S_PORT I2S_NUM_0
#else
// MVSR hardware (T3-S3 V1): separate MEMS mic (I2S0) + MAX98357A amp (I2S1)
#ifndef MVSR_MIC_I2S_PORT
#define MVSR_MIC_I2S_PORT I2S_NUM_0
#endif
#ifndef MVSR_SPK_I2S_PORT
#define MVSR_SPK_I2S_PORT I2S_NUM_1
#endif
#endif // VOICE_MEMO_ES8311

#endif // HAS_VOICE_MEMO

// !! CRITICAL — DO NOT CHANGE THIS BITRATE !!
// Voice memos MUST be compressed to Codec2 1300 bps before LoRa transmission.
// This reduces a 5-second memo from ~80KB (raw PCM) to ~812 bytes (Codec2).
// Without compression, voice memos would take minutes to transmit over LoRa.
// Previously regressed — this is a hard requirement. See also PhoneVoiceUploadModule.cpp.
// Codec2 1300 mode: 52 bits/frame (7 bytes), 320 samples/frame, 40ms frames
#define VOICE_MEMO_CODEC2_MODE CODEC2_MODE_1300

VoiceMemoModule *voiceMemoModule = nullptr;

// Forward declaration — defined below, called from MediaTransferModule completion callback
static void forwardImageToPhone(const uint8_t *imageData, uint32_t size, uint32_t fromNode, uint32_t transferId);

VoiceMemoModule::VoiceMemoModule()
    : concurrency::OSThread("VoiceMemo")
{
    // Codec2 initialization is deferred until first use (startRecording, playVoiceMemo,
    // or generateAndSendTestMemo). Eager init at boot consumes ~30KB of internal SRAM
    // which starves NimBLE's connection buffer pool and causes BLE crashes.
    LOG_INFO("VoiceMemo: module created (codec2 init deferred)");

    // Register with MediaTransferModule for transfer completion callbacks
    if (mediaTransferModule) {
        mediaTransferModule->setTransferCompleteCallback(
            [](const uint8_t *data, uint32_t size, meshtastic_MediaContentType type,
               uint32_t fromNode, uint32_t transferId) {
                if (type == meshtastic_MediaContentType_VOICE_MEMO && voiceMemoModule) {
                    voiceMemoModule->onTransferComplete(data, size, fromNode, transferId);
                }
                else if (type == meshtastic_MediaContentType_IMAGE_THUMBNAIL ||
                         type == meshtastic_MediaContentType_IMAGE_LOWRES) {
#if HAS_SCREEN
                    // Create a StoredMessage for received pictures so they show in the chat
                    StoredMessage sm;
                    uint32_t nowSecs = getValidTime(RTCQuality::RTCQualityDevice, false);
                    if (nowSecs) {
                        sm.timestamp = nowSecs;
                        sm.isBootRelative = false;
                    } else {
                        sm.timestamp = millis() / 1000;
                        sm.isBootRelative = true;
                    }
                    sm.sender = fromNode;
                    sm.channelIndex = 0;
                    sm.dest = myNodeInfo.my_node_num;
                    sm.type = MessageType::DM_TO_US;
                    sm.isPicture = true;
                    sm.textOffset = MessageStore::storeText("[Picture]", 9);
                    sm.textLength = 9;
                    messageStore.addLiveMessage(std::move(sm));
#endif
                    LOG_INFO("MediaTransfer: Received image from 0x%08x (%u bytes), forwarding to phone", fromNode, size);

                    // Forward image data to connected phone via PRIVATE_APP binary headers.
                    // Uses IMAGE_START/CHUNK/END header types (0x04/0x05/0x06) so the iOS app
                    // can reassemble and persist the image. If phone is disconnected,
                    // PhoneBufferModule will buffer the packets for replay on reconnect.
                    forwardImageToPhone(data, size, fromNode, transferId);
                }
            });
    }
}

VoiceMemoModule::~VoiceMemoModule()
{
    deinitCodec2();
#ifdef HAS_VOICE_MEMO
    deinitMic();
    deinitSpeaker();
#endif
}

void VoiceMemoModule::initCodec2()
{
    if (codec2) return;

    codec2 = codec2_create(VOICE_MEMO_CODEC2_MODE);
    if (!codec2) {
        LOG_ERROR("VoiceMemo: Failed to create Codec2");
        return;
    }

    codec2_set_lpc_post_filter(codec2, 1, 0, 0.8, 0.2);
    codecBytesPerFrame = (codec2_bits_per_frame(codec2) + 7) / 8;
    samplesPerFrame = codec2_samples_per_frame(codec2);
    pcmBuffer = new int16_t[samplesPerFrame];

    LOG_INFO("VoiceMemo: Codec2 mode %d — %d bytes/frame, %d samples/frame",
             VOICE_MEMO_CODEC2_MODE, codecBytesPerFrame, samplesPerFrame);
}

void VoiceMemoModule::deinitCodec2()
{
    if (codec2) {
        codec2_destroy(codec2);
        codec2 = nullptr;
    }
    delete[] pcmBuffer;
    pcmBuffer = nullptr;
}

// --- I2S Mic / Speaker ---

#ifdef HAS_VOICE_MEMO

#ifdef VOICE_MEMO_ES8311
// ============================================================
// ES8311 codec backend (T-LoRa Pager)
// Single I2S port through ES8311 for both mic input and speaker output.
// Must release AudioThread's I2S_NUM_1 first since it shares the same pins.
// ============================================================

void VoiceMemoModule::initMic()
{
    if (micInitialized) return;

    // Release AudioThread's I2S to free shared BCK/WS/MCLK pins
    i2s_driver_uninstall(I2S_NUM_1);

    // Reconfigure ES8311 codec for 8kHz mono recording via I2C
    CodecConfig cfg;
    cfg.input_device = ADC_INPUT_LINE1;
    cfg.output_device = DAC_OUTPUT_ALL;
    cfg.i2s.bits = BIT_LENGTH_16BITS;
    cfg.i2s.rate = RATE_8K;
    board.setConfig(cfg);

    // Install legacy I2S in RX mode for mic capture
    i2s_config_t i2sCfg = {};
    i2sCfg.mode = (i2s_mode_t)(I2S_MODE_MASTER | I2S_MODE_RX);
    i2sCfg.sample_rate = 8000;
    i2sCfg.bits_per_sample = I2S_BITS_PER_SAMPLE_16BIT;
    i2sCfg.channel_format = I2S_CHANNEL_FMT_ONLY_LEFT;
    i2sCfg.communication_format = I2S_COMM_FORMAT_STAND_I2S;
    i2sCfg.intr_alloc_flags = 0;
    i2sCfg.dma_buf_count = 8;
    i2sCfg.dma_buf_len = samplesPerFrame;
    i2sCfg.use_apll = false;

    esp_err_t err = i2s_driver_install(ES8311_I2S_PORT, &i2sCfg, 0, NULL);
    if (err != ESP_OK) {
        LOG_ERROR("VoiceMemo: ES8311 I2S mic install failed: %d", err);
        return;
    }

    i2s_pin_config_t pins = {};
    pins.mck_io_num = DAC_I2S_MCLK;
    pins.bck_io_num = DAC_I2S_BCK;
    pins.ws_io_num = DAC_I2S_WS;
    pins.data_out_num = I2S_PIN_NO_CHANGE;
    pins.data_in_num = DAC_I2S_DIN;

    err = i2s_set_pin(ES8311_I2S_PORT, &pins);
    if (err != ESP_OK) {
        LOG_ERROR("VoiceMemo: ES8311 mic pin config failed: %d", err);
        i2s_driver_uninstall(ES8311_I2S_PORT);
        return;
    }

    i2s_start(ES8311_I2S_PORT);
    micInitialized = true;
    LOG_INFO("VoiceMemo: ES8311 mic initialized (BCK=%d WS=%d DIN=%d MCLK=%d)",
             DAC_I2S_BCK, DAC_I2S_WS, DAC_I2S_DIN, DAC_I2S_MCLK);
}

void VoiceMemoModule::deinitMic()
{
    if (!micInitialized) return;
    i2s_stop(ES8311_I2S_PORT);
    i2s_driver_uninstall(ES8311_I2S_PORT);
    micInitialized = false;

    // Restore ES8311 to normal 44.1kHz output mode for ringtones/TTS
    CodecConfig cfg;
    cfg.input_device = ADC_INPUT_LINE1;
    cfg.output_device = DAC_OUTPUT_ALL;
    cfg.i2s.bits = BIT_LENGTH_16BITS;
    cfg.i2s.rate = RATE_44K;
    board.setConfig(cfg);
    board.setVolume(75);

    LOG_INFO("VoiceMemo: ES8311 mic deinitialized, codec restored to 44.1kHz");
}

void VoiceMemoModule::initSpeaker()
{
    if (spkInitialized) return;

    // Release AudioThread's I2S to free shared BCK/WS/MCLK pins
    i2s_driver_uninstall(I2S_NUM_1);

    // Enable amplifier via XL9555 GPIO expander
#ifdef USE_XL9555
    io.digitalWrite(EXPANDS_AMP_EN, HIGH);
#endif
    delay(10);

    // Reconfigure ES8311 codec for 8kHz playback
    CodecConfig cfg;
    cfg.input_device = ADC_INPUT_LINE1;
    cfg.output_device = DAC_OUTPUT_ALL;
    cfg.i2s.bits = BIT_LENGTH_16BITS;
    cfg.i2s.rate = RATE_8K;
    board.setConfig(cfg);
    board.setVolume(90);

    // Install legacy I2S in TX mode for speaker output
    i2s_config_t i2sCfg = {};
    i2sCfg.mode = (i2s_mode_t)(I2S_MODE_MASTER | I2S_MODE_TX);
    i2sCfg.sample_rate = 8000;
    i2sCfg.bits_per_sample = I2S_BITS_PER_SAMPLE_16BIT;
    i2sCfg.channel_format = I2S_CHANNEL_FMT_ONLY_LEFT;
    i2sCfg.communication_format = I2S_COMM_FORMAT_STAND_I2S;
    i2sCfg.intr_alloc_flags = 0;
    i2sCfg.dma_buf_count = 8;
    i2sCfg.dma_buf_len = samplesPerFrame;
    i2sCfg.use_apll = false;
    i2sCfg.tx_desc_auto_clear = true;

    esp_err_t err = i2s_driver_install(ES8311_I2S_PORT, &i2sCfg, 0, NULL);
    if (err != ESP_OK) {
        LOG_ERROR("VoiceMemo: ES8311 speaker I2S install failed: %d", err);
#ifdef USE_XL9555
        io.digitalWrite(EXPANDS_AMP_EN, LOW);
#endif
        return;
    }

    i2s_pin_config_t pins = {};
    pins.mck_io_num = DAC_I2S_MCLK;
    pins.bck_io_num = DAC_I2S_BCK;
    pins.ws_io_num = DAC_I2S_WS;
    pins.data_out_num = DAC_I2S_DOUT;
    pins.data_in_num = I2S_PIN_NO_CHANGE;

    err = i2s_set_pin(ES8311_I2S_PORT, &pins);
    if (err != ESP_OK) {
        LOG_ERROR("VoiceMemo: ES8311 speaker pin config failed: %d", err);
        i2s_driver_uninstall(ES8311_I2S_PORT);
#ifdef USE_XL9555
        io.digitalWrite(EXPANDS_AMP_EN, LOW);
#endif
        return;
    }

    i2s_start(ES8311_I2S_PORT);
    spkInitialized = true;
    LOG_INFO("VoiceMemo: ES8311 speaker initialized (BCK=%d WS=%d DOUT=%d MCLK=%d)",
             DAC_I2S_BCK, DAC_I2S_WS, DAC_I2S_DOUT, DAC_I2S_MCLK);
}

void VoiceMemoModule::deinitSpeaker()
{
    if (!spkInitialized) return;
    i2s_stop(ES8311_I2S_PORT);
    i2s_driver_uninstall(ES8311_I2S_PORT);
    spkInitialized = false;

    // Disable amplifier
#ifdef USE_XL9555
    io.digitalWrite(EXPANDS_AMP_EN, LOW);
#endif

    // Restore ES8311 to normal 44.1kHz mode for ringtones/TTS
    CodecConfig cfg;
    cfg.input_device = ADC_INPUT_LINE1;
    cfg.output_device = DAC_OUTPUT_ALL;
    cfg.i2s.bits = BIT_LENGTH_16BITS;
    cfg.i2s.rate = RATE_44K;
    board.setConfig(cfg);
    board.setVolume(75);

    LOG_INFO("VoiceMemo: ES8311 speaker deinitialized, codec restored to 44.1kHz");
}

bool VoiceMemoModule::captureAndEncodeFrame()
{
    if (!micInitialized || !codec2 || !pcmBuffer) return false;

    size_t bytesRead = 0;
    esp_err_t err = i2s_read(ES8311_I2S_PORT, pcmBuffer,
                              samplesPerFrame * sizeof(int16_t),
                              &bytesRead, pdMS_TO_TICKS(100));

    if (err != ESP_OK || bytesRead < (size_t)(samplesPerFrame * sizeof(int16_t)))
        return false;

    encodeFrame(pcmBuffer);
    return true;
}

bool VoiceMemoModule::decodeAndPlayFrame()
{
    if (!spkInitialized || !codec2 || !pcmBuffer) return false;
    if (playOffset + codecBytesPerFrame > playBuffer.size()) return false;

    codec2_decode(codec2, pcmBuffer, playBuffer.data() + playOffset);
    playOffset += codecBytesPerFrame;

    size_t bytesWritten = 0;
    i2s_write(ES8311_I2S_PORT, pcmBuffer,
              samplesPerFrame * sizeof(int16_t),
              &bytesWritten, pdMS_TO_TICKS(500));
    return true;
}

#else // !VOICE_MEMO_ES8311

// ============================================================
// MVSR hardware backend (T3-S3 V1)
// Separate MEMS mic on I2S_NUM_0 + MAX98357A speaker amp on I2S_NUM_1.
// ============================================================

void VoiceMemoModule::setMicEnable(bool enable)
{
    pinMode(MVSR_MIC_EN, OUTPUT);
    // V1.0 (I2S): HIGH=enable, LOW=disable
    // V1.1 (PDM): LOW=enable (inverted), HIGH=disable
    if (micEnableInverted)
        digitalWrite(MVSR_MIC_EN, enable ? LOW : HIGH);
    else
        digitalWrite(MVSR_MIC_EN, enable ? HIGH : LOW);
}

bool VoiceMemoModule::initMicI2S()
{
    i2s_config_t cfg = {};
    cfg.mode = (i2s_mode_t)(I2S_MODE_MASTER | I2S_MODE_RX);
    cfg.sample_rate = 8000;
    cfg.bits_per_sample = I2S_BITS_PER_SAMPLE_16BIT;
    cfg.channel_format = I2S_CHANNEL_FMT_ONLY_LEFT;
    cfg.communication_format = I2S_COMM_FORMAT_STAND_I2S;
    cfg.intr_alloc_flags = 0;
    cfg.dma_buf_count = 8;
    cfg.dma_buf_len = samplesPerFrame;
    cfg.use_apll = false;

    esp_err_t err = i2s_driver_install(MVSR_MIC_I2S_PORT, &cfg, 0, NULL);
    if (err != ESP_OK) {
        LOG_ERROR("VoiceMemo: I2S mic install failed: %d", err);
        return false;
    }

    i2s_pin_config_t pins = {};
    pins.bck_io_num = MVSR_MIC_BCLK;
    pins.ws_io_num = MVSR_MIC_WS;
    pins.data_out_num = I2S_PIN_NO_CHANGE;
    pins.data_in_num = MVSR_MIC_DATA;

    err = i2s_set_pin(MVSR_MIC_I2S_PORT, &pins);
    if (err != ESP_OK) {
        LOG_ERROR("VoiceMemo: I2S mic pin config failed: %d", err);
        i2s_driver_uninstall(MVSR_MIC_I2S_PORT);
        return false;
    }

    i2s_start(MVSR_MIC_I2S_PORT);
    return true;
}

bool VoiceMemoModule::initMicPDM()
{
    i2s_config_t cfg = {};
    cfg.mode = (i2s_mode_t)(I2S_MODE_MASTER | I2S_MODE_RX | I2S_MODE_PDM);
    cfg.sample_rate = 8000;
    cfg.bits_per_sample = I2S_BITS_PER_SAMPLE_16BIT;
    cfg.channel_format = I2S_CHANNEL_FMT_ONLY_LEFT;
    cfg.communication_format = I2S_COMM_FORMAT_STAND_I2S;
    cfg.intr_alloc_flags = 0;
    cfg.dma_buf_count = 8;
    cfg.dma_buf_len = samplesPerFrame;
    cfg.use_apll = false;

    esp_err_t err = i2s_driver_install(MVSR_MIC_I2S_PORT, &cfg, 0, NULL);
    if (err != ESP_OK) {
        LOG_ERROR("VoiceMemo: PDM mic install failed: %d", err);
        return false;
    }

    // PDM mode: WS pin = CLK, DATA pin = data in, BCLK unused
    i2s_pin_config_t pins = {};
    pins.bck_io_num = I2S_PIN_NO_CHANGE;
    pins.ws_io_num = MVSR_MIC_WS;
    pins.data_out_num = I2S_PIN_NO_CHANGE;
    pins.data_in_num = MVSR_MIC_DATA;

    err = i2s_set_pin(MVSR_MIC_I2S_PORT, &pins);
    if (err != ESP_OK) {
        LOG_ERROR("VoiceMemo: PDM mic pin config failed: %d", err);
        i2s_driver_uninstall(MVSR_MIC_I2S_PORT);
        return false;
    }

    i2s_start(MVSR_MIC_I2S_PORT);
    return true;
}

void VoiceMemoModule::initMic()
{
    if (micInitialized) return;

#ifdef MVSR_BOARD_V11
    // Forced V1.1 PDM mode
    micMode = MicMode::PDM;
    micEnableInverted = true;
#elif defined(MVSR_BOARD_V10)
    // Forced V1.0 I2S mode
    micMode = MicMode::I2S_STANDARD;
    micEnableInverted = false;
#elif defined(MVSR_MIC_AUTODETECT)
    // Auto-detect: try I2S first (V1.0). If mic reads return only silence/noise
    // after initialization, we'll detect PDM on subsequent frames.
    // Default to I2S (V1.0) for initial attempt.
    micMode = MicMode::I2S_STANDARD;
    micEnableInverted = false;
#endif

    setMicEnable(true);
    delay(10);

    bool ok;
    if (micMode == MicMode::PDM) {
        ok = initMicPDM();
    } else {
        ok = initMicI2S();
    }

    if (!ok) {
#ifdef MVSR_MIC_AUTODETECT
        // I2S failed — try PDM (V1.1 board)
        if (micMode == MicMode::I2S_STANDARD) {
            LOG_WARN("VoiceMemo: I2S mic failed, trying PDM (V1.1)");
            micMode = MicMode::PDM;
            micEnableInverted = true;
            setMicEnable(true);
            delay(10);
            ok = initMicPDM();
        }
        if (!ok) {
            LOG_ERROR("VoiceMemo: Both I2S and PDM mic init failed");
            setMicEnable(false);
            return;
        }
#else
        setMicEnable(false);
        return;
#endif
    }

    micInitialized = true;
    LOG_INFO("VoiceMemo: Mic initialized — mode=%s, WS=%d, DATA=%d, EN=%d (inverted=%d)",
             micMode == MicMode::PDM ? "PDM" : "I2S",
             MVSR_MIC_WS, MVSR_MIC_DATA, MVSR_MIC_EN, micEnableInverted);
}

void VoiceMemoModule::deinitMic()
{
    if (!micInitialized) return;
    i2s_stop(MVSR_MIC_I2S_PORT);
    i2s_driver_uninstall(MVSR_MIC_I2S_PORT);
    setMicEnable(false);
    micInitialized = false;

    // Restore boot button GPIO — the I2S driver corrupts GPIO0 input state on T3-S3,
    // causing it to read as permanently pressed. Re-init as INPUT_PULLUP to fix.
    // Also reset OneButton state to prevent the GPIO restore from being interpreted
    // as a 30-second button release (which would trigger shutdown).
#ifdef BUTTON_PIN
    pinMode(BUTTON_PIN, INPUT_PULLUP);
    extern ButtonThread *UserButtonThread;
    if (UserButtonThread)
        UserButtonThread->needsButtonReset = true;
    LOG_INFO("VoiceMemo: restored GPIO%d (button) after I2S deinit", BUTTON_PIN);
#endif
}

void VoiceMemoModule::initSpeaker()
{
    if (spkInitialized) return;

    pinMode(MVSR_SPK_SD_MODE, OUTPUT);
    digitalWrite(MVSR_SPK_SD_MODE, HIGH);
    delay(10);

    i2s_config_t cfg = {};
    cfg.mode = (i2s_mode_t)(I2S_MODE_MASTER | I2S_MODE_TX);
    cfg.sample_rate = 8000;
    cfg.bits_per_sample = I2S_BITS_PER_SAMPLE_16BIT;
    cfg.channel_format = I2S_CHANNEL_FMT_ONLY_LEFT;
    cfg.communication_format = I2S_COMM_FORMAT_STAND_I2S;
    cfg.intr_alloc_flags = 0;
    cfg.dma_buf_count = 8;
    cfg.dma_buf_len = samplesPerFrame;
    cfg.use_apll = false;
    cfg.tx_desc_auto_clear = true;

    esp_err_t err = i2s_driver_install(MVSR_SPK_I2S_PORT, &cfg, 0, NULL);
    if (err != ESP_OK) {
        LOG_ERROR("VoiceMemo: speaker I2S install failed: %d", err);
        return;
    }

    i2s_pin_config_t pins = {};
    pins.bck_io_num = MVSR_SPK_BCLK;
    pins.ws_io_num = MVSR_SPK_LRCLK;
    pins.data_out_num = MVSR_SPK_DATA;
    pins.data_in_num = I2S_PIN_NO_CHANGE;

    err = i2s_set_pin(MVSR_SPK_I2S_PORT, &pins);
    if (err != ESP_OK) {
        LOG_ERROR("VoiceMemo: speaker pin config failed: %d", err);
        i2s_driver_uninstall(MVSR_SPK_I2S_PORT);
        return;
    }

    i2s_start(MVSR_SPK_I2S_PORT);
    spkInitialized = true;
    LOG_INFO("VoiceMemo: Speaker initialized (BCLK=%d LRCLK=%d DATA=%d SD=%d)",
             MVSR_SPK_BCLK, MVSR_SPK_LRCLK, MVSR_SPK_DATA, MVSR_SPK_SD_MODE);
}

void VoiceMemoModule::deinitSpeaker()
{
    if (!spkInitialized) return;
    i2s_stop(MVSR_SPK_I2S_PORT);
    i2s_driver_uninstall(MVSR_SPK_I2S_PORT);
    digitalWrite(MVSR_SPK_SD_MODE, LOW);
    spkInitialized = false;
}

bool VoiceMemoModule::captureAndEncodeFrame()
{
    if (!micInitialized || !codec2 || !pcmBuffer) return false;

    size_t bytesRead = 0;
    esp_err_t err = i2s_read(MVSR_MIC_I2S_PORT, pcmBuffer,
                              samplesPerFrame * sizeof(int16_t),
                              &bytesRead, pdMS_TO_TICKS(100));

    if (err != ESP_OK || bytesRead < (size_t)(samplesPerFrame * sizeof(int16_t)))
        return false;

    encodeFrame(pcmBuffer);
    return true;
}

bool VoiceMemoModule::decodeAndPlayFrame()
{
    if (!spkInitialized || !codec2 || !pcmBuffer) return false;
    if (playOffset + codecBytesPerFrame > playBuffer.size()) return false;

    codec2_decode(codec2, pcmBuffer, playBuffer.data() + playOffset);
    playOffset += codecBytesPerFrame;

    size_t bytesWritten = 0;
    i2s_write(MVSR_SPK_I2S_PORT, pcmBuffer,
              samplesPerFrame * sizeof(int16_t),
              &bytesWritten, pdMS_TO_TICKS(500));
    return true;
}

#endif // VOICE_MEMO_ES8311

#endif // HAS_VOICE_MEMO

// --- Core logic (works on any ESP32 with Codec2) ---

void VoiceMemoModule::encodeFrame(const int16_t *pcm)
{
    if (!codec2) return;
    size_t prev = recordBuffer.size();
    recordBuffer.resize(prev + codecBytesPerFrame);
    codec2_encode(codec2, recordBuffer.data() + prev, (short *)pcm);
}

bool VoiceMemoModule::startRecording(uint32_t maxDurationMs)
{
    if (state != State::IDLE) {
        LOG_WARN("VoiceMemo: busy (state=%d)", (int)state);
        return false;
    }
    if (!codec2) {
        initCodec2();
        if (!codec2) return false;
    }

    if (maxDurationMs > 60000) maxDurationMs = 60000;
    if (maxDurationMs < 1000) maxDurationMs = 1000;
    maxRecordMs = maxDurationMs;

    recordBuffer.clear();
    recordBuffer.reserve(maxDurationMs / 10); // ~100 B/s, overshoot OK
    recordStartTime = millis();
    state = State::RECORDING;

#ifdef HAS_VOICE_MEMO
    initMic();

    // Restore boot button GPIO immediately after I2S init — the I2S driver
    // corrupts GPIO0 input state on ESP32-S3, making it read as permanently
    // pressed. The existing fix in deinitMic() only runs AFTER recording,
    // leaving the button unresponsive during the entire recording session.
    // Restoring here keeps the button working for Submit/Cancel interactions.
#ifdef BUTTON_PIN
    pinMode(BUTTON_PIN, INPUT_PULLUP);
    extern ButtonThread *UserButtonThread;
    if (UserButtonThread)
        UserButtonThread->needsButtonReset = true;
    LOG_INFO("VoiceMemo: restored GPIO%d (button) after I2S init", BUTTON_PIN);
#endif
#endif

    LOG_INFO("VoiceMemo: Recording started (max %u ms)", maxDurationMs);
    return true;
}

uint32_t VoiceMemoModule::stopAndSend(uint32_t destNodeId, uint8_t channelIndex)
{
    if (state != State::RECORDING) {
        LOG_WARN("VoiceMemo: not recording");
        return 0;
    }

    state = State::IDLE;
#ifdef HAS_VOICE_MEMO
    deinitMic();
#endif

    if (recordBuffer.empty()) {
        LOG_WARN("VoiceMemo: nothing recorded");
        return 0;
    }

    uint32_t durationSec = (millis() - recordStartTime + 500) / 1000;
    LOG_INFO("VoiceMemo: Stopped. %u bytes, ~%u s", recordBuffer.size(), durationSec);

    uint32_t checksum = crc32(recordBuffer.data(), recordBuffer.size());

    if (!mediaTransferModule) {
        LOG_ERROR("VoiceMemo: no MediaTransferModule");
        recordBuffer.clear();
        return 0;
    }

    uint32_t tid = mediaTransferModule->startTransfer(
        destNodeId, channelIndex,
        recordBuffer.data(), recordBuffer.size(),
        meshtastic_MediaContentType_VOICE_MEMO,
        checksum, "audio/codec2", durationSec);

    // Forward the recorded memo to the connected phone so it appears in the app
    if (tid > 0) {
        forwardToPhone(recordBuffer.data(), recordBuffer.size(), myNodeInfo.my_node_num, tid);
    }

    recordBuffer.clear();
    recordBuffer.shrink_to_fit();

    if (tid > 0)
        LOG_INFO("VoiceMemo: transfer %u to 0x%08x", tid, destNodeId);
    else
        LOG_ERROR("VoiceMemo: transfer start failed");

    return tid;
}

void VoiceMemoModule::cancelRecording()
{
    if (state != State::RECORDING) return;
    state = State::IDLE;
#ifdef HAS_VOICE_MEMO
    deinitMic();
#endif
    recordBuffer.clear();
    recordBuffer.shrink_to_fit();
    LOG_INFO("VoiceMemo: Recording cancelled");
}

bool VoiceMemoModule::playVoiceMemo(const uint8_t *data, uint32_t size)
{
    if (state != State::IDLE) {
        LOG_WARN("VoiceMemo: busy (state=%d)", (int)state);
        return false;
    }
    if (!codec2) {
        initCodec2();
        if (!codec2) return false;
    }
    if (size < (uint32_t)codecBytesPerFrame) {
        LOG_WARN("VoiceMemo: data too small");
        return false;
    }

    playBuffer.assign(data, data + size);
    playOffset = 0;
    state = State::PLAYING;

#ifdef HAS_VOICE_MEMO
    initSpeaker();
#endif

    uint32_t frames = size / codecBytesPerFrame;
    LOG_INFO("VoiceMemo: Playing %u frames (~%u ms)", frames, frames * 40);
    return true;
}

void VoiceMemoModule::stopPlayback()
{
    if (state != State::PLAYING) return;
    state = State::IDLE;
#ifdef HAS_VOICE_MEMO
    deinitSpeaker();
#endif
    playBuffer.clear();
    playBuffer.shrink_to_fit();
    playOffset = 0;
    LOG_INFO("VoiceMemo: Playback stopped");
}

void VoiceMemoModule::onTransferComplete(const uint8_t *data, uint32_t size, uint32_t fromNode, uint32_t transferId)
{
    LOG_INFO("VoiceMemo: Received voice memo from 0x%08x (%u bytes, tid=%u)", fromNode, size, transferId);

    // Store for on-device playback
    ReceivedMemo memo;
    memo.data.assign(data, data + size);
    memo.fromNode = fromNode;
    memo.timestamp = getValidTime(RTCQuality::RTCQualityDevice, false);
    if (memo.timestamp == 0)
        memo.timestamp = millis() / 1000;

    receivedMemos[transferId] = std::move(memo);

    // Evict oldest if over limit
    while (receivedMemos.size() > MAX_STORED_MEMOS) {
        receivedMemos.erase(receivedMemos.begin());
    }

#if HAS_SCREEN
    // Create StoredMessage for display in chat
    StoredMessage sm;
    sm.timestamp = receivedMemos[transferId].timestamp;
    sm.isBootRelative = (getValidTime(RTCQuality::RTCQualityDevice, false) == 0);
    sm.sender = fromNode;
    sm.channelIndex = 0;
    sm.dest = myNodeInfo.my_node_num;
    sm.type = MessageType::DM_TO_US;
    sm.isVoiceMemo = true;
    sm.voiceMemoTransferId = transferId;
    sm.textOffset = MessageStore::storeText("[voice memo]", 12);
    sm.textLength = 12;
    messageStore.addLiveMessage(std::move(sm));
#endif

    // Defer heavy work (Codec2 init, I2S speaker init, PCM decode) to runOnce()
    // to avoid stack overflow — this callback runs deep inside the handleReceived chain.
    pendingCompletion.data.assign(data, data + size);
    pendingCompletion.fromNode = fromNode;
    pendingCompletion.transferId = transferId;
#ifdef HAS_VOICE_MEMO
    pendingCompletion.needsPlayback = true;
#endif
    pendingCompletion.needsPhoneForward = true;
}

void VoiceMemoModule::forwardToPhone(const uint8_t *codec2Data, uint32_t size, uint32_t fromNode, uint32_t transferId)
{
    // Decode Codec2 data to PCM, then stream to phone in batches via runOnce().
    // This avoids blocking the main thread for seconds with thousands of packets.

    if (!codec2) initCodec2();
    if (!codec2 || codecBytesPerFrame <= 0 || samplesPerFrame <= 0) {
        LOG_ERROR("VoiceMemo: Cannot decode — Codec2 not initialized");
        return;
    }

    uint32_t numFrames = size / codecBytesPerFrame;
    if (numFrames == 0) {
        LOG_WARN("VoiceMemo: Data too small for Codec2 decode (%u bytes, need %d per frame)",
                 size, codecBytesPerFrame);
        deinitCodec2();
        return;
    }

    uint32_t pcmSize = numFrames * samplesPerFrame * sizeof(int16_t);
    phoneForward.pcmData.resize(pcmSize);
    int16_t *pcmOut = (int16_t *)phoneForward.pcmData.data();

    for (uint32_t f = 0; f < numFrames; f++) {
        codec2_decode(codec2, pcmOut + f * samplesPerFrame,
                      const_cast<uint8_t *>(codec2Data + f * codecBytesPerFrame));
    }

    deinitCodec2();

    LOG_INFO("VoiceMemo: Decoded %u Codec2 bytes -> %u PCM bytes (%u frames), streaming to phone",
             size, pcmSize, numFrames);

    static constexpr uint32_t CHUNK_SIZE = 200;
    phoneForward.fromNode = fromNode;
    phoneForward.transferId = transferId;
    phoneForward.totalChunks = (pcmSize + CHUNK_SIZE - 1) / CHUNK_SIZE;
    phoneForward.nextChunk = 0;
    phoneForward.checksum = crc32(phoneForward.pcmData.data(), pcmSize);
    phoneForward.sentStart = false;
    phoneForward.isImage = false;
    phoneForward.active = true;
    state = State::FORWARDING_TO_PHONE;
}

bool VoiceMemoModule::sendPhoneForwardBatch()
{
    // Send up to BATCH_SIZE chunks per runOnce() call to avoid blocking
    static constexpr uint32_t BATCH_SIZE = 20;
    static constexpr uint32_t CHUNK_SIZE = 200;

    auto &pf = phoneForward;
    uint32_t dataSize = pf.pcmData.size();

    // Binary header: [type:1][transferId:4][seqNum:2][totalChunks:2][checksum:4] = 13 bytes
    auto buildHeader = [&](uint8_t type, uint16_t seq) -> std::vector<uint8_t> {
        std::vector<uint8_t> hdr(13);
        hdr[0] = type;
        memcpy(&hdr[1], &pf.transferId, 4);
        memcpy(&hdr[5], &seq, 2);
        uint16_t tc = (uint16_t)pf.totalChunks;
        memcpy(&hdr[7], &tc, 2);
        memcpy(&hdr[9], &pf.checksum, 4);
        return hdr;
    };

    auto sendPacket = [&](const std::vector<uint8_t> &packet) {
        meshtastic_MeshPacket *mp = packetPool.allocZeroed();
        if (!mp) return;
        mp->to = myNodeInfo.my_node_num;
        mp->from = pf.fromNode;
        mp->decoded.portnum = meshtastic_PortNum_PRIVATE_APP;
        mp->decoded.payload.size = packet.size();
        memcpy(mp->decoded.payload.bytes, packet.data(), packet.size());
        mp->decoded.want_response = false;
        service->sendToPhone(mp);
    };

    // Type codes: voice = 0x01/0x02/0x03, image = 0x04/0x05/0x06
    uint8_t typeStart = pf.isImage ? 0x04 : 0x01;
    uint8_t typeChunk = pf.isImage ? 0x05 : 0x02;
    uint8_t typeEnd   = pf.isImage ? 0x06 : 0x03;

    // Send START header on first call
    if (!pf.sentStart) {
        sendPacket(buildHeader(typeStart, 0));
        pf.sentStart = true;
    }

    // Send a batch of chunks
    uint32_t sent = 0;
    while (pf.nextChunk < pf.totalChunks && sent < BATCH_SIZE) {
        uint32_t offset = pf.nextChunk * CHUNK_SIZE;
        uint32_t chunkLen = std::min(CHUNK_SIZE, dataSize - offset);

        auto hdr = buildHeader(typeChunk, (uint16_t)pf.nextChunk);
        hdr.insert(hdr.end(), pf.pcmData.data() + offset, pf.pcmData.data() + offset + chunkLen);
        sendPacket(hdr);
        pf.nextChunk++;
        sent++;
    }

    // All chunks sent — send END and clean up
    if (pf.nextChunk >= pf.totalChunks) {
        sendPacket(buildHeader(typeEnd, 0));
        LOG_INFO("VoiceMemo: Forwarded %u bytes to phone (%u chunks, %s)",
                 dataSize, pf.totalChunks, pf.isImage ? "image" : "voice");
        pf.reset();
        return false; // done
    }

    return true; // more chunks remain
}

// Helper: set up image forwarding to phone via the streaming state machine.
// Called from MediaTransferModule's completion callback — defers actual sending
// to VoiceMemoModule::runOnce() to avoid blocking the main thread.
static void forwardImageToPhone(const uint8_t *imageData, uint32_t size, uint32_t fromNode, uint32_t transferId)
{
    if (!voiceMemoModule) {
        LOG_ERROR("ForwardImage: voiceMemoModule not available");
        return;
    }

    // If already forwarding something, log a warning but proceed (overwrites previous)
    auto &pf = voiceMemoModule->phoneForward;
    if (pf.active) {
        LOG_WARN("ForwardImage: Overwriting active phone forward (was %s, chunk %u/%u)",
                 pf.isImage ? "image" : "voice", pf.nextChunk, pf.totalChunks);
    }

    static constexpr uint32_t CHUNK_SIZE = 200;

    pf.pcmData.assign(imageData, imageData + size);
    pf.fromNode = fromNode;
    pf.transferId = transferId;
    pf.totalChunks = (size + CHUNK_SIZE - 1) / CHUNK_SIZE;
    pf.nextChunk = 0;
    pf.checksum = VoiceMemoModule::crc32(imageData, size);
    pf.sentStart = false;
    pf.isImage = true;
    pf.active = true;

    LOG_INFO("ForwardImage: Queued %u bytes for streaming to phone (%u chunks, crc=%08x)",
             size, pf.totalChunks, pf.checksum);
}

const VoiceMemoModule::ReceivedMemo *VoiceMemoModule::getMemo(uint32_t transferId) const
{
    auto it = receivedMemos.find(transferId);
    return (it != receivedMemos.end()) ? &it->second : nullptr;
}

uint32_t VoiceMemoModule::generateAndSendTestMemo(uint32_t destNodeId, uint8_t channelIndex,
                                                    uint32_t durationMs)
{
    if (!codec2) {
        initCodec2();
        if (!codec2) return 0;
    }
    if (durationMs > 30000) durationMs = 30000;
    if (durationMs < 1000) durationMs = 1000;

    uint32_t totalFrames = durationMs / 40;
    recordBuffer.clear();
    recordBuffer.reserve(totalFrames * codecBytesPerFrame);

    int16_t *synth = new int16_t[samplesPerFrame];
    uint32_t sampleIdx = 0;

    for (uint32_t f = 0; f < totalFrames; f++) {
        for (int i = 0; i < samplesPerFrame; i++) {
            // 440 Hz sine at 8 kHz sample rate
            float t = (float)(sampleIdx + i) / 8000.0f;
            synth[i] = (int16_t)(16000.0f * sinf(2.0f * 3.14159265f * 440.0f * t));
        }
        sampleIdx += samplesPerFrame;
        encodeFrame(synth);
    }
    delete[] synth;

    LOG_INFO("VoiceMemo: Generated test memo — %u frames, %u bytes", totalFrames, recordBuffer.size());

    uint32_t checksum = crc32(recordBuffer.data(), recordBuffer.size());
    if (!mediaTransferModule) {
        LOG_ERROR("VoiceMemo: no MediaTransferModule");
        recordBuffer.clear();
        return 0;
    }

    uint32_t tid = mediaTransferModule->startTransfer(
        destNodeId, channelIndex,
        recordBuffer.data(), recordBuffer.size(),
        meshtastic_MediaContentType_VOICE_MEMO,
        checksum, "audio/codec2",
        (durationMs + 500) / 1000);

    recordBuffer.clear();
    recordBuffer.shrink_to_fit();

    if (tid > 0)
        LOG_INFO("VoiceMemo: test memo transfer %u to 0x%08x", tid, destNodeId);
    return tid;
}

float VoiceMemoModule::getRecordingDuration() const
{
    if (state != State::RECORDING) return 0.0f;
    return (float)(millis() - recordStartTime) / 1000.0f;
}

int32_t VoiceMemoModule::runOnce()
{
    if (!moduleConfig.has_media_transfer || !moduleConfig.media_transfer.enabled) {
        return disable();
    }

    switch (state) {
    case State::RECORDING: {
        if (millis() - recordStartTime >= maxRecordMs) {
            LOG_INFO("VoiceMemo: max duration reached (%u ms)", maxRecordMs);
            state = State::IDLE;
#ifdef HAS_VOICE_MEMO
            deinitMic();
#endif
            return 1000;
        }
#ifdef HAS_VOICE_MEMO
        captureAndEncodeFrame();
        return 5;
#else
        return 100;
#endif
    }

    case State::PLAYING: {
#ifdef HAS_VOICE_MEMO
        if (!decodeAndPlayFrame()) {
            LOG_INFO("VoiceMemo: Playback complete");
            stopPlayback();
            return 1000;
        }
        return 5;
#else
        stopPlayback();
        return 1000;
#endif
    }

    case State::FORWARDING_TO_PHONE: {
        // Stream chunks to phone in batches — yields between batches
        if (sendPhoneForwardBatch()) {
            return 5; // more chunks remain, reschedule immediately
        }
        // Done forwarding — check if playback is also pending
        state = State::IDLE;
        if (pendingCompletion.needsPlayback) {
            pendingCompletion.needsPlayback = false;
            playVoiceMemo(pendingCompletion.data.data(), pendingCompletion.data.size());
            pendingCompletion.data.clear();
            pendingCompletion.data.shrink_to_fit();
        }
        return 100;
    }

    case State::IDLE:
    default:
        // Check for active phone forward (queued by forwardImageToPhone)
        if (phoneForward.active) {
            state = State::FORWARDING_TO_PHONE;
            return 0; // process immediately
        }
        // Process deferred transfer completion (Codec2 decode then streaming forward)
        if (pendingCompletion.needsPhoneForward) {
            pendingCompletion.needsPhoneForward = false;
            forwardToPhone(pendingCompletion.data.data(), pendingCompletion.data.size(),
                           pendingCompletion.fromNode, pendingCompletion.transferId);
            // forwardToPhone sets state = FORWARDING_TO_PHONE, so return immediately
            return 0;
        }
        if (pendingCompletion.needsPlayback) {
            pendingCompletion.needsPlayback = false;
            playVoiceMemo(pendingCompletion.data.data(), pendingCompletion.data.size());
            pendingCompletion.data.clear();
            pendingCompletion.data.shrink_to_fit();
        }
        return 1000;
    }
}

uint32_t VoiceMemoModule::crc32(const uint8_t *data, uint32_t length)
{
    uint32_t crc = 0xFFFFFFFF;
    for (uint32_t i = 0; i < length; i++) {
        crc ^= data[i];
        for (int j = 0; j < 8; j++) {
            if (crc & 1)
                crc = (crc >> 1) ^ 0xEDB88320;
            else
                crc >>= 1;
        }
    }
    return ~crc;
}

#endif // ARCH_ESP32
