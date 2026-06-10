#include "SerialConsole.h"
#include "Default.h"
#include "NodeDB.h"
#include "PowerFSM.h"
#include "Throttle.h"
#include "concurrency/LockGuard.h"
#include "configuration.h"
#include "time.h"

#if defined(ARDUINO_USB_CDC_ON_BOOT) && ARDUINO_USB_CDC_ON_BOOT
#define IS_USB_SERIAL
#ifdef SERIAL_HAS_ON_RECEIVE
#undef SERIAL_HAS_ON_RECEIVE
#endif
#include "HWCDC.h"
#endif

#ifdef RP2040_SLOW_CLOCK
#define Port Serial2
#else
#ifdef USER_DEBUG_PORT // change by WayenWeng
#define Port USER_DEBUG_PORT
#else
#define Port Serial
#endif
#endif
// Defaulting to the formerly removed phone_timeout_secs value of 15 minutes
#define SERIAL_CONNECTION_TIMEOUT (15 * 60) * 1000UL

SerialConsole *console;

void consoleInit()
{
    if (console) {
        return;
    }
    auto sc = new SerialConsole(); // Must be dynamically allocated because we are now inheriting from thread

#if defined(SERIAL_HAS_ON_RECEIVE)
    // onReceive does only exist for HardwareSerial not for USB CDC serial
    Port.onReceive([sc]() { sc->rxInt(); });
#else
    (void)sc;
#endif
    DEBUG_PORT.rpInit(); // Simply sets up semaphore
}

void consolePrintf(const char *format, ...)
{
    va_list arg;
    va_start(arg, format);
    console->vprintf(nullptr, format, arg);
    va_end(arg);
    console->flush();
}

size_t SerialConsole::write(uint8_t c)
{
    // When using protobufs, all serial writes must be serialized with
    // streamLock to prevent interleaved bytes on the USB CDC endpoint.
    // Unserialized character writes from debug logging can interleave with
    // protobuf packet emission, corrupting the CDC framing and causing the
    // host to disconnect the device (observed on XIAO ESP32-S3).
    if (usingProtobufs) {
        concurrency::LockGuard guard(&streamLock);
        if (c == '\n')
            RedirectablePrint::write('\r');
        return RedirectablePrint::write(c);
    }
    if (c == '\n')
        RedirectablePrint::write('\r');
    return RedirectablePrint::write(c);
}

SerialConsole::SerialConsole() : StreamAPI(&Port), RedirectablePrint(&Port), concurrency::OSThread("SerialConsole")
{
    api_type = TYPE_SERIAL;
    assert(!console);
    console = this;
    canWrite = false; // We don't send packets to our port until it has talked to us first

#ifdef RP2040_SLOW_CLOCK
    Port.setTX(SERIAL2_TX);
    Port.setRX(SERIAL2_RX);
#endif
    Port.begin(SERIAL_BAUD);
#if defined(ARCH_NRF52) || defined(CONFIG_IDF_TARGET_ESP32S2) || defined(CONFIG_IDF_TARGET_ESP32S3) || defined(ARCH_RP2040) ||   \
    defined(CONFIG_IDF_TARGET_ESP32C3) || defined(CONFIG_IDF_TARGET_ESP32C6)
    time_t timeout = millis();
    while (!Port) {
        if (Throttle::isWithinTimespanMs(timeout, FIVE_SECONDS_MS)) {
            delay(100);
        } else {
            break;
        }
    }
#endif
#if defined(CONFIG_IDF_TARGET_ESP32S2) || defined(CONFIG_IDF_TARGET_ESP32S3) ||                                                   \
    defined(CONFIG_IDF_TARGET_ESP32C3) || defined(CONFIG_IDF_TARGET_ESP32C6)
    // Bound (but do NOT zero) the USB-CDC (HWCDC) write timeout. Under heavy serial
    // output — e.g. the media-transfer module forwarding START/CHUNK/COMPLETE packets —
    // the default (effectively blocking) write can stall the loop task long enough to
    // trip the task watchdog → chip reset → USB re-enumerates ("Device not configured").
    // But setting it to 0 (fully non-blocking) drops bytes during the normal config-dump
    // burst at connect, corrupting the protobuf stream so the phone/CLI handshake never
    // completes ("Error parsing FromRadio"). 100 ms is the balance: the host drains the
    // buffer in well under 100 ms during an active read, so no handshake drops, yet a
    // single write can never block anywhere near the multi-second loop watchdog.
    Port.setTxTimeoutMs(100);
#endif
#if !ARCH_PORTDUINO
    emitRebooted();
#endif
}

int32_t SerialConsole::runOnce()
{
#ifdef HELTEC_MESH_SOLAR
    // After enabling the mesh solar serial port module configuration, command processing is handled by the serial port module.
    if (moduleConfig.serial.enabled && moduleConfig.serial.override_console_serial_port &&
        moduleConfig.serial.mode == meshtastic_ModuleConfig_SerialConfig_Serial_Mode_MS_CONFIG) {
        return 250;
    }
#endif

    int32_t delay = runOncePart();
#if defined(SERIAL_HAS_ON_RECEIVE) || defined(CONFIG_IDF_TARGET_ESP32S2)
    return Port.available() ? delay : INT32_MAX;
#elif defined(IS_USB_SERIAL)
    // Always use normal delay — HWCDC::isPlugged() can glitch momentarily
    // causing the serial console to become permanently unresponsive.
    return delay;
#else
    return delay;
#endif
}

void SerialConsole::flush()
{
    Port.flush();
}

// trigger tx of serial data
void SerialConsole::onNowHasData(uint32_t fromRadioNum)
{
    setIntervalFromNow(0);
}

// trigger rx of serial data
void SerialConsole::rxInt()
{
    setIntervalFromNow(0);
}

// For the serial port we can't really detect if any client is on the other side, so instead just look for recent messages.
// Note: HWCDC::isPlugged() check removed — it can return false during momentary USB signal glitches
// on ESP32-S3, causing permanent serial disconnection. The timeout-based check is sufficient.
bool SerialConsole::checkIsConnected()
{
    return Throttle::isWithinTimespanMs(lastContactMsec, SERIAL_CONNECTION_TIMEOUT);
}

/**
 * we override this to notice when we've received a protobuf over the serial
 * stream.  Then we shut off debug serial output.
 */
bool SerialConsole::handleToRadio(const uint8_t *buf, size_t len)
{
    // only talk to the API once the configuration has been loaded and we're sure the serial port is not disabled.
    if (config.has_lora && config.security.serial_enabled) {
        // Switch to protobufs for log messages
        usingProtobufs = true;
        canWrite = true;

        return StreamAPI::handleToRadio(buf, len);
    } else {
        return false;
    }
}

void SerialConsole::log_to_serial(const char *logLevel, const char *format, va_list arg)
{
    if (usingProtobufs) {
        if (config.security.debug_log_api_enabled) {
            meshtastic_LogRecord_Level ll = RedirectablePrint::getLogLevel(logLevel);
            auto thread = concurrency::OSThread::currentThread;
            emitLogRecord(ll, thread ? thread->ThreadName.c_str() : "", format, arg);
        }
        // When protobuf API is active but debug logging disabled: suppress entirely
        // to prevent plain text from corrupting the protobuf serial stream
        return;
    }
    RedirectablePrint::log_to_serial(logLevel, format, arg);
}