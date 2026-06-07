#include "RadioLib.h"

// LR1121 PA dual-band RF switch truth table
// DIO5/DIO6 control sub-GHz path (proven working)
// DIO7/DIO8 control 2.4 GHz path (from LilyGo LR1121 PA hardware docs)
static const uint32_t rfswitch_dio_pins[] = {
    RADIOLIB_LR11X0_DIO5, RADIOLIB_LR11X0_DIO6,
    RADIOLIB_LR11X0_DIO7, RADIOLIB_LR11X0_DIO8,
    RADIOLIB_NC};

static const Module::RfSwitchMode_t rfswitch_table[] = {
    //                          DIO5  DIO6  DIO7  DIO8
    {LR11x0::MODE_STBY,   {LOW,  LOW,  LOW,  LOW }},
    {LR11x0::MODE_RX,     {LOW,  HIGH, LOW,  LOW }},  // sub-GHz RX
    {LR11x0::MODE_TX,     {HIGH, LOW,  LOW,  LOW }},  // sub-GHz TX
    {LR11x0::MODE_TX_HP,  {HIGH, LOW,  LOW,  LOW }},  // sub-GHz TX high power
    {LR11x0::MODE_TX_HF,  {LOW,  LOW,  HIGH, LOW }},  // 2.4 GHz TX
    {LR11x0::MODE_GNSS,   {LOW,  LOW,  LOW,  LOW }},
    {LR11x0::MODE_WIFI,   {LOW,  LOW,  LOW,  HIGH}},  // 2.4 GHz RX (WiFi scan)
    END_OF_MODE_TABLE,
};
