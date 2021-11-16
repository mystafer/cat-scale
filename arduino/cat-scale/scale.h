#ifndef scale_h
#define scale_h

#include <Arduino.h>
#include "HX711.h"  // HX711 library for the scale
#include "DFRobot_HT1632C.h" // Library for DFRobot LED matrix display

#define DEFAULT_CALIBRATION_FACTOR -10800.0

#define DOUT 2  // D5  Pin connected to HX711 data output pin
#define CLK  10 // D3  Pin connected to HX711 clk pin

#define DEFAULT_NUM_MEASUREMENTS 10  // Number of measurements
#define DEFAULT_LARGE_THRESHOLD 1.0  // Measures only if the weight is greater than 2 lb
#define DEFAULT_SMALL_THRESHOLD 0.5  // Restart averaging if the weight changes more than 0.5 lb

#define IGNORE_HIGH_THRESHOLD 200.0 // high lbs to ignore above
#define IGNORE_LOW_THRESHOLD  -50.0  // low lbs to ignore below

#define DEFAULT_TARE_HOLD_THRESHOLD 0.25        // retare the scale if the weight is holding steady for tare hold time
#define DEFAULT_TARE_HOLD_TIME 5 * 60 * 1000    // amount of time to wait in ms at a specific value before reseting scale
#define MIN_TARE_HOLD_TIME 15 * 1000            // min amount of time to wait in ms at a specific value before reseting scale

#define DATA 5  // D6 Pin for DFRobot LED matrix display
#define CS   13 // D2 Pin for DFRobot LED matrix display
#define WR   4  // D7 Pin for DFRobot LED matrix display


typedef void (*PublishWeightToAWSFunction)(float weight, bool tare);

#define NEG_INF -1.0 / 0.0

struct ScaleSettings {
  float calibrationFactor = DEFAULT_CALIBRATION_FACTOR;
  int numMeasurements = DEFAULT_NUM_MEASUREMENTS;
  float largeThreshold = DEFAULT_LARGE_THRESHOLD;
  float smallThreshold = DEFAULT_SMALL_THRESHOLD;
  float tareHoldThreshold = DEFAULT_TARE_HOLD_THRESHOLD;
  long tareHoldTime = DEFAULT_TARE_HOLD_TIME;
};

class ScaleManager
{
  private:
    int tareCounter = 0;
    float lastDisplayedValue = NEG_INF;
    DFRobot_HT1632C* ht1632c;
    HX711 scale = HX711();
    PublishWeightToAWSFunction publishWeightToAWS;

    float lastHoldValue = NEG_INF;
    long lastHoldTime = 0;

    
  public:
    ScaleSettings settings;
    
    ScaleManager(DFRobot_HT1632C *ht1632c, PublishWeightToAWSFunction publishWeightToAWS);

    void setup();
    void loop();
    void tareScale();

    void updateSettings(ScaleSettings settings);

  private:
    void displayNumericValueOnLCD(float value);
    bool checkWeightHolding(float weight);
};

#endif
