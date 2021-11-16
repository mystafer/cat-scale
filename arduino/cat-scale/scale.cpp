#include "scale.h"

ScaleManager::ScaleManager(DFRobot_HT1632C *ht1632c, PublishWeightToAWSFunction publishWeightToAWS) {
  this->ht1632c = ht1632c;
  this->publishWeightToAWS = publishWeightToAWS;
}

void ScaleManager::displayNumericValueOnLCD(float value) {
    if (value != lastDisplayedValue) {
      lastDisplayedValue = value;
      char result[8]; // Buffer big enough for 7-character float
      dtostrf(value, 6, 1, result);
      ht1632c->clearScreen();
      ht1632c->print(result);  // Display on LED matrix
    }
}


void ScaleManager::setup() {
  // setup scale
  scale.begin(DOUT, CLK);
  
  updateSettings(settings);
  
  //Assuming there is no weight on the scale at start up, reset the scale to 0
  ht1632c->print("reset...",50);
  tareScale();
}

void ScaleManager::tareScale() {
  scale.tare();
  ESP.wdtFeed();
  tareCounter = 0;
  displayNumericValueOnLCD(0);
  publishWeightToAWS(0, true);
}

bool ScaleManager::checkWeightHolding(float weight) {

  // weight is super low, reset
  if (weight < IGNORE_LOW_THRESHOLD) {
      Serial.println("Weight super low, updating as new tare value");
      ht1632c->print("low tare...", 50);
      tareScale();
      lastHoldTime = millis();
  }
  
  // weight has varied over threshold, reset counters
  else if (lastHoldTime == 0 || abs(weight - lastHoldValue) > settings.tareHoldThreshold) {
    lastHoldValue = weight;
    lastHoldTime = millis();
    return false;
  }

  // check if scale is already non-zero before resetting
  else if (abs(weight) > settings.tareHoldThreshold) {
    
    // weight is holding, check timing
    if (millis() - lastHoldTime > settings.tareHoldTime) {
      Serial.println("Weight holding, updating as new tare value");
      ht1632c->print("new tare...", 50);
      tareScale();
      lastHoldTime = millis();
    }
    
  }

  // scale is already zero, reset timer
  else {
    lastHoldTime = millis();
  }

  return true;
}

void ScaleManager::loop() {

  float weight = scale.get_units(settings.numMeasurements/2);  // initial measure measurement - get a few of them but not entire count
  ESP.wdtFeed();
//  Serial.println(weight);

  float avgweight = 0;
  if (weight > settings.largeThreshold) { // Takes measures if the weight is greater than the threshold

    // scale is no longer empty
    float weight0 = scale.get_units();
    for (int i = 0; i < settings.numMeasurements; i++) {  // Takes several measurements
      delay(100);
      weight = scale.get_units();
      ESP.wdtFeed();
      avgweight = avgweight + weight;
      if (abs(weight - weight0) > settings.smallThreshold) {
        avgweight = 0;
        i = 0;
      }
      weight0 = weight;
    }
    
    avgweight = avgweight / settings.numMeasurements; // Calculate average weight
    if (abs(avgweight) < settings.tareHoldThreshold) 
      displayNumericValueOnLCD(0.0);
    else
      displayNumericValueOnLCD(avgweight);
    
    //save weight to AWS
    if (!checkWeightHolding(avgweight) && avgweight > IGNORE_LOW_THRESHOLD && avgweight < IGNORE_HIGH_THRESHOLD)
      publishWeightToAWS(avgweight, false);

    ESP.wdtFeed();
  }
  else {

    // check if weight has returned to zero
    avgweight = scale.get_units(settings.numMeasurements);
    ESP.wdtFeed();
    if (abs(avgweight) < settings.tareHoldThreshold) {
      displayNumericValueOnLCD(0.0);
      publishWeightToAWS(0, false);
      ESP.wdtFeed();
    }
      
    checkWeightHolding(avgweight);
    ESP.wdtFeed();
  }
}

void ScaleManager::updateSettings(ScaleSettings settings) {
  Serial.println("Changing scale settings");

  this->settings = settings;

  Serial.print("Setting calibration to -> ");
  Serial.println(settings.calibrationFactor);
  scale.set_scale(settings.calibrationFactor);  
}
