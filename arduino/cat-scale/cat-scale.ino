#define AWS_ENABLED true

#include "scale.h"
#include <ArduinoJson.h>

// set up LED matrix display
DFRobot_HT1632C ht1632c = DFRobot_HT1632C(DATA, WR,CS); 

#if AWS_ENABLED
  #include "AWSIOT.h"
  
  // create AWS IOT connection manager
  void messageHandler(String &topic, String &payload);
  void iotEventListener(int event, const char* msg);
  AWSIOT awsiot = AWSIOT(messageHandler, iotEventListener);
#endif

// create scale manager
void publishWeightToAWS(float weight, bool tare);
ScaleManager scaleMgr = ScaleManager(&ht1632c, publishWeightToAWS);

// keep track of shadow version being processed
int currentShadowVersion = -1;

void setup() {

  // start the serial connection
  Serial.begin(115200);
  delay(2000);
  Serial.println();
  Serial.println();
  
  // setup LED matrix display
  ht1632c.begin();
  ht1632c.isLedOn(true);
  ht1632c.clearScreen();
  delay(500);

  ESP.wdtDisable();
  
  // connect to AWS
  #if AWS_ENABLED
    awsiot.setup();
  #endif
  
  // setup scale
  scaleMgr.setup();
}

void loop() {
  #if AWS_ENABLED
    awsiot.loop();
  #endif
  
  scaleMgr.loop();

  ESP.wdtFeed();
}


void publishWeightToAWS(float weight, bool tare) {
  #if AWS_ENABLED
    awsiot.publishWeightToAWS(weight, tare);
  #endif
}


void processShadowSettings(JsonVariant state, long shadowVersion) {
  if (shadowVersion > currentShadowVersion) {
    
    // make a copy of current settings
    ScaleSettings settings;
    memcpy ( &settings, &(scaleMgr.settings), sizeof(settings) );

    // calibration factor cannot be zero
    float calibrationFactor = state["calibrationFactor"];
    if (calibrationFactor != 0) {
      settings.calibrationFactor = calibrationFactor;
    }

    // number of measurements to take must be greater than 1
    int numMeasurements = state["numMeasurements"];
    if (numMeasurements > 1) {
      settings.numMeasurements = numMeasurements;
    }

    // large threshold must be greater than zero
    float largeThreshold = state["largeThreshold"];
    if (largeThreshold > 0) {
      settings.largeThreshold = largeThreshold;
    }

    // large threshold must be greater than zero and less than large
    float smallThreshold = state["smallThreshold"];
    if (smallThreshold > 0 && smallThreshold < settings.largeThreshold) {
      settings.smallThreshold = smallThreshold;
    }

    // tare hold threshold must be greater than zero
    float tareHoldThreshold = state["tareHoldThreshold"];
    if (tareHoldThreshold > 0) {
      settings.tareHoldThreshold = tareHoldThreshold;
    }

    // tare hold time must be greater than minimum value
    long tareHoldTime = state["tareHoldTime"];
    if (tareHoldTime > MIN_TARE_HOLD_TIME) {
      settings.tareHoldTime = tareHoldTime;
    }
    
    // store currently fetched version
    currentShadowVersion = shadowVersion;

    // update scale settings
    scaleMgr.updateSettings(settings);
    ESP.wdtFeed();
  }  
}

void messageHandler(String &topic, String &payload) {
  ESP.wdtFeed();
  Serial.println("incoming: " + topic + " - " + payload);

  // check which topic is sending data
  if (topic == AWS_IOT_SHADOW_GET_ACCEPTED_SUB) {
    // holds parsed document
    StaticJsonDocument<4096> doc;
  
    // filter for desired information
    StaticJsonDocument<200> filter;
    filter["state"] = true;
    filter["version"] = true;
    filter["state"]["desired"] = true;
    if (deserializeJson(doc, payload, DeserializationOption::Filter(filter)) == DeserializationError::Ok) {
      Serial.println("Get payload");
      serializeJson(doc, Serial);
      Serial.println();

      // retrieve initial settings
      JsonVariant state = doc["state"]["desired"];
      
      // retrieve version
      long shadowVersion = doc["version"];

      processShadowSettings(state, shadowVersion);
    }
  }

  else if (topic == AWS_IOT_SHADOW_UPDATE_DELTA_SUB) {
    // holds parsed document
    StaticJsonDocument<4096> doc;
  
    // filter for desired information
    StaticJsonDocument<200> filter;
    filter["state"] = true;
    filter["version"] = true;
    if (deserializeJson(doc, payload, DeserializationOption::Filter(filter)) == DeserializationError::Ok) {
      Serial.println("Delta payload");
      serializeJson(doc, Serial);
      Serial.println();

      // retrieve initial settings
      JsonVariant state = doc["state"];
      
      // retrieve version
      long shadowVersion = doc["version"];

      processShadowSettings(state, shadowVersion);
    }
  }

  // anything send on subscribe topic will tare the scale
  else if (topic == AWS_IOT_SUBSCRIBE_TOPIC) {
    Serial.println("Reset message received");
    ht1632c.print("tare...", 50);
    ESP.wdtFeed();
    scaleMgr.tareScale();
  }
}

void iotEventListener(int event, const char* msg) {
  if (strlen(msg) > 5) {
    ht1632c.print(msg, 50);
  }
  else {
    ht1632c.clearScreen();
    ht1632c.print(msg);
  }
}
