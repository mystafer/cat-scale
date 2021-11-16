
#ifndef AWSIOT_h
#define AWSIOT_h

#include "secrets.h"
#include <WiFiClientSecure.h>
#include <MQTT.h>
#include <ArduinoJson.h>
#include <ESP8266WiFi.h>
#include <ESPDateTime.h>


// The MQTT topics that this device should publish/subscribe
#define AWS_IOT_PUBLISH_TOPIC   "catScale/pub"
#define AWS_IOT_SUBSCRIBE_TOPIC "catScale/sub"

#define SHADOWNAME "CatScaleConfig"
#define AWS_IOT_SHADOW_TOPIC_BASE    "$aws/things/Cat-Scale-Firebeetle8266/shadow/name/" + (String) SHADOWNAME
#define AWS_IOT_SHADOW_GET_PUB (String) AWS_IOT_SHADOW_TOPIC_BASE + "/get"
#define AWS_IOT_SHADOW_UPDATE_PUB (String) AWS_IOT_SHADOW_TOPIC_BASE + "/update"
#define AWS_IOT_SHADOW_GET_ACCEPTED_SUB (String) AWS_IOT_SHADOW_TOPIC_BASE + "/get/accepted"
#define AWS_IOT_SHADOW_UPDATE_DELTA_SUB (String) AWS_IOT_SHADOW_TOPIC_BASE + "/update/delta"

#define AWS_IOT_EVENT_CONNECTING_WIFI 1
#define AWS_IOT_EVENT_CONNECTING_AWS 2
#define AWS_IOT_EVENT_CONNECTED_AWS 3
#define AWS_IOT_EVENT_FAILURE_MQTT 4
#define AWS_IOT_EVENT_FAILURE_DATE 5

typedef void (*MessageHandler)(String &topic, String &payload);
typedef void (*EventListenerFunction) (int event, const char* msg);

class AWSIOT{
  public:
    AWSIOT(MessageHandler messageHandler, EventListenerFunction eventListener);
    
    void setup();
    void loop();
    void publishShadowConfigToAWS(StaticJsonDocument<512> doc);
    void publishWeightToAWS(float weight, bool tare);
    
  private:
    void lwMQTTErr(lwmqtt_err_t reason);
    void lwMQTTErrConnection(lwmqtt_return_code_t reason);
    
    void connectToMqtt(bool nonBlocking = false);
    void connectToWiFi(String init_str);
    void checkWiFiThenMQTT(void);
    
    void initNetwork();
    void connectAWS();
  
    MQTTClient client = MQTTClient(4096);
    
    WiFiClientSecure net = WiFiClientSecure();
    BearSSL::X509List aws_ca = BearSSL::X509List(AWS_CERT_CA);
    BearSSL::X509List client_cert = BearSSL::X509List(AWS_CERT_CRT);
    BearSSL::PrivateKey client_key = BearSSL::PrivateKey(AWS_CERT_PRIVATE);
    
    bool dateSet = false;
    MessageHandler messageHandler;
    EventListenerFunction eventListener;

    float lastPublishedWeight = -1.0 / 0.0;
};

#endif
