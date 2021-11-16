
#include "AWSIOT.h"

AWSIOT::AWSIOT(MessageHandler messageHandler, EventListenerFunction eventListener){
  this->messageHandler = messageHandler;
  this->eventListener = eventListener;
}

void AWSIOT::lwMQTTErr(lwmqtt_err_t reason)
{
  if (reason == lwmqtt_err_t::LWMQTT_SUCCESS)
    Serial.print("Success");
  else if (reason == lwmqtt_err_t::LWMQTT_BUFFER_TOO_SHORT)
    Serial.print("Buffer too short");
  else if (reason == lwmqtt_err_t::LWMQTT_VARNUM_OVERFLOW)
    Serial.print("Varnum overflow");
  else if (reason == lwmqtt_err_t::LWMQTT_NETWORK_FAILED_CONNECT)
    Serial.print("Network failed connect");
  else if (reason == lwmqtt_err_t::LWMQTT_NETWORK_TIMEOUT)
    Serial.print("Network timeout");
  else if (reason == lwmqtt_err_t::LWMQTT_NETWORK_FAILED_READ)
    Serial.print("Network failed read");
  else if (reason == lwmqtt_err_t::LWMQTT_NETWORK_FAILED_WRITE)
    Serial.print("Network failed write");
  else if (reason == lwmqtt_err_t::LWMQTT_REMAINING_LENGTH_OVERFLOW)
    Serial.print("Remaining length overflow");
  else if (reason == lwmqtt_err_t::LWMQTT_REMAINING_LENGTH_MISMATCH)
    Serial.print("Remaining length mismatch");
  else if (reason == lwmqtt_err_t::LWMQTT_MISSING_OR_WRONG_PACKET)
    Serial.print("Missing or wrong packet");
  else if (reason == lwmqtt_err_t::LWMQTT_CONNECTION_DENIED)
    Serial.print("Connection denied");
  else if (reason == lwmqtt_err_t::LWMQTT_FAILED_SUBSCRIPTION)
    Serial.print("Failed subscription");
  else if (reason == lwmqtt_err_t::LWMQTT_SUBACK_ARRAY_OVERFLOW)
    Serial.print("Suback array overflow");
  else if (reason == lwmqtt_err_t::LWMQTT_PONG_TIMEOUT)
    Serial.print("Pong timeout");
  Serial.println();
}

void AWSIOT::lwMQTTErrConnection(lwmqtt_return_code_t reason)
{
  if (reason == lwmqtt_return_code_t::LWMQTT_CONNECTION_ACCEPTED)
    Serial.print("Connection Accepted");
  else if (reason == lwmqtt_return_code_t::LWMQTT_UNACCEPTABLE_PROTOCOL)
    Serial.print("Unacceptable Protocol");
  else if (reason == lwmqtt_return_code_t::LWMQTT_IDENTIFIER_REJECTED)
    Serial.print("Identifier Rejected");
  else if (reason == lwmqtt_return_code_t::LWMQTT_SERVER_UNAVAILABLE)
    Serial.print("Server Unavailable");
  else if (reason == lwmqtt_return_code_t::LWMQTT_BAD_USERNAME_OR_PASSWORD)
    Serial.print("Bad UserName/Password");
  else if (reason == lwmqtt_return_code_t::LWMQTT_NOT_AUTHORIZED)
    Serial.print("Not Authorized");
  else if (reason == lwmqtt_return_code_t::LWMQTT_UNKNOWN_RETURN_CODE)
    Serial.print("Unknown Return Code");
  Serial.println();
}

void AWSIOT::connectToMqtt(bool nonBlocking)
{
  Serial.print("MQTT connecting ");
  while (!client.connected())
  {
    ESP.wdtFeed();
    
    if (client.connect(THINGNAME))
    {
      ESP.wdtFeed();

      // subscribe to topics
      client.subscribe(AWS_IOT_SUBSCRIBE_TOPIC);
      client.subscribe(AWS_IOT_SHADOW_GET_ACCEPTED_SUB);
      client.subscribe(AWS_IOT_SHADOW_UPDATE_DELTA_SUB);
      Serial.println("MQTT connected!");
      if (eventListener)
        eventListener(AWS_IOT_EVENT_CONNECTED_AWS, "Connected!");

      // initialize settings for scale
      client.publish(AWS_IOT_SHADOW_GET_PUB, "");
    }
    else
    {
      ESP.wdtFeed();
      Serial.print("failed, reason -> ");
      lwMQTTErrConnection(client.returnCode());
      lwMQTTErr(client.lastError());

      if (!nonBlocking)
      {
        Serial.println(" < try again in 2 seconds");
        delay(2000);
        ESP.wdtFeed();
      }
      else
      {
        Serial.println(" <");
      }
    }
    if (nonBlocking)
      break;
  }
}

void AWSIOT::connectToWiFi(String init_str)
{
  if (init_str != emptyString)
    Serial.print(init_str);
  while (WiFi.status() != WL_CONNECTED)
  {
    Serial.print(".");
    delay(1000);
    ESP.wdtFeed();
  }
  if (init_str != emptyString) {
    Serial.println();
    Serial.println("ok!");
  }
}

void AWSIOT::checkWiFiThenMQTT(void)
{
  connectToWiFi("Checking WiFi");
  connectToMqtt();
}


void AWSIOT::initNetwork()
{
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

  Serial.print("Connecting to Wi-Fi");
  if (eventListener)
    eventListener(AWS_IOT_EVENT_CONNECTING_WIFI, "Wi-Fi");

  while (WiFi.status() != WL_CONNECTED){
    delay(500);
    Serial.print(".");
    ESP.wdtFeed();
  }
  Serial.println();

  // configure date
  if (!dateSet) {
    Serial.println("Updating date/time");
    DateTime.setTimeZone("EST");
    DateTime.begin();
    ESP.wdtFeed();
    if (!DateTime.isTimeValid()) {
        Serial.println("Failed to get time from server.");
        Serial.println(DateTime.toString());

        if (eventListener)
          eventListener(AWS_IOT_EVENT_FAILURE_DATE, "Date");

        delay(2000);
        ESP.restart();
    }
    Serial.println(DateTime.toString());
    dateSet = true;
  }
}

void AWSIOT::connectAWS()
{
  Serial.println("Connecting to AWS IOT");
  if (eventListener)
    eventListener(AWS_IOT_EVENT_CONNECTING_AWS, "AWS");
 
  // Configure WiFiClientSecure to use the AWS IoT device credentials
  net.setTrustAnchors(&aws_ca);
  net.setClientRSACert(&client_cert, &client_key);

  // Connect to the MQTT broker on the AWS endpoint we defined earlier
  client.setKeepAlive(50);
  client.setTimeout(4000);
  client.begin(AWS_IOT_ENDPOINT, 8883, net);

  // Create a message handler
  client.onMessage(this->messageHandler);
  Serial.println("AWS IoT Connected!");
}

void AWSIOT::publishShadowConfigToAWS(StaticJsonDocument<512> doc)
{
  Serial.println("Sending shadow doc -");
  serializeJson(doc, Serial);
  Serial.println();
  
  ESP.wdtFeed();
  char jsonBuffer[4096];
  serializeJson(doc, jsonBuffer);
  
  client.publish(AWS_IOT_SHADOW_UPDATE_PUB, jsonBuffer);
  ESP.wdtFeed();
    
  Serial.println("Published to AWS");
}

void AWSIOT::publishWeightToAWS(float weight, bool tare)
{
  if (weight != lastPublishedWeight) {
    Serial.print("Measured weight: ");
    Serial.print(weight, 1);
    Serial.print(" lbs, tare -> ");
    Serial.println(tare);
  
    StaticJsonDocument<200> doc;
    doc["time"] = DateTime.toUTCString();  
    doc["weight"] = weight;
    doc["tare"] = tare;
    char jsonBuffer[512];
    serializeJson(doc, jsonBuffer);
  
    client.publish(AWS_IOT_PUBLISH_TOPIC, jsonBuffer);
    ESP.wdtFeed();

    lastPublishedWeight = weight;
  }
}

void AWSIOT::setup()
{
  initNetwork();
  ESP.wdtFeed();
  connectAWS();
  ESP.wdtFeed();
  connectToMqtt();
  ESP.wdtFeed();
}

void AWSIOT::loop()
{
  if(!client.connected()){
    Serial.println("Not connected...");
    lwMQTTErr(client.lastError());
    lwMQTTErrConnection(client.returnCode());
    
    Serial.println("AWS IoT Failure!");
    if (eventListener)
      eventListener(AWS_IOT_EVENT_FAILURE_MQTT, "AWS IoT Failure!");
    delay(1000);
    checkWiFiThenMQTT();
  }
  else {
    client.loop();
    ESP.wdtFeed();
  }
}
