import bluetooth
import Adafruit_DHT
import logging
import paho.mqtt.client as mqtt
import requests
import json
import time
import datetime
from r7insight import R7InsightHandler

# Callback methods for MQTT
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

def on_message(client, userdata, message):
    log.info(message.payload.decode("utf-8") + " Weather_Summary={}, Outdoor_Temp={}, Outdoor_Humidity={}".format(summary, outdoor_temp, outdoor_humidity))
    print("Message received (MQTT) and logged \n" + str(message.payload.decode("utf-8")) +
            " Weather_Summary={}, Outdoor_Temp={}, Outdoor_Humidity={}\n".format(summary, outdoor_temp, outdoor_humidity))
    #print("message topic=",message.topic)
    #print("message qos=",message.qos)
    #print("message retain flag=",message.retain)

# Set up logging to Rapid7
log = logging.getLogger('r7insight')
log.setLevel(logging.INFO)

# Stephen G R7 log
handler = R7InsightHandler('d405ebb0-14bd-43a5-a9d3-1dafebce53a6', 'eu')

# Stephen K R7 log
#handler = R7InsightHandler('8d4816d7-026a-48fb-845a-eddf5808ac5b', 'eu')

# Add custom handler with log token here
# handler = R7InsightHandler('XXXXX', 'eu')

log.addHandler(handler)


# Adafruit settings
sensor = 11
pin = 4

# Sensor settings
sensor_ID = "stephen_gaffney"

# MQTT settings
broker_address="localhost"

# Get current weather data
# Call once but in pratice call hourly to update the current weather
try:
    api = ('https://api.darksky.net/forecast/98cdd61d77bab4d8d739f78b33'
              'e06c30/53.3498,-6.2603?units=si')

    current_weather_data = requests.get(api)
    current_weather_data = json.loads(current_weather_data.text)

    summary = current_weather_data["currently"]["summary"]
    outdoor_temp = current_weather_data["currently"]["temperature"]
    outdoor_humidity = current_weather_data["currently"]["humidity"] * 100

except:
    summary = "NaN"
    outdoor_temp = "NaN"
    outdoor_humidity = "NaN"

# Infinite loop that changes between protocols every 2 minutes (12 hours)
while True:

    #print("USING MQTT PROTOCOL")
    client = mqtt.Client("Gaffney") #create new instance
    client.on_message=on_message #attach function to callback
    client.connect(broker_address) #connect to broker
    client.subscribe("sensor_readings")

    connection_open_time = datetime.datetime.now()
    tm = connection_open_time

    # Round down to nearest 2 minute (120 seconds) to ensure synchronicity
    connection_open_time = tm - datetime.timedelta(minutes=tm.minute % 2,
                                                    seconds=tm.second,
                                                    microseconds=tm.microsecond)

    now = datetime.datetime.now()
    time_elapsed = (now - connection_open_time).total_seconds()

    # Loop that lasts 120 seconds for the MQTT protocol
    while True:

        # Break when 2 minutes has elapsed
        if time_elapsed >= 120:
            break

        client.loop_start()

        try:
            date_time = datetime.datetime.now()
            second = date_time.second

            # Get data from own sensor and log every 5 seconds
            if second % 5 == 0:

                humidity, temp = Adafruit_DHT.read_retry(sensor, pin)
                log.info("{} Protocol=MQTT, SensorID={}, Temperature={}, Humidity={} Weather_Summary={}, Outdoor_Temp={}, Outdoor_Humidity={}".format(date_time.strftime('%Y-%m-%d %H:%M:%S.%f'), sensor_ID, temp, humidity, summary, outdoor_temp, outdoor_humidity))
                print("LOGGED OWN DATA \n{} Protocol=MQTT, SensorID={}, Temperature={}, Humidity={} Weather_Summary={}, Outdoor_Temp={}, Outdoor_Humidity={}\n".format(date_time.strftime('%Y-%m-%d %H:%M:%S.%f'), sensor_ID, temp, humidity, summary, outdoor_temp, outdoor_humidity))
                time.sleep(1)

        except Exception as e:
            print(e)

        now = datetime.datetime.now()
        time_elapsed = (now - connection_open_time).total_seconds()

    try:
        client.loop_stop()

    except:
        pass

    # Switch to bluetooth after 120seconds

    #print("USING BLUETOOTH")
    connection_open_time = datetime.datetime.now()
    tm = connection_open_time
    # Round down to nearest 2 minute (120 seconds) to ensure synchronicity
    connection_open_time = tm - datetime.timedelta(minutes=tm.minute %2,
                                                    seconds=tm.second,
                                                    microseconds=tm.microsecond)
    now = datetime.datetime.now()
    time_elapsed = (now - connection_open_time).total_seconds()


    # Set up server socket on port 1
    server_sock=bluetooth.BluetoothSocket( bluetooth.RFCOMM )
    port = 1
    server_sock.bind(("",port))

    server_sock.listen(1)
    # Open socket with client
    client_sock,address = server_sock.accept()

    # Loop that lasts 120 seconds for thee RFCOMM protocol
    while True:

        # Break when 2 minutes has elapsed
        if time_elapsed >= 120:
            break

        try:
            date_time = datetime.datetime.now()
            second = date_time.second

            # Get data from own sensor and log every 5 seconds
            if second % 5 == 0:

                humidity, temp = Adafruit_DHT.read_retry(sensor, pin)
                log.info("{} Protocol=RFCOMM, SensorID={}, Temperature={}, Humidity={} Weather_Summary={}, Outdoor_Temp={}, Outdoor_Humidity={}".format(date_time.strftime('%Y-%m-%d %H:%M:%S.%f'), sensor_ID, temp, humidity, summary, outdoor_temp, outdoor_humidity))
                print("LOGGED OWN DATA \n{} Protocol=MQTT, SensorID={}, Temperature={}, Humidity={} Weather_Summary={}, Outdoor_Temp={}, Outdoor_Humidity={}\n".format(date_time.strftime('%Y-%m-%d %H:%M:%S.%f'), sensor_ID, temp, humidity, summary, outdoor_temp, outdoor_humidity))

                data = client_sock.recv(1024)
                data = data.decode()
                log.info(data + " Weather_Summary={}, Outdoor_Temp={}, Outdoor_Humidity={}".format(summary, outdoor_temp, outdoor_humidity))
                print("Message retrieved (RFCOMM) and logged \n" + data + " Weather_Summary={}, Outdoor_Temp={}, Outdoor_Humidity={}\n".format(summary, outdoor_temp, outdoor_humidity))

        except Exception as e:
             print(e)

        now = datetime.datetime.now()
        time_elapsed = (now - connection_open_time).total_seconds()

    try:
        client_sock.close()

    except Exception as e:
        print(e)

    try:
        server_sock.close()

    except Exception as e:
        print(e)
