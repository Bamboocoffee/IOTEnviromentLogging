import paho.mqtt.client as mqtt
import Adafruit_DHT
import bluetooth
import time
import datetime

# Adafruit settings
sensor = 11
pin = 4

# Sensor settings
sensor_ID = "stephen_keenan"

# Bluetooth settings
bd_addr = "B8:27:EB:3A:A4:F0"
port = 1

# MQTT settings
broker_address="192.168.0.39"

while True:

    #print("USING MQTT")
    client = mqtt.Client("Keenan")
    client.connect(broker_address)
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

        try:
            date_time = datetime.datetime.now()
            second = date_time.second

            # Get data from sensor every 5 seconds and send to broker
            if second % 5 == 0:

                humidity, temp = Adafruit_DHT.read_retry(sensor, pin)
                print("PROTOCOL - MQTT, sending data to broker")
                client.publish("sensor_readings", "{} Protocol=MQTT, SensorID={}, Temperature={}, Humidity={}".format(date_time.strftime('%Y-%m-%d %H:%M:%S.%f'), sensor_ID, temp, humidity))

                time.sleep(1)

        except Exception as e:
            print(e)

        now = datetime.datetime.now()
        time_elapsed = (now - connection_open_time).total_seconds()
        timestamp = now.timestamp()

    try:
        client.disconnect()

    except:
        pass

    #print("Switched Protocol")

    connection_open_time = datetime.datetime.now()
    tm = connection_open_time

    # Round down to nearest 2 minute (120 seconds) to ensure synchronicity
    connection_open_time = tm - datetime.timedelta(minutes=tm.minute %2,
                                                    seconds=tm.second,
                                                    microseconds=tm.microsecond)
    now = datetime.datetime.now()
    time_elapsed = (now - connection_open_time).total_seconds()

    while True:
        try:
            # Open bluetooth socket
            sock=bluetooth.BluetoothSocket( bluetooth.RFCOMM )

            # Try connect to host, keep trying until connected
            sock.connect((bd_addr, port))
            break

        except Exception as e:
            print(e)
            time.sleep(1)

    # Loop that lasts 120 seconds for the RFCOMM protocol
    while True:

        # Break when 2 minutes has elapsed
        if time_elapsed >= 120:
            break

        try:
            date_time = datetime.datetime.now()
            second = date_time.second

            # Get data from sensor every 5 seconds and send to broker
            if second % 5 == 0:

                humidity, temp = Adafruit_DHT.read_retry(sensor, pin)
                print("PROTOCOL - BLUETOOTH, sending data to server")
                sock.send("{} Protocol=RFCOMM, SensorID={}, Temperature={}, Humidity={}".format(date_time.strftime('%Y-%m-%d %H:%M:%S.%f'), sensor_ID, temp, humidity))

                time.sleep(1)

        except Exception as e:
            print(e)

        now = datetime.datetime.now()
        time_elapsed = (now - connection_open_time).total_seconds()


    try:
        # Close connection until needed again
        sock.close()

    except:
        pass
