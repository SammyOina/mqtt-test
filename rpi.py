import datetime
import json
import os
import ssl
import time
import random

import jwt
import paho.mqtt.client as mqtt

from deps import *

# GCP parameters 
project_id = 'qualis-c1bec'  # Your project ID.
registry_id = 'speed-registry'  # Your registry name.
device_id = 'esp8266'  # Your device name.
private_key_file = 'ec_private.pem'  # Path to private key.
algorithm = 'ES256'  # Authentication key format.
cloud_region = 'us-central1'  # Project region.
ca_certs = 'roots.pem'  # CA root certificate path.
mqtt_bridge_hostname = 'mqtt.googleapis.com'  # GC bridge hostname.
mqtt_bridge_port = 8883  # Bridge port.
message_type = 'event'  # Message type (event or state).

           
def main():

    client = mqtt.Client(
        client_id='projects/{}/locations/{}/registries/{}/devices/{}'.format(
            project_id,
            cloud_region,
            registry_id,
            device_id))
    client.username_pw_set(
        username='unused',
        password=create_jwt(
            project_id,
            private_key_file,
            algorithm))
    client.tls_set(ca_certs=ca_certs, tls_version=ssl.PROTOCOL_TLSv1_2)

    device = Device()

    client.on_connect = device.on_connect
    client.on_publish = device.on_publish
    client.on_disconnect = device.on_disconnect
    client.on_subscribe = device.on_subscribe
    client.on_message = device.on_message
    client.connect(mqtt_bridge_hostname, mqtt_bridge_port)
    client.loop_start()

    mqtt_telemetry_topic = '/devices/{}/events'.format(device_id)
    mqtt_config_topic = '/devices/{}/config'.format(device_id)

    # Wait up to 5 seconds for the device to connect.
    device.wait_for_connection(5)

    client.subscribe(mqtt_config_topic, qos=1)
    
    num_message = 0
    try:        
        while num_message <= 10:
            speed = random.randint(0,80)
            if True:   
                currentTime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                num_message += 1
                # Form payload in JSON format.
                data = {
                    'num_message' : num_message,
                    'vehicle_Id': 1234,
                    'speed': speed,
                    'message': "Hello",
                    'time' : currentTime
                }
                payload = json.dumps(data, indent=4)
                print('Publishing payload', payload)
                client.publish(mqtt_telemetry_topic, payload, qos=1)  
            time.sleep(0.1)     

    except KeyboardInterrupt:
        # Exit script on ^C.
        pass
        client.disconnect()
        client.loop_stop()
        print('Exit with ^C. Goodbye!')
        

if __name__ == '__main__':
    main()