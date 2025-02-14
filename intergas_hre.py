#!/usr/bin/env python
import argparse
import csv
import ctypes
import glob
import serial
import sys
import time
import os
import json
import paho.mqtt.client as mqtt
from struct import *

# MQTT Settings
MQTT_BROKER = "homeassistant.local"
MQTT_PORT = 1883
MQTT_CLIENT_ID = "intergas_boiler"
MQTT_DISCOVERY_PREFIX = "homeassistant"

DEVICE_ID = "intergas_boiler"
DEVICE_NAME = "Intergas Boiler"
DEVICE_MODEL = "Kombi Kompakt HRE 36/30"


class MQTTHandler:
    def __init__(self, mqtt_user, mqtt_password):
        print("Initializing MQTT client...")
        # Create MQTT client with explicit API version
        self.client = mqtt.Client(
            client_id=MQTT_CLIENT_ID,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2
        )
        self.client.username_pw_set(mqtt_user, mqtt_password)

        # Set callbacks
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.setup_done = False
        self.client.will_set(f"intergas/{DEVICE_ID}/status", "offline", retain=True)
        self.reconnect_count = 0

    def connect(self):
        try:
            self.client.connect(MQTT_BROKER, MQTT_PORT)
            self.client.loop_start()
        except Exception as e:
            print(f"MQTT Connection failed: {str(e)}")
            sys.exit(1)

    def on_connect(self, client, userdata, flags, rc, something):
        print(f"Connected to MQTT broker with result code {rc}")
        if not self.setup_done:
            self.setup_discovery()
            self.setup_done = True

    def on_disconnect(self, client, userdata, rc):
        self.reconnect_count += 1
        print(f"Disconnected from MQTT broker (attempt {self.reconnect_count})")
        time.sleep(min(self.reconnect_count * 5, 30))  # Exponential backoff
        self.client.connect(MQTT_BROKER, MQTT_PORT)

    def setup_discovery(self):
        """Set up Home Assistant MQTT discovery for the boiler device"""
        device_info = {
            "identifiers": [DEVICE_ID],
            "name": DEVICE_NAME,
            "model": DEVICE_MODEL,
            "manufacturer": "Intergas",
        }

        # Main climate control
        climate_config = {
            "name": f"{DEVICE_NAME} Climate",
            "unique_id": f"{DEVICE_ID}_climate",
            "device": device_info,
            "current_temperature_topic": f"intergas/{DEVICE_ID}/flow_temp/state",
            "temperature_state_topic": f"intergas/{DEVICE_ID}/temp_set/state",
            "status_topic": f"intergas/{DEVICE_ID}/status/state",
            "temperature_unit": "C",
            "modes": ["heat", "off"],
        }
        self.client.publish(
            f"{MQTT_DISCOVERY_PREFIX}/climate/{DEVICE_ID}/config",
            json.dumps(climate_config),
            retain=True
        )

        # Individual sensors
        sensors = {
            "temp_setpoint": {"name": "Temperature Setpoint", "unit": "°C", "device_class": "temperature"},
            "flow_temp": {"name": "Flow Temperature", "unit": "°C", "device_class": "temperature"},
            "return_temp": {"name": "Return Temperature", "unit": "°C", "device_class": "temperature"},
            "dhw_temp": {"name": "DHW Temperature", "unit": "°C", "device_class": "temperature"},
            # "outside_temp": {"name": "Outside Temperature", "unit": "°C", "device_class": "temperature"},
            # "pressure": {"name": "System Pressure", "unit": "bar", "device_class": "pressure"},
            "fan_speed": {"name": "Fan Speed", "unit": "RPM"},
        }

        for sensor_id, config in sensors.items():
            sensor_config = {
                "name": f"{DEVICE_NAME} {config['name']}",
                "unique_id": f"{DEVICE_ID}_{sensor_id}",
                "state_topic": f"intergas/{DEVICE_ID}/{sensor_id}/state",
                "unit_of_measurement": config["unit"],
                "device": device_info,
            }
            if "device_class" in config:
                sensor_config["device_class"] = config["device_class"]

            self.client.publish(
                f"{MQTT_DISCOVERY_PREFIX}/sensor/{DEVICE_ID}_{sensor_id}/config",
                json.dumps(sensor_config),
                retain=True
            )

    def publish_data(self, data):
        """Publish boiler data to MQTT topics"""
        base_topic = f"intergas/{DEVICE_ID}"

        # Publish sensor values
        self.client.publish(f"{base_topic}/temp_setpoint/state", f"{data['temp_set']:.1f}")
        self.client.publish(f"{base_topic}/flow_temp/state", f"{data['flow_temp']:.1f}")
        self.client.publish(f"{base_topic}/return_temp/state", f"{data['return_temp']:.1f}")
        self.client.publish(f"{base_topic}/dhw_temp/state", f"{data['dhw_temp']:.1f}")
        # self.client.publish(f"{base_topic}/outside_temp/state", f"{data['outside_temp']:.1f}")
        # self.client.publish(f"{base_topic}/pressure/state", f"{data['pressure']:.1f}")
        self.client.publish(f"{base_topic}/fan_speed/state", f"{data['fan_speed']:.0f}")
        self.client.publish(f"{base_topic}/status/state", data['status'])

c_uint8 = ctypes.c_uint8

class B27Flags_bits(ctypes.LittleEndianStructure):
    _fields_ = [
            ("gp_switch", c_uint8, 1),
            ("tap_switch", c_uint8, 1),
            ("roomtherm", c_uint8, 1),
            ("pump", c_uint8, 1),
            ("dwk", c_uint8, 1),
            ("alarm_status", c_uint8, 1),
            ("ch_cascade_relay", c_uint8, 1),
            ("opentherm", c_uint8, 1),
        ]

class B27Flags(ctypes.Union):
    _fields_ = [("b", B27Flags_bits),
                ("asbyte", c_uint8)]

class B29Flags_bits(ctypes.LittleEndianStructure):
    _fields_ = [
            ("gasvalve", c_uint8, 1),
            ("spark", c_uint8, 1),
            ("io_signal", c_uint8, 1),
            ("ch_ot_disabled", c_uint8, 1),
            ("low_water_pressure", c_uint8, 1),
            ("pressure_sensor", c_uint8, 1),
            ("burner_block", c_uint8, 1),
            ("grad_flag", c_uint8, 1),
        ]

class B29Flags(ctypes.Union):
    _fields_ = [("b", B29Flags_bits),
                ("asbyte", c_uint8)]

def parse_packet(s):
    # Convert bytes to list of integers if needed
    if isinstance(s, bytes):
        d = list(s)
    else:
        d = list(map(ord, unpack('=cccccccccccccccccccccccccccccccc', s)))

    def getFloat(msb, lsb):
        if msb > 127:
            f = -(float(msb ^ 255) + 1) * 256 - lsb / 100
        else:
            f = float(msb * 265 + lsb) / 100
        return f

    t1 = getFloat(d[1],d[0])  # Rookgassensor (?)
    t2 = getFloat(d[3],d[2])  # Aanvoersensor S1
    t3 = getFloat(d[5],d[4])  # Retoursensor S2
    t4 = getFloat(d[7],d[6])  # Warmwatersensor S3
    t5 = getFloat(d[9],d[8])  # Boilersensor S4
    t6 = getFloat(d[11],d[10])  # buitenvoeler (?)
    ch_pressure = getFloat(d[13],d[12])
    temp_set = getFloat(d[15],d[14])
    fanspeed_set = getFloat(d[17],d[16]) * 100
    fanspeed = getFloat(d[19],d[18]) * 100
    fan_pwm = getFloat(d[21],d[20])
    io_curr = getFloat(d[23],d[22])

    flags = B27Flags()
    flags.asbyte = d[27]
    gp_switch = flags.b.gp_switch
    tap_switch = flags.b.tap_switch
    roomtherm = flags.b.roomtherm
    pump = flags.b.pump
    dwk = flags.b.dwk
    alarm_status = flags.b.alarm_status
    ch_cascade_relay = flags.b.ch_cascade_relay
    opentherm = flags.b.opentherm

    B29flags = B29Flags()
    B29flags.asbyte = d[29]
    gasvalve = B29flags.b.gasvalve
    spark = B29flags.b.spark
    io_signal = B29flags.b.io_signal
    ch_ot_disabled = B29flags.b.ch_ot_disabled
    low_water_pressure = B29flags.b.low_water_pressure
    pressure_sensor = B29flags.b.pressure_sensor
    burner_block = B29flags.b.burner_block
    grad_flag = B29flags.b.grad_flag

    ch_pressure = None
    if not B29flags.b.pressure_sensor:
        ch_pressure = -35
    displ_code = d[24]

    # Add status code interpretation
    status_codes = {
        51: "Hot water",
        102: "CV Brandt",
        126: "Idle",
        204: "Recirculate tap water",
        231: "Recirculate heating water",
    }
    status = status_codes.get(displ_code, f"Unknown ({displ_code})")

    return {
        'status': status,
        'flow_temp': t2,
        'return_temp': t3,
        'dhw_temp': t4,
        'outside_temp': t6,
        'pressure': ch_pressure,
        'temp_set': temp_set,
        'fan_speed': fanspeed,
        'fan_speed_set': fanspeed_set,
        'pump_active': pump,
        'flame_on': gasvalve,
        'opentherm': opentherm,
        'raw_values': [t1, t2, t3, t4, t5, t6, ch_pressure, temp_set, fanspeed_set, fanspeed, fan_pwm,
                      io_curr, gp_switch, tap_switch, roomtherm, pump, dwk, alarm_status, ch_cascade_relay, opentherm,
                      gasvalve, spark, io_signal, ch_ot_disabled, low_water_pressure, pressure_sensor, burner_block, grad_flag,
                      ch_pressure]
    }

def parse_file(csvfile):
    with open(csvfile, "r") as fh:
        reader = csv.reader(fh, delimiter=";", lineterminator='\n')
        for row in reader:
            # Update to handle base64 encoding in Python 3
            import base64
            pkt = parse_packet(base64.b64decode(row[1]))
            raw_values = pkt['raw_values']
            print(" ".join(map(str, [row[0]] + raw_values)))


def display_readings(data):
    """Display current boiler readings in a readable format"""
    print("\033[2J\033[H")  # Clear screen and move cursor to top
    print(f"Status: {data['status']}")
    print(f"Flow Temperature: {data['flow_temp']:.1f}°C")
    print(f"Return Temperature: {data['return_temp']:.1f}°C")
    print(f"DHW Temperature: {data['dhw_temp']:.1f}°C")
    # print(f"Outside Temperature: {data['outside_temp']:.1f}°C")
    print(f"System Pressure: {data['pressure']:.1f} bar")
    print(f"Temperature Setpoint: {data['temp_set']:.1f}°C")
    print(f"OpenTherm: {data['opentherm']:.0f}")
    print(f"Fan Speed: {data['fan_speed']:.0f} rpm (set: {data['fan_speed_set']:.0f} rpm)")
    print(f"Pump: {'ON' if data['pump_active'] else 'OFF'}")
    print(f"Flame: {'ON' if data['flame_on'] else 'OFF'}")
    print("\nPress Ctrl+C to stop...")

def get_packet(port, mqtt_user, mqtt_password):
    mqtt_handler = MQTTHandler(mqtt_user, mqtt_password)
    mqtt_handler.connect()

    while True:  # Add outer reconnection loop
        try:
            with serial.Serial(port, 9600, timeout=2) as ser:
                print(f"Connected to {port}")
                while True:
                    ts = time.time()
                    ser.write(b'S?\r')
                    s = ser.read(32)
                    if len(s) == 32:
                        data = parse_packet(s)
                        mqtt_handler.publish_data(data)
                        display_readings(data)
                    time.sleep(1)
        except serial.SerialException as e:
            print(f"Serial connection lost: {e}")
            time.sleep(10)  # Wait before retry
            continue
        except Exception as e:
            print(f"Unexpected error: {e}")
            time.sleep(10)
            continue

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Intergas boiler reader')
    parser.add_argument('--mqtt-user', required=True, help='MQTT username')
    parser.add_argument('--mqtt-password', required=True, help='MQTT password')
    parser.add_argument('--port', required=True, help='Serial port')
    args = parser.parse_args()

    get_packet(args.port, args.mqtt_user, args.mqtt_password)
