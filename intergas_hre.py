#!/usr/bin/env python
import argparse
import csv
import ctypes
import serial
import sys
import time
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
DEVICE_MANUFACTURER = "Intergas"

MQTT_BASE_TOPIC = f"boiler/{DEVICE_ID}"

SENSORS = {
    "flow_temp": {
        "name": "Flow Temperature",
        "device_class": "temperature",
        "unit_of_measurement": "°C",
        "state_class": "measurement"
    },
    "return_temp": {
        "name": "Return Temperature",
        "device_class": "temperature",
        "unit_of_measurement": "°C",
        "state_class": "measurement"
    },
    "dhw_temp": {
        "name": "Hot Water Temperature",
        "device_class": "temperature",
        "unit_of_measurement": "°C",
        "state_class": "measurement"
    },
    "temp_set": {
        "name": "Temperature Setpoint",
        "device_class": "temperature",
        "unit_of_measurement": "°C",
        "state_class": "measurement"
    },
    "fan_speed": {
        "name": "Burner Fan Speed",
        "device_class": "frequency",
        "unit_of_measurement": "rpm",
        "state_class": "measurement"
    },
    "status": {
        "name": "Status",
        "device_class": None,
        "unit_of_measurement": None,
        "state_class": None
    }
}

class MQTTHandler:
    def __init__(self, mqtt_user, mqtt_password):
        print("Initializing MQTT client...")
        self.client = mqtt.Client(
            client_id=MQTT_CLIENT_ID,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2
        )
        self.client.username_pw_set(mqtt_user, mqtt_password)

        # Set callbacks
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.setup_done = False
        self.client.will_set(f"{MQTT_BASE_TOPIC}/status", "offline", retain=True)
        self.reconnect_count = 0
        self.cached_values = {}  # Cache last published values to avoid sending out unnecessary messages to mqtt

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
        """Setup Home Assistant MQTT discovery"""
        device_info = {
            "identifiers": [DEVICE_ID],
            "name": DEVICE_NAME,
            "model": DEVICE_MODEL,
            "manufacturer": DEVICE_MANUFACTURER
        }

        # Register sensors
        for sensor_id, config in SENSORS.items():
            sensor_config = {
                "name": config['name'],
                "unique_id": sensor_id,
                "device_class": config['device_class'],
                "state_class": config['state_class'],
                "unit_of_measurement": config['unit_of_measurement'],
                "state_topic": f"{MQTT_BASE_TOPIC}/{sensor_id}/state",
                "device": device_info
            }

            self.client.publish(
                f"{MQTT_DISCOVERY_PREFIX}/sensor/{DEVICE_ID}/{sensor_id}/config",
                json.dumps(sensor_config),
                retain=True
            )

    def publish_data(self, data):
        """Publish boiler data to MQTT topics only when values change"""
        for key in SENSORS.keys():
            if key not in data:
                continue

            value = data[key]
            # print(f"Debug - Publishing {key}: {value}")
            topic = f"{MQTT_BASE_TOPIC}/{key}/state"

            current_state = str(value)
            if key not in self.cached_values or current_state != self.cached_values[key]:
                self.client.publish(topic, current_state)
                self.cached_values[key] = current_state

c_uint8 = ctypes.c_uint8

# Byte 27 flags
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

# Byte 29 flags
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

    t1 = getFloat(d[1],d[0])    # exhaust temperature (?)
    t2 = getFloat(d[3],d[2])    # flow temperature
    t3 = getFloat(d[5],d[4])    # return temperature
    t4 = getFloat(d[7],d[6])    # hot water temperature
    t5 = getFloat(d[9],d[8])    # boiler temperature (?)
    t6 = getFloat(d[11],d[10])  # outside temp (?)
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

    data = {
        'status': status,
        'flow_temp': round(t2, 1),
        'return_temp': round(t3, 1),
        'dhw_temp': round(t4, 1),
        'outside_temp': round(t6, 1),
        'pressure': ch_pressure,
        'temp_set': round(temp_set, 1),
        'fan_speed': fanspeed,
        'fan_speed_set': fanspeed_set,
        'fan_pwm': fan_pwm,
        'io_current': io_curr,
        'gp_switch': gp_switch,
        'tap_switch': tap_switch,
        'room_therm': roomtherm,
        'pump_active': pump,
        'dhw_active': dwk,
        'alarm_status': alarm_status,
        'ch_cascade_relay': ch_cascade_relay,
        'opentherm': opentherm,
        'flame_on': gasvalve,
        'spark': spark,
        'io_signal': io_signal,
        'ch_ot_disabled': ch_ot_disabled,
        'low_water_pressure': low_water_pressure,
        'pressure_sensor': pressure_sensor,
        'burner_block': burner_block,
        'grad_flag': grad_flag
    }

    return data

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
                    ser.write(b'S?\r')
                    s = ser.read(32)
                    if len(s) == 32:
                        data = parse_packet(s)
                        mqtt_handler.publish_data(data)
                        display_readings(data)
                    time.sleep(1)
        except serial.SerialException as e:
            print(f"Serial connection lost: {e}")
            time.sleep(10)
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
