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
    "hot_water_temp": {
        "name": "Hot Water Temperature",
        "device_class": "temperature",
        "unit_of_measurement": "°C",
        "state_class": "measurement"
    },
    "temp_setpoint": {
        "name": "Temperature Setpoint",
        "device_class": "temperature",
        "unit_of_measurement": "°C",
        "state_class": "measurement"
    },
    "fan_speed": {
        "name": "Burner Fan Speed",
        "device_class": "speed",
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

def parse_packet(s):
    # Convert bytes to list of integers if needed
    if isinstance(s, bytes):
        d = list(s)
    else:
        d = list(map(ord, unpack('=cccccccccccccccccccccccccccccccc', s)))

    def convert_to_signed_word(msb, lsb):
        """Convert MSB/LSB bytes to signed 16-bit integer"""
        word = (msb << 8 | lsb)
        # Convert to signed 16-bit
        if word > 32767:
            word -= 65536
        return word

    def getFloat(msb, lsb):
        word = convert_to_signed_word(msb, lsb)
        return float(word) / 100.0

    def getTemp(msb, lsb):
        word = convert_to_signed_word(msb, lsb)
        if word <= -5100 or word == 32767:  # 32767 is SHRT_MAX
            # Intergas gives -5100 for disconnected sensors
            return float('nan')

        return float(word) / 100.0

    def getInt(msb, lsb):
        word = convert_to_signed_word(msb, lsb)
        return int(word)

    def get_bool(data, bit):
        return bool(data & (1 << bit))

    t1 = getTemp(d[1],d[0])    # heat exchanger temperature
    t2 = getTemp(d[3],d[2])    # flow temperature
    t3 = getTemp(d[5],d[4])    # return temperature
    t4 = getTemp(d[7],d[6])    # hot water temperature
    t5 = getTemp(d[9],d[8])    # boiler temperature (?)
    t6 = getTemp(d[11],d[10])  # outside temp (?)
    water_pressure = getFloat(d[13],d[12])
    temp_setpoint = getFloat(d[15],d[14])
    fanspeed_set = getInt(d[17],d[16])
    fanspeed = getInt(d[19],d[18])
    fan_pwm = getFloat(d[21],d[20])
    ionisation_current = getFloat(d[23],d[22])
    displ_code = d[24]

    # TO-DO: What's on byte 25?

    # bit flags from byte 26
    gp_switch = get_bool(d[26], 0)
    tap_switch = get_bool(d[26], 1)
    roomtherm = get_bool(d[26], 2)
    pump = get_bool(d[26], 3)
    three_way_valve = get_bool(d[26], 4)
    alarm_status = get_bool(d[26], 5)
    ch_cascade_relay = get_bool(d[26], 6)
    opentherm = get_bool(d[26], 7)

    # TO-DO: What are the flags for byte 27 beyond fault code?

    # bit flags from byte 28
    gas_valve = get_bool(d[28], 0)
    spark = get_bool(d[28], 1)
    ionisation_signal = get_bool(d[28], 2)
    opentherm_disabled = get_bool(d[28], 3)
    low_water_pressure = get_bool(d[28], 4)
    pressure_sensor = get_bool(d[28], 5)
    burner_block = get_bool(d[28], 6)
    gradient_flag = get_bool(d[28], 7)

    if not pressure_sensor:
        water_pressure = float('nan')

    if get_bool(d[27], 7):
        # last known fault code
        fault_code = prettify_fault_code(d[29])
    else:
        fault_code = "None"

    # Add status code interpretation
    status_codes = {
        51: "Recirculating tap water",
        0: "Central Heating active (1)",
        102: "Central Heating active (2)",
        126: "Idle",
        170: "Service mode",
        204: "Hot water active",
        231: "Central Heating ramp down",
    }
    status = status_codes.get(displ_code, f"Unknown ({displ_code})")

    data = {
        'status': status,
        'temp_setpoint': round(temp_setpoint, 1),
        'flow_temp': round(t2, 1),
        'return_temp': round(t3, 1),
        'hot_water_temp': round(t4, 1),
        'heat_exchanger_temp': round(t1, 1),
        'outside_temp': round(t6, 1),
        't5': round(t5, 1),
        'fan_speed': fanspeed,
        'fan_speed_setpoint': fanspeed_set,
        'fan_pwm': fan_pwm,
        'pump_active': pump,
        'gas_valve': gas_valve,
        'spark': spark,
        'ionisation_current': ionisation_current,
        'alarm_status': alarm_status,
        'fault_code': fault_code,
        'pressure_sensor': pressure_sensor,
        'pressure': water_pressure,
        'low_water_pressure': low_water_pressure,
        'gp_switch': gp_switch,
        'tap_switch': tap_switch,
        'opentherm': opentherm,
        'room_thermostat': roomtherm,
        'three_way_valve': three_way_valve,
        'ch_cascade_relay': ch_cascade_relay,
        'ionisation_signal': ionisation_signal,
        'opentherm_disabled': opentherm_disabled,
        'burner_block': burner_block,
        'gradient_flag': gradient_flag,
        'byte_26_flags': f"{bin(d[26])[2:].zfill(8)}",
        'byte_27_flags': f"{bin(d[27])[2:].zfill(8)}",
        'byte_28_flags': f"{bin(d[28])[2:].zfill(8)}"
    }

    return data

def prettify_key(key):
    return ' '.join(word.capitalize() for word in key.split('_'))

def prettify_fault_code(code):
    fault_codes = {
        0: "F000 - Sensor defect",
        1: "F001 - Temperature too high during central heating demand",
        2: "F002 - Temperature too high during domestic hot water (DHW) demand",
        3: "F003 - Flue gas temperature too high",
        4: "F004 - No flame during startup",
        5: "F005 - Flame disappears during operation",
        6: "F006 - Flame simulation error",
        7: "F007 - No or insufficient ionisation flow",
        8: "F008 - Fan speed incorrect",
        9: "F009 - Burner controller has internal fault",
        10: "F010 - Sensor fault",
        11: "F011 - Sensor fault",
        12: "F012 - Sensor 5 fault",
        14: "F014 - Mounting fault sensor",
        15: "F015 - Mounting fault sensor S1",
        16: "F016 - Mounting fault S3",
        18: "F018 - Flue and/or air supply duct is blocked",
        19: "F019 - BMM error",
        27: "F027 - Short circuit of outdoor",
        28: "F028 - Reset error",
        29: "F029 - Gas valve error",
        30: "F030 - Sensor S3 fault",
        31: "F031 - Sensor fault S1"
    }
    return fault_codes.get(code, f"Unknown fault code: {code}")

def display_readings(data):
    """Display current boiler readings in a readable format"""
    print("\033[2J\033[H")  # Clear screen and move cursor to top
    for key, value in data.items():
        pretty_key = prettify_key(key)
        if isinstance(value, float):
            print(f"{pretty_key}: {value:.1f}")
        elif isinstance(value, bool):
            print(f"{pretty_key}: {'ON' if value else 'OFF'}")
        else:
            print(f"{pretty_key}: {value}")
    print("\nPress Ctrl+C to stop...")

def get_packet(port, mqtt_user, mqtt_password):
    mqtt_handler = MQTTHandler(mqtt_user, mqtt_password)
    mqtt_handler.connect()

    while True:  # outer reconnection loop
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
                    time.sleep(2)
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
