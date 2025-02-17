#!/usr/bin/env python
import argparse
import serial
import logging
import sys
import time
import json
import paho.mqtt.client as mqtt
from struct import *
from logging.handlers import RotatingFileHandler

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

LOG_FILE = "log.txt"
LOG_FILE_MQTT = "log_mqtt.txt"
LOG_FILE_DATA = "log_data.txt"

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
    "pump_speed": {
        "name": "Pump Speed",
        "device_class": "power_factor",
        "unit_of_measurement": "%",
        "state_class": "measurement",
        "accuracy_decimals": 0,
        "icon": "mdi:pump"
    },
    "status": {
        "name": "Status",
        "device_class": None,
        "unit_of_measurement": None,
        "state_class": None
    },
    "gas_meter_heating": {
        "name": "Gas meter (heating)",
        "device_class": "gas",
        "unit_of_measurement": "m³",
        "state_class": "total_increasing",
        "icon": "mdi:meter-gas",
        "accuracy_decimals": 3
    },
    "gas_meter_hot_water": {
        "name": "Gas meter (hot water)",
        "device_class": "gas",
        "unit_of_measurement": "m³",
        "state_class": "total_increasing",
        "icon": "mdi:meter-gas",
        "accuracy_decimals": 3
    }
}

class MQTTHandler:
    def __init__(self, mqtt_user, mqtt_password):
        logger.info("Initializing MQTT client...")
        self.client = mqtt.Client(
            client_id=MQTT_CLIENT_ID,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2
        )
        self.client.username_pw_set(mqtt_user, mqtt_password)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.will_set(f"{MQTT_BASE_TOPIC}/status", "offline", retain=True)

        self.setup_done = False
        self.reconnect_count = 0
        self.cached_sensor_values = {}  # Cache last published values to avoid sending out unnecessary messages to mqtt

    def connect(self):
        try:
            self.client.connect(MQTT_BROKER, MQTT_PORT)
            self.client.loop_start()
        except Exception as e:
            logger.error(f"MQTT Connection failed: {str(e)}")
            sys.exit(1)

    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            logger.info("Connected to MQTT broker successfully")

            if not self.setup_done:
                self.setup_discovery()
                self.setup_done = True

            # Publish online status
            self.client.publish(f"{MQTT_BASE_TOPIC}/status", "online", retain=True, qos=1)
        else:
            logger.error(f"Connection to MQTT broker failed with result code {rc}")

    def on_disconnect(self, client, userdata, rc):
        self.reconnect_count += 1
        logger.info(f"Disconnected from MQTT broker (attempt {self.reconnect_count})")
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
            config["unique_id"] = sensor_id
            config["device"] = device_info
            config["state_topic"] = f"{MQTT_BASE_TOPIC}/{sensor_id}/state"
            config["availability_topic"] = f"{MQTT_BASE_TOPIC}/status"

            self.client.publish(
                f"{MQTT_DISCOVERY_PREFIX}/sensor/{DEVICE_ID}/{sensor_id}/config",
                json.dumps(config),
                retain=True
            )

    def publish_data(self, data):
        """Publish boiler data to MQTT topics only when values change"""
        data_logger.debug(data)

        for sensor_id in SENSORS.keys():
            if sensor_id not in data:
                logger.error(f"Missing data for sensor '{sensor_id}'")
                continue

            value = data[sensor_id]
            topic = f"{MQTT_BASE_TOPIC}/{sensor_id}/state"

            current_state = str(value)
            if sensor_id not in self.cached_sensor_values or current_state != self.cached_sensor_values[sensor_id]:
                try:
                    result = self.client.publish(
                        topic,
                        current_state,
                        qos=1,
                        retain=True
                    )

                    if result.rc == mqtt.MQTT_ERR_SUCCESS:
                        self.cached_sensor_values[sensor_id] = current_state
                        mqtt_logger.debug(f"{topic}: {current_state}")
                    else:
                        logger.error(f"Failed to publish to {topic}: {result.rc}")
                except Exception as e:
                    logger.error(f"Error publishing to {topic}: {str(e)}")
                    self.client.disconnect()
                    self.client.reconnect()

## Data parsing

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

def getFloat24(b1, b2, b3):
    value = (b1 << 16) | (b2 << 8) | b3
    if value & 0x800000:
        value -= 0x1000000
    return float(value) / 100.0

def getFloat32(b1, b2, b3, b4):
    value = (b1 << 24) | (b2 << 16) | (b3 << 8) | b4
    if value & 0x80000000:
        value -= 0x100000000
    return float(value) / 100.0


def getTemp(msb, lsb):
    word = convert_to_signed_word(msb, lsb)
    if word <= -5100 or word == 32767:  # 32767 is SHRT_MAX
        # Intergas gives -5100 for disconnected sensors
        return float('nan')
    return float(word) / 100.0

def getInt(msb, lsb):
    word = convert_to_signed_word(msb, lsb)
    return int(word)

def getInt24(b1, b2, b3):
    value = (b1 << 16) | (b2 << 8) | b3
    # Handle sign bit (if b1's MSB is 1)
    if value & 0x800000:
        value -= 0x1000000
    return value

def get_bool(data, bit):
    return bool(data & (1 << bit))

def parse_status_response(s):
    heat_exchanger_temp = getTemp(s[1],s[0])    # not 100% sure
    flow_temp = getTemp(s[3], s[2])
    return_temp = getTemp(s[5], s[4])
    hot_water_temp = getTemp(s[7], s[6])
    t5 = getTemp(s[9], s[8])                     # boiler temp (?)
    outside_temp = getTemp(s[11], s[10])
    water_pressure = getFloat(s[13], s[12])
    temp_setpoint = getFloat(s[15], s[14])
    fan_speed_setpoint = getInt(s[17], s[16])
    fan_speed = getInt(s[19], s[18])
    fan_pwm = getFloat(s[21], s[20])            # watts?
    ionisation_current = getFloat(s[23], s[22])
    displ_code = s[24]

    # TO-DO: What's on byte 25?

    # bit flags from byte 26
    gp_switch = get_bool(s[26], 0)
    tap_switch = get_bool(s[26], 1)
    roomtherm = get_bool(s[26], 2)
    pump = get_bool(s[26], 3)
    three_way_valve = get_bool(s[26], 4)
    alarm_status = get_bool(s[26], 5)
    ch_cascade_relay = get_bool(s[26], 6)
    opentherm = get_bool(s[26], 7)

    # TO-DO: What are the flags for byte 27 beyond fault code?

    # bit flags from byte 28
    gas_valve = get_bool(s[28], 0)
    spark = get_bool(s[28], 1)
    ionisation_signal = get_bool(s[28], 2)
    opentherm_disabled = get_bool(s[28], 3)
    low_water_pressure = get_bool(s[28], 4)
    pressure_sensor = get_bool(s[28], 5)
    burner_block = get_bool(s[28], 6)
    gradient_flag = get_bool(s[28], 7)

    if not pressure_sensor:
        water_pressure = float('nan')

    if get_bool(s[27], 7):
        # last known fault code
        fault_code = prettify_fault_code(s[29])
    else:
        fault_code = "None"

    # Outside temp sensor may not be available
    if outside_temp < -50:
        outside_temp = "N/A"

    # Add status code interpretation
    status_codes = {
        51: "Hot water ramp down",
        0: "Central Heating active",
        102: "Central Heating health-check",    # anti-blockade run once every 24 hours
        126: "Idle",
        170: "Service mode",
        204: "Hot water active",
        231: "Central Heating ramp down",       # water recirculation after each heating period
    }
    status = status_codes.get(displ_code, f"Unknown ({displ_code})")

    data = {
        'status': status,
        'temp_setpoint': round(temp_setpoint, 1),
        'flow_temp': round(flow_temp, 1),
        'return_temp': round(return_temp, 1),
        'hot_water_temp': round(hot_water_temp, 1),
        'heat_exchanger_temp': round(heat_exchanger_temp, 1),
        'outside_temp': outside_temp,
        't5': round(t5, 1),
        'fan_speed': fan_speed,
        'fan_speed_setpoint': fan_speed_setpoint,
        'fan_pwm': fan_pwm,
        'pump': pump,
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
        'byte_26_flags': f"{bin(s[26])[2:].zfill(8)}",
        'byte_27_flags': f"{bin(s[27])[2:].zfill(8)}",
        'byte_28_flags': f"{bin(s[28])[2:].zfill(8)}"
    }

    return data

def parse_status_extra_response(s):
    tap_flow = getFloat(s[1], s[0])
    pump_speed = (200 - int(s[2])) / 2   # percentage speed 0-100

    return {
        'tap_flow': tap_flow,
        'pump_speed': int(pump_speed)
    }

def parse_stats_response(s):
    line_power_connected_hours = getInt24(s[30], s[1], s[0])
    line_power_connected_count = getInt(s[3], s[2])
    heating_hours = getInt(s[5], s[4])
    hot_water_hours = getInt(s[7], s[6])
    burner_start_count_heating = getInt24(s[31], s[9], s[8])
    ignition_failed = getInt(s[11], s[10])
    flame_lost = getInt(s[13], s[12])
    reset_count = getInt(s[15], s[14])
    gas_meter_heating = getFloat32(s[19], s[18], s[17], s[16]) / 100    # m3
    gas_meter_hot_water = getFloat32(s[23], s[22], s[21], s[20]) / 100  # m3
    water_meter = getFloat24(s[28], s[25], s[24])
    burner_start_count_hot_water = getInt24(s[29], s[27], s[26])

    data = {
        'line_power_connected_hours': line_power_connected_hours,
        'line_power_connected_count': line_power_connected_count,
        'heating_hours': heating_hours,
        'hot_water_hours': hot_water_hours,
        'burner_start_count_heating': burner_start_count_heating,
        'burner_start_count_hot_water': burner_start_count_hot_water,
        'gas_meter_heating': round(gas_meter_heating, 3),
        'gas_meter_hot_water': round(gas_meter_hot_water, 3),
        'water_meter': water_meter,
        'ignition_failed': ignition_failed,
        'flame_lost': flame_lost,
        'reset_count': reset_count
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
    last_stats_time = 0

    while True:  # outer serial reconnection loop
        try:
            with serial.Serial(port, 9600, timeout=2) as ser:
                logger.info(f"Connected to {port}")
                while True:
                    # Retrieve status
                    ser.write(b'S?\r')
                    data = ser.read(32)
                    if len(data) == 32:
                        parsed_status_data = parse_status_response(data)

                    # Retrieve status extra
                    ser.write(b'S2\r')
                    data = ser.read(32)
                    if len(data) == 32:
                        parsed_status_extra_data = parse_status_extra_response(data)

                    # Retrieve runtime stats every 60 seconds
                    current_time = time.time()
                    if current_time - last_stats_time >= 60:
                        last_stats_time = current_time
                        ser.write(b'HN\r')
                        data = ser.read(32)
                        if len(data) == 32:
                            parsed_stats_data = parse_stats_response(data)

                    aggregated_data = parsed_status_data | parsed_status_extra_data | parsed_stats_data
                    display_readings(aggregated_data)
                    mqtt_handler.publish_data(aggregated_data)

                    time.sleep(2)

        except serial.SerialException as e:
            logger.error(f"Serial connection lost: {e}")
            time.sleep(10)
            continue
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            time.sleep(10)
            continue

def make_general_logger():
    log_file = logging.FileHandler(LOG_FILE)
    log_file.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    log_file.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger = logging.getLogger("logger")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(log_file)
    logger.addHandler(console_handler)

    return logger

def make_mqtt_logger():
    log_file = RotatingFileHandler(LOG_FILE_MQTT, maxBytes=10000000, backupCount=1)
    log_file.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    log_file.setFormatter(formatter)

    logger = logging.getLogger("mqtt_logger")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(log_file)

    return logger

def make_data_logger():
    log_file = RotatingFileHandler(LOG_FILE_DATA, maxBytes=10000000, backupCount=1)
    log_file.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    log_file.setFormatter(formatter)

    logger = logging.getLogger("data_logger")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(log_file)

    return logger

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Intergas boiler reader')
    parser.add_argument('--mqtt-user', required=True, help='MQTT username')
    parser.add_argument('--mqtt-password', required=True, help='MQTT password')
    parser.add_argument('--port', required=True, help='Serial port')
    args = parser.parse_args()

    logger = make_general_logger()
    mqtt_logger = make_mqtt_logger()
    data_logger = make_data_logger()

    get_packet(args.port, args.mqtt_user, args.mqtt_password)
