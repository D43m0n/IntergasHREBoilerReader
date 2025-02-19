# Intro

*Note: This project is under development. Some features may be incomplete or subject to change.*

This started as a fork of https://github.com/rixvet/IntergasBoilerReader adapted to work with my *Intergas Kombi Kompakt HRE 36/30* boiler, but quickly grew up to a state where most of the code is new. Features:
- Reads and displays in the console most of the known data available for the HRE boiler (state, extra state and runtime stats commands).
- The relevant data is published to an MQTT broker as sensors bundled in a boiler device that is auto-discovered by Home Assistant.
- Much improved error handling, data logging and reconnection logic.

# Dependencies
```
pip install pyserial paho-mqtt
```

# Connection to the boiler

You'll need a FTDI USB to TTL serial device (e.g. I purchased [this one](https://es.aliexpress.com/item/1005006445462581.html)) and wire it to the boiler's X5 connector (using an ATX 4 pin plug like [this one](https://es.aliexpress.com/item/1005006821754564.html?)) following this schema:

FTDI TTL | X5 Interface
---------|-------------
RX       | Rx
TX       | Tx
GND      | Gnd

With the VCC jumper set to 5v. Do NOT connect the VCC wire however or the boiler will trip.

# Usage

I run this on a Raspberry Pi 3B+ permanently placed near the Intergas boiler.

Edit the MQTT details, host, port and client ID in the python script. Then find out the associated serial port and run it, e.g.:

`python intergas_hre.py --mqtt-user boiler --mqtt-password somepassword --port /dev/tty.usbserial-5`
