from base64 import b64decode
from datetime import datetime
from enum import Enum, auto
import logging
from typing import Dict, List

from cowtracker.db import connection
from cowtracker.cows import Cows


logger = logging.getLogger('messages')

cows = Cows()  # global singleton


class MessageStatus(Enum):
    ERROR = auto()
    NOFIX = auto()
    INDOOR = auto()


class Message():
    def __init__(self, data: Dict[str, str]):
        self.__data = data

        try:
            try:
                self.dev_eui = int(
                    self.__data['end_device_ids']['dev_eui'], 16) & 0x1FF
            except Exception:
                self.dev_eui = 0

            self.__uplink_message = self.__data['uplink_message']
            self.__settings = self.__data['settings']

            try:
                self.port = self.__uplink_message['f_port']
            except Exception:
                self.port = -1

            self.__raw = self.__uplink_message['frm_payload']
            self.__base64 = b64decode(self.__raw)
        except Exception:
            self.__base64 = None
            logger.exception(f"Invalid message received: {self.__data}")

        try:
            self.__rssi = self.__uplink_message['rx_metadata'][0]['rssi']
            self.__snr = self.__uplink_message['rx_metadata'][0]['snr']
            self.__sf = self.__settings['data_rate']['lora']['spreading_factor']
        except Exception:
            logger.exception(
                f"Couldn't extract rssi and snr from message: {self.__data}")
            self.__rssi = None
            self.__snr = None
            self.__sf = None

    @property
    def payload(self):
        return f"0x{self.__base64.hex()}" if self.__base64 is not None else '0x000000'

    def __repr__(self):
        return f"{self.dev_eui}:{self.port} -> {self.payload}"

    @staticmethod
    def _signed(val: int, bits: int):
        return val - (1 << bits) if val >= (1 << (bits - 1)) else val

    @staticmethod
    def _rdlsbf(bytes_: List, offset: int, length: int):
        val = 0
        offset += length - 1
        while length:
            val = val*256 + bytes_[offset]
            offset -= 1
            length -= 1

        return val

    def decode(self):
        """
        Decodes message on port 136:

        Byte:   0       1        2      3 4 5 6   7 8 9 10
        Field:  Status  Battery  Temp   Lat       Lon
        """
        if self.port == 136:
            # Byte 0: status
            # Bit 4: GSP module error
            # Bit 3: no fix
            # Bit 2: indoor
            flags = self.__base64[0]
            self.status = set()
            if flags & (1 << 4):
                self.status.add(MessageStatus.ERROR)
            if flags & (1 << 3):
                self.status.add(MessageStatus.NOFIX)
            if flags & (1 << 2):
                self.status.add(MessageStatus.INDOOR)

            # Byte 1: battery
            # Bits[3:0] unsigned value ν, range 1 – 14, battery voltage in V = (25 + ν) ÷ 10.
            # Bits [7:4] unsigned value κ, range 0 – 15 remaining battery capacity in % = 100 × (κ ÷ 15).
            self.battery = ((self.__base64[1] & 0x0F) + 25) / 10.0
            self.batt_capacity = (self.__base64[1] >> 4) * 100 / 15

            # Byte 2: temperature
            # Bits [6:0] unsigned value τ, range 0 – 127; temperature in °C = τ - 32
            self.temperature = (self.__base64[2] & 0x7F) - 32

            # Byte 3-6: latitude (lsbf)
            # Bits[27:0] signed value φ, range - 90, 000, 000 – 90, 000, 000; WGS84 latitude in ° = φ ÷ 1, 000, 000.
            # Bits [31:28] RFU
            self.latitude = Message._signed(Message._rdlsbf(
                self.__base64, 3, 4) & 0x0FFFFFFF, 28) / 1000000

            # Byte 7-10: longitude+accuracy (lsbf)
            # Bits [28:0] signed value λ, range -179,999,999 – 180,000,000; WGS84 longitude in ° = λ ÷ 1,000,000.
            # Bits [31:29] unsigned value α, range 0-7; position accuracy estimate in m = 2α + 2 (max).
            # The value 7 represents an accuracy estimate of worse than 256m.
            lonacc = Message._rdlsbf(self.__base64, 7, 4)
            self.longitude = Message._signed(
                lonacc & 0x1FFFFFFF, 29) / 1000000
            self.accuracy = 2 * ((lonacc >> 29) & 0x7) + 2

            return {
                "dev_eui": self.dev_eui,
                "battery": self.battery,
                "latitude": self.latitude,
                "longitude": self.longitude,
                "accuracy": self.accuracy,
                "temp": self.temperature,
                "status": self.status,
                "rssi": self.__rssi,
                "snr": self.__snr,
                "sf": self.__sf
            }
        else:
            logger.info(
                f"Skipping received message: {self.payload} on port {self.port}.")

    async def store(self):
        """
        Store message to db
        """
        async with await connection() as conn:
            sql = f'''
            INSERT INTO meas
            (
                deveui,
                t,
                pos,
                accuracy,
                batt_v,
                batt_cap,
                temp,
                rssi,
                snr,
                sf
            )
            VALUES
            (
                {self.dev_eui},
                '{datetime.utcnow()}',
                '({self.latitude}, {self.longitude})',
                {self.accuracy},
                {self.battery if self.battery else 'NULL'},
                {self.batt_capacity if self.batt_capacity else 'NULL'},
                {self.temperature if self.temperature else 'NULL'},
                {self.__rssi if self.__rssi else 'NULL'},
                {self.__snr if self.__snr else 'NULL'},
                {self.__sf if self.__sf else 'NULL'}
            );
            '''
            await conn.execute(sql)

            # trigger cow movement check
            await cows.check_cow_movement(self.dev_eui)
