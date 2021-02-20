import asyncio
from asyncpg import Connection, Record
from datetime import datetime, timezone
from enum import Enum, auto
from geopy.distance import distance as geodist
import logging
from typing import Any, Dict, List, Mapping, Optional
from zoneinfo import ZoneInfo

from cowtracker.db import connection

logger = logging.getLogger("cows")


class WarningType(Enum):
    NO_MSG_RECV = "WARN_NO_MSGS_RECV"
    BATT_LOW = "WARN_BATT_LOW"
    COW_NOT_MOVING = "WARN_COW_NOT_MOVING"
    COW_TOO_FAR = "WARN_COW_TOO_FAR"


class WarningVariant(Enum):
    WARNING = "warning"
    DANGER = "danger"
    INFO = "info"
    NONE = ""


class Warning():
    def __init__(self,
                 code: WarningType,
                 variant: WarningVariant,
                 value: Optional[Any] = None):
        self.code = code
        self.variant = variant
        self.value = value

    def to_json(self):
        return {
            'code': self.code.value,
            'variant': self.variant.value,
            'value': self.value
        }

    def __repr__(self):
        if self.code == WarningType.BATT_LOW:
            return f"Batería baja: {self.value[0]}V ({self.value[1]}%)"

        if self.code == WarningType.NO_MSG_RECV:
            t = self.value
            t.replace(timezone.utc).astimezone(
                tz=self.TZ).strftime("%H:%M:%S %d-%m")

            return f"No envía mensajes desde {t}"

        if self.code == WarningType.COW_TOO_FAR:
            return f"El animal está muy lejos, a {round(self.value, 1)}m del salineadero"


class PointRecord():
    BATT_V_NORMAL = 3.6
    BATT_V_WARN = 3.4
    BATT_CAP_WARN = 90
    BATT_CAP_DANGER = 80
    REF_POS = (6.7346666, -72.7717729)  # antenna location
    DIST_M_WARN = 1000
    DIST_M_DANGER = 2000
    TIME_S_WARN = 3600*4
    TIME_S_DANGER = 3600*6
    TZ = ZoneInfo('America/Bogota')

    def __init__(self, record: Record):
        self._data = record
        self.status = WarningVariant.NONE

    @property
    def localtime(self):
        t = self._data['t']
        return t.replace(tzinfo=timezone.utc).astimezone(tz=self.TZ)

    @property
    def timestamp(self):
        t = self._data['t']
        return t.timestamp()

    @property
    def point(self):
        pos_ = self._data['pos']
        return (pos_.x, pos_.y)

    def get_warnings(self, to_json: Optional[bool] = True) -> List[Warning]:
        t = self._data['t']
        batt_V = self._data['batt_v']
        batt_cap = self._data['batt_cap']
        warns: List[Dict] = []
        self.status = WarningVariant.INFO

        # low battery warning
        if (batt_V < self.BATT_V_NORMAL and batt_V > self.BATT_V_WARN) or\
                (batt_cap < self.BATT_CAP_WARN and batt_cap > self.BATT_CAP_DANGER):
            w = Warning(WarningType.BATT_LOW,
                        WarningVariant.WARNING, (batt_V, batt_cap))
            warns.append(w.to_json() if to_json else w)
            self.status = WarningVariant.WARNING

        if batt_V < self.BATT_V_WARN or batt_cap < self.BATT_CAP_DANGER:
            w = Warning(WarningType.BATT_LOW,
                        WarningVariant.DANGER, (batt_V, batt_cap))
            warns.append(w.to_json() if to_json else w)
            self.status = WarningVariant.WARNING

        dist2ref = geodist(self.point, self.REF_POS).meters
        if dist2ref > self.DIST_M_WARN and dist2ref < self.DIST_M_DANGER:
            w = Warning(WarningType.COW_TOO_FAR,
                        WarningVariant.WARNING, int(dist2ref))
            warns.append(w.to_json() if to_json else w)
            self.status = WarningVariant.WARNING

        if dist2ref > self.DIST_M_DANGER:
            w = Warning(WarningType.COW_TOO_FAR,
                        WarningVariant.DANGER, int(dist2ref))
            warns.append(w.to_json() if to_json else w)
            self.status = WarningVariant.WARNING

        now = datetime.utcnow()
        deltaT = now.timestamp() - t.timestamp()
        if deltaT > self.TIME_S_WARN and deltaT < self.TIME_S_DANGER:
            w = Warning(WarningType.NO_MSG_RECV,
                        WarningVariant.WARNING, int(deltaT/3600))
            warns.append(w.to_json() if to_json else w)
            self.status = WarningVariant.WARNING

        if deltaT > self.TIME_S_DANGER:
            w = Warning(WarningType.NO_MSG_RECV,
                        WarningVariant.DANGER, int(deltaT/3600))
            warns.append(w.to_json() if to_json else w)
            self.status = WarningVariant.WARNING

        return warns

    def to_json(self,
                name: Optional[str] = None,
                include_warnings: Optional[bool] = True) -> Dict:
        point: Dict = {}
        for key, value in self._data.items():
            if key == 'pos':
                point[key] = {'lat': value.x, 'lon': value.y}
            elif key == 't':
                t = value
                t = value.timestamp()
                point[key] = t
            elif key != 'id' and key != 'deveui':
                point[key] = value

        if name is not None:
            point['name'] = name

        if include_warnings:
            warns = self.get_warnings()
            point['warnings'] = warns
            point['status'] = self.status.value
            # TODO: get last movement from global warnings

        return point


class DBException(Exception):
    class Code(Enum):
        E_UNSPECIFIED = auto()
        E_UNKNOWN_COW = auto()

    def __init__(self, message: str = None,
                 code: 'DBException.Code' = Code.E_UNSPECIFIED):
        self.code = code
        self.message = message

    def __str__(self):
        return f"{self.code.name} {self.message if self.message else ''}"


class _Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(
                _Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Cows(metaclass=_Singleton):
    CHECKUP_PERIOD_HOURS = 6
    LAST_MSG_TIME_S_WARN = 3600*3

    def __init__(self):
        self._mapping: Optional[Mapping[str, int]] = None
        self.warnings = {'cows': {}, 'general': []}

    async def aioinit(self):
        records = await Cows._map_names_to_deveuis()
        if len(records) > 0:
            self._mapping = {x['name']: x['deveui'] for x in records}

        # start periodic task
        loop = asyncio.get_event_loop()
        loop.create_task(
            self._periodic_checkup(self.CHECKUP_PERIOD_HOURS*3600))

    async def _check_all_cows(self):
        warnings = []
        last_msg_received = 0
        last_msg_date = None
        async with await connection() as conn:
            for name in self._mapping:
                deveui = self._mapping[name]
                points = await Cows._get_last_coords_per_id(conn, deveui, 1)
                record = points[0]
                if record.timestamp > last_msg_received:
                    last_msg_received = record.timestamp
                    last_msg_date = record.localtime

                warns = record.get_warnings(to_json=False)
                if len(warns) > 0:
                    warnings.append((name, warns))

        now = datetime.utcnow().timestamp()
        if (now - last_msg_received) > self.LAST_MSG_TIME_S_WARN:
            logger.info("Possible gateway error, no message received since: {last_msg_date}")
            msg = f"Ningún mensaje recibido desde: {last_msg_date.strftime('%H:%M %d-%m')}"
            #TODO send email
            return

        if len(warnings) == 0:
            return

        msg = "Las siguientes alarmas requieren revisión:\n"
        for name, warns in warnings:
            msg += f"{name}:\n"
            for w in warns:
                msg += f"- {w}\n"

        logger.info(f"Warnings found in the periodic checkup {msg}")
        logger.info("Sending email")
        # TODO: send email

    async def _periodic_checkup(self, period: int):
        logger.info(f"scheduling periodic checkup with period {period}")
        while True:
            try:
                logger.info("Running periodic checkup")
                await self._check_all_cows()
            except Exception:
                logger.exception("Error while running periodic checkup")

            await asyncio.sleep(period)

    async def get_mapping(self) -> Mapping[str, int]:
        if self._mapping is None:
            await self.aioinit()
        return self._mapping

    async def get_names(self) -> List[str]:
        if self._mapping is None:
            await self.aioinit()
        return [x for x in self._mapping.keys()]

    @staticmethod
    async def _get_last_coords_per_id(
        conn: Connection,
        deveui: int,
        n_points: Optional[int] = 1,
    ) -> List[Dict[str, Any]]:

        sql = f'''
        SELECT * FROM meas WHERE deveui={deveui} 
        ORDER BY t DESC LIMIT {n_points};
        '''
        records = await conn.fetch(sql)
        points: List[PointRecord] = []

        for r in records:
            points.append(PointRecord(r))
        return points

    async def get_last_coords(self,
                              name: str,
                              n_points: Optional[int] = 1,
                              ) -> List[Dict[str, Any]]:

        if name not in self._mapping:
            raise DBException(
                f"Cow: {name} does exist in database", DBException.Code.E_UNKNOWN_COW)

        async with await connection() as conn:
            deveui = self._mapping[name]
            points = await Cows._get_last_coords_per_id(conn, deveui, n_points)
            if len(points) > 0:
                current_pos = points.pop(0).to_json(
                    name=name, include_warnings=True)
                return [current_pos] + [p.to_json(name=name, include_warnings=False) for p in points]
            else:
                return []

    async def get_current_pos_all_cows(self) -> List[Dict[str, Any]]:
        points = []
        async with await connection() as conn:
            # TODO: check if there is a more efficient way
            for name in self._mapping:
                deveui = self._mapping[name]
                meas = await Cows._get_last_coords_per_id(conn, deveui, 1)
                if len(meas) == 1:
                    p = meas[0]
                    points.append(p.to_json(name=name, include_warnings=True))

        return points

    @staticmethod
    async def _map_names_to_deveuis():
        async with await connection() as conn:
            sql = f'''
            SELECT c.name, t.deveui FROM cows c INNER JOIN trackers t on t.label=c.label;
            '''
            return await conn.fetch(sql)
