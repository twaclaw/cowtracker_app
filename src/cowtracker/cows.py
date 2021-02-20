from asyncpg import Connection, Record
from datetime import datetime, timezone
from enum import Enum, auto
from geopy.distance import distance as geodist
import logging
from typing import Any, Dict, List, Mapping, Optional
from zoneinfo import ZoneInfo

from cowtracker.db import connection


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
    TZ = "America/Bogota"

    def __init__(self, record: Record):
        self._data = record
        self.status = WarningVariant.NONE

    @property
    def localtime(self):
        t = self._data['t']
        return t.replace(timezone.utc).astimezone(tz=self.TZ)

    @property
    def timestamp(self):
        t = self._data['t']
        return t.timestamp()

    def get_warnings(self) -> List[Warning]:
        t = self._data['t']
        batt_V = self._data['batt_v']
        batt_cap = self._data['batt_cap']
        pos_ = self._data['pos']
        pos = (pos_.x, pos_.y)

        warns: List[Dict] = []
        self.status = WarningVariant.INFO

        # TODO: get last movement from global warnings

        # low battery warning
        if (batt_V < self.BATT_V_NORMAL and batt_V > self.BATT_V_WARN) or\
                (batt_cap < self.BATT_CAP_WARN and batt_cap > self.BATT_CAP_DANGER):
            w = Warning(WarningType.BATT_LOW, WarningVariant.WARNING, batt_V)
            warns.append(w.to_json())
            self.status = WarningVariant.WARNING

        if batt_V < self.BATT_V_WARN or batt_cap < self.BATT_CAP_DANGER:
            w = Warning(WarningType.BATT_LOW, WarningVariant.DANGER, batt_V)
            warns.append(w.to_json())
            self.status = WarningVariant.WARNING

        dist2ref = geodist(pos, self.REF_POS).meters
        if dist2ref > self.DIST_M_WARN and dist2ref < self.DIST_M_DANGER:
            w = Warning(WarningType.COW_TOO_FAR,
                        WarningVariant.WARNING, int(dist2ref))
            warns.append(w.to_json())
            self.status = WarningVariant.WARNING

        if dist2ref > self.DIST_M_DANGER:
            w = Warning(WarningType.COW_TOO_FAR,
                        WarningVariant.DANGER, int(dist2ref))
            warns.append(w.to_json())
            self.status = WarningVariant.WARNING

        now = datetime.utcnow()
        deltaT = now.timestamp() - t.timestamp()
        if deltaT > self.TIME_S_WARN and deltaT < self.TIME_S_DANGER:
            w = Warning(WarningType.NO_MSG_RECV,
                        WarningVariant.WARNING, int(deltaT/3600))
            warns.append(w.to_json())
            self.status = WarningVariant.WARNING

        if deltaT > self.TIME_S_DANGER:
            w = Warning(WarningType.NO_MSG_RECV,
                        WarningVariant.DANGER, int(deltaT/3600))
            warns.append(w.to_json())
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
    def __init__(self):
        self._mapping: Optional[Mapping[str, int]] = None
        self.tz = ZoneInfo('America/Bogota')
        self.warnings = {'cows': {}, 'general': []}

    async def aioinit(self):
        records = await Cows._map_names_to_deveuis()
        if len(records) > 0:
            self._mapping = {x['name']: x['deveui'] for x in records}

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
