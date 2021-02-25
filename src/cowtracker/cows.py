import asyncio
from asyncpg import Connection, Record
from datetime import datetime, tzinfo
from enum import Enum, auto
from geopy.distance import distance as geodist
import logging
from pytz import timezone
from typing import Any, Dict, List, Mapping, Optional


from cowtracker.db import connection
from cowtracker.email import Email

logger = logging.getLogger("cows")

# global variables

_BATT_V_NORMAL = None
_BATT_V_WARN = None
_BATT_CAP_WARN = None
_BATT_CAP_DANGER = None
_REF_POS = None
_DIST_M_WARN = None
_DIST_M_DANGER = None
_TIME_S_WARN = None
_TIME_S_DANGER = None
_TZ = timezone('America/Bogota')


def set_warn_levels(warn_levels):
    global _BATT_V_NORMAL
    global _BATT_V_WARN
    global _BATT_CAP_WARN
    global _BATT_CAP_DANGER
    global _REF_POS
    global _DIST_M_WARN
    global _DIST_M_DANGER
    global _TIME_S_WARN
    global _TIME_S_DANGER

    _BATT_V_NORMAL = warn_levels['batt_v_normal']
    _BATT_V_WARN = warn_levels['batt_v_warn']
    _BATT_CAP_WARN = warn_levels['batt_cap_warn']
    _BATT_CAP_DANGER = warn_levels['batt_cap_danger']
    _REF_POS = warn_levels['ref_pos']
    _DIST_M_WARN = warn_levels['dist_m_warn']
    _DIST_M_DANGER = warn_levels['dist_m_danger']
    _TIME_S_WARN = 3600*warn_levels['time_h_warn']
    _TIME_S_DANGER = 3600*warn_levels['time_h_danger']


class _WarningType(Enum):
    NO_MSG_RECV = "WARN_NO_MSGS_RECV"
    BATT_LOW = "WARN_BATT_LOW"
    COW_NOT_MOVING = "WARN_COW_NOT_MOVING"
    COW_TOO_FAR = "WARN_COW_TOO_FAR"


class _WarningVariant(Enum):
    WARNING = "warning"
    DANGER = "danger"
    INFO = "info"
    NONE = ""


class _Warning():
    def __init__(self,
                 code: _WarningType,
                 variant: _WarningVariant,
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
        if self.code == _WarningType.BATT_LOW:
            return f"Batería baja: {self.value[0]}V ({self.value[1]}%)"

        if self.code == _WarningType.NO_MSG_RECV:
            t: datetime = self.value
            t.astimezone(_TZ).strftime("%H:%M:%S %d-%m")

            return f"No envía mensajes desde {t}"

        if self.code == _WarningType.COW_TOO_FAR:
            return f"El animal está muy lejos, a {round(self.value, 1)}m del salineadero"


class _PointRecord():
    def __init__(self, record: Record):
        self._data = record
        self.status = _WarningVariant.NONE

    @property
    def localtime(self):
        t: datetime = self._data['t']
        return t.astimezone(_TZ)

    @property
    def timestamp(self):
        t = self._data['t']
        return t.timestamp()

    @property
    def point(self):
        pos_ = self._data['pos']
        return (pos_.x, pos_.y)

    @property
    def accuracy(self):
        return self._data['accuracy']

    def get_warnings(self, to_json: Optional[bool] = True) -> List[_Warning]:
        t = self._data['t']
        batt_V = self._data['batt_v']
        batt_cap = self._data['batt_cap']
        warns: List[Dict] = []
        self.status = _WarningVariant.INFO

        # low battery warning
        if (batt_V < _BATT_V_NORMAL and batt_V > _BATT_V_WARN) or\
                (batt_cap < _BATT_CAP_WARN and batt_cap > _BATT_CAP_DANGER):
            w = _Warning(_WarningType.BATT_LOW,
                         _WarningVariant.WARNING, (batt_V, batt_cap))
            warns.append(w.to_json() if to_json else w)
            self.status = _WarningVariant.WARNING

        if batt_V < _BATT_V_WARN or batt_cap < _BATT_CAP_DANGER:
            w = _Warning(_WarningType.BATT_LOW,
                         _WarningVariant.DANGER, (batt_V, batt_cap))
            warns.append(w.to_json() if to_json else w)
            self.status = _WarningVariant.WARNING

        # check if cow is too far away from reference point
        dist2ref = geodist(self.point, _REF_POS).meters
        if dist2ref > _DIST_M_WARN and dist2ref < _DIST_M_DANGER:
            w = _Warning(_WarningType.COW_TOO_FAR,
                         _WarningVariant.WARNING, int(dist2ref))
            warns.append(w.to_json() if to_json else w)
            self.status = _WarningVariant.WARNING

        if dist2ref > _DIST_M_DANGER:
            w = _Warning(_WarningType.COW_TOO_FAR,
                         _WarningVariant.DANGER, int(dist2ref))
            warns.append(w.to_json() if to_json else w)
            self.status = _WarningVariant.DANGER

        # check if device is not sending data
        now = datetime.utcnow()
        deltaT = now.timestamp() - t.timestamp()
        if deltaT > _TIME_S_WARN and deltaT < _TIME_S_DANGER:
            w = _Warning(_WarningType.NO_MSG_RECV,
                         _WarningVariant.WARNING, int(deltaT/3600))
            warns.append(w.to_json() if to_json else w)
            self.status = _WarningVariant.WARNING

        if deltaT > _TIME_S_DANGER:
            w = _Warning(_WarningType.NO_MSG_RECV,
                         _WarningVariant.DANGER, int(deltaT/3600))
            warns.append(w.to_json() if to_json else w)
            self.status = _WarningVariant.WARNING

        return warns

    def to_json(self,
                name: Optional[str] = None,
                include_warnings: Optional[bool] = True,
                no_mov_warn: Optional[_Warning] = None) -> Dict:
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
            if no_mov_warn:
                self.status = _WarningVariant.DANGER
                warns.append(no_mov_warn)

            point['status'] = self.status.value
            point['warnings'] = warns

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
        self._mapping_by_deveui: Optional[Mapping[str, int]] = None
        self.email_sender: Optional[Email] = None
        self.cows_not_moving: Dict[str, _Warning] = {}

    async def _create_name_deveui_mapping(self):
        records = await Cows._map_names_to_deveuis()
        if len(records) > 0:
            self._mapping = {x['name']: x['deveui'] for x in records}
            self._mapping_by_deveui = {x['deveui']: x['name'] for x in records}


    async def aioinit(self, email_sender: Email):
        self.email_sender = email_sender
        await self._create_name_deveui_mapping()
        # start periodic task
        loop = asyncio.get_event_loop()
        loop.create_task(
            self._periodic_checkup(self.CHECKUP_PERIOD_HOURS*3600))

    @staticmethod
    def _get_localtime() -> datetime:
        now = datetime.utcnow()
        return now.astimezone(_TZ)

    async def _check_all_cows(self):
        warnings = []
        last_msg_received = 0
        last_msg_date = None
        if self._mapping is None:
            await self._create_name_deveui_mapping()

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
            logger.info(
                f"Possible gateway error, no message received since: {last_msg_date}")
            msg = f"Ningún mensaje recibido desde las: {last_msg_date.strftime('%H:%M %d-%m-%Y')}"
            self.email_sender.send_email(
                "[REVISAR] No se están recibiendo mensajes", msg)
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
        self.email_sender.send_email("[REVISAR] Alarmas", msg)

    async def _periodic_checkup(self, period: int):
        logger.info(f"scheduling periodic checkup with period {period}")
        while True:
            try:
                logger.info("Running periodic checkup")
                await self._check_all_cows()
            except Exception:
                logger.exception("Error while running periodic checkup")

            await asyncio.sleep(period)

    @staticmethod
    def _is_same_pos(p1: _PointRecord, p2: _PointRecord) -> bool:
        """ Checks if two points are approximately the same
        """
        acc = max(p1.accuracy, p2.accuracy)  # takes the largest accuracy
        delta = geodist(p1.point, p2.point).meters
        return delta > 2*acc

    async def check_cow_movement(self, deveui: int):
        """ Check if a cow is moving

        This function is called whenever a new record is stored to the database
        """
        now = self._get_localtime()
        # Skip during the night
        if now.hour < 8 and now.hour > 20:
            return

        async with await connection() as conn:
            points = await self._get_last_coords_per_id(conn, deveui, 20)

        n_points = len(points)
        if n_points > 3:
            p0 = points[0]
            p1 = points[1]
            p2 = points[2]

            # if current position is the same as  previous two
            if self._is_same_pos(p0, p1) and self._is_same_pos(p0, p2):
                logger.info(
                    f"Current position is the same as previous two p[0]: {p0.point}, p[-1]: {p1.point}, p[-2]: {p2.point}")

                i = 3
                p = points[i]
                while i < n_points and self._is_same_pos(p0, p):
                    p = points[i]
                    i += 1

                name = self._mapping_by_deveui[deveui]
                last_msg_date = p.localtime

                self.cows_not_moving[name] = _Warning(
                    _WarningType.COW_NOT_MOVING, _WarningVariant.DANGER, p.timestamp)

                logger.info(f"{name} not moving since {last_msg_date}")
                msg = f"{name} no se mueve  al menos desde: {last_msg_date.strftime('%H:%M %d-%m-%Y')}"
                self.email_sender.send_email(
                    "[URGENTE] {name} no se está moviendo!", msg)

    async def get_mapping(self) -> Mapping[str, int]:
        if self._mapping is None:
            await self._create_name_deveui_mapping()
        return self._mapping

    async def get_names(self) -> List[str]:
        if self._mapping is None:
            await self._create_name_deveui_mapping()
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
        points: List[_PointRecord] = []

        for r in records:
            points.append(_PointRecord(r))
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
                    no_mov_warn = self.cows_not_moving[name] if name in self.cows_not_moving else None
                    points.append(
                        p.to_json(name=name, no_mov_warn=no_mov_warn))

        return points

    @staticmethod
    async def _map_names_to_deveuis():
        async with await connection() as conn:
            sql = f'''
            SELECT c.name, t.deveui FROM cows c INNER JOIN trackers t on t.label=c.label;
            '''
            return await conn.fetch(sql)
