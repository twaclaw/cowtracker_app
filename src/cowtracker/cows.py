from asyncpg import Connection
from datetime import datetime, timezone
from enum import Enum, auto
import logging
from typing import Any, Dict, List, Mapping, Optional
from zoneinfo import ZoneInfo

from cowtracker.db import connection


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

    async def _get_last_coords_per_id(self,
                                      conn: Connection,
                                      deveui: int,
                                      n_points: Optional[int] = 1,
                                      to_local_tz: Optional[bool] = False,
                                      ui_format: Optional[bool] = False
                                      ) -> List[Dict[str, Any]]:

        sql = f'''
        SELECT * FROM meas WHERE deveui={deveui} 
        ORDER BY t DESC LIMIT {n_points};
        '''
        records = await conn.fetch(sql)
        points: List[Dict[str, Any]] = []

        if len(records) > 0:
            for r in records:
                point: Dict[str, Any] = {}
                for key, value in r.items():
                    if key == 'pos':
                        if ui_format:
                            point[key] = {'lat': value.x, 'lon': value.y}
                        else:
                            point[key] = (value.x, value.y)
                    elif key == 't':
                        t = value
                        if to_local_tz:
                            t = value.replace(
                                tzinfo=timezone.utc).astimezone(tz=self.tz)
                        if ui_format:
                            t = value.timestamp()
                        point[key] = t
                    elif key == 'id' or key == 'deveui':
                        if not ui_format: # skip fields if consumed by UI
                            point[key] = value 
                    else:
                        point[key] = value
                
                points.append(point)

        return points

    async def get_last_coords(self,
                              name: str,
                              n_points: Optional[int] = 1,
                              to_local_tz: Optional[bool] = False,
                              ui_format: Optional[bool] = False
                              ) -> List[Dict[str, Any]]:

        if name not in self._mapping:
            raise DBException(
                f"Cow: {name} does exist in database", DBException.Code.E_UNKNOWN_COW)

        async with await connection() as conn:
            deveui = self._mapping[name]
            return await self._get_last_coords_per_id(conn, deveui, n_points, to_local_tz, ui_format)

    async def get_current_pos_all_cows(self,
                                       to_local_tz: Optional[bool] = False,
                                       ui_format: Optional[bool] = False
                                       ) -> List[Dict[str, Any]]:

        points = []
        async with await connection() as conn:
            # TODO: check if there is a more efficient way
            for name in self._mapping:
                deveui = self._mapping[name]
                meas = await self._get_last_coords_per_id(conn, deveui, 1, to_local_tz, ui_format)
                if len(meas) == 1:
                    point = meas[0]
                    point['name'] = name
                    points.append(point)

        return points

    @staticmethod
    async def _map_names_to_deveuis():
        async with await connection() as conn:
            sql = f'''
            SELECT c.name, t.deveui FROM cows c INNER JOIN trackers t on t.label=c.label;
            '''
            return await conn.fetch(sql)
