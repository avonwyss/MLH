import ast
import datetime
from typing import *

from farmware_tools import device

from Farmbot import Farmware, Plant, Sequence
from utils import parse_datetime, parse_date, dump_datetime, dump_date, local_to_utc, utc_to_local, utc_now, local_now, Entity


class Config(Entity):
    query: Optional[Dict[str, Any]]
    save_meta: Optional[Dict[str, Any]]
    travel_height: Optional[int]
    init: Optional[Sequence]
    before: Optional[Sequence]
    after: Optional[Sequence]
    end: Optional[Sequence]
    offset_x: Optional[int]
    offset_y: Optional[int]


class MLH(Farmware[Config]):
    def __init__(self, app_name: str):
        super().__init__(Config, app_name)

    def execute(self):
        plants = self.query_points(Plant, self.config.query)
        if not plants:
            device.log(f"The query did not yield any plants, skipping execution", 'info')
            return
        self.execute_sequence(self.config.init)
        for plant in self.sort_moves(plants):
            self.execute_sequence(self.config.before)
            self.moveto_smart(plant, 100, self.config.offset_x or 0, self.config.offset_y or 0, 0, self.config.travel_height)
            self.execute_sequence(self.config.after)
            # TODO: update metadata
        self.execute_sequence(self.config.end)
