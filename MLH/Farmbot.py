import ast
import os
import requests
import math
import time
import urllib.parse
import re
import OpenFarm
import operator
import json
from abc import abstractmethod
from farmware_tools import device, app
from datetime import *
from typing import *
from utils import utc_now, get_factory, Entity, parse_datetime, dump_datetime, parse_offset, TAny


class Identifiable(Entity):
    id: int
    created_at: datetime
    updated_at: datetime


class Coordinate(Entity):
    x: int
    y: int
    z: int

    def __add__(self, other) -> 'Coordinate':
        return Coordinate({
            'x': self.x + other.x,
            'y': self.y + other.y,
            'z': self.z + other.z
        })

    def __sub__(self, other) -> 'Coordinate':
        return Coordinate({
            'x': self.x - other.x,
            'y': self.y - other.y,
            'z': self.z - other.z
        })

    def __neg__(self) -> 'Coordinate':
        return Coordinate({
            'x': -self.x,
            'y': -self.y,
            'z': -self.z
        })

    def distance(self, x: int, y: int) -> int:
        return int(math.sqrt((self.x - x) ** 2 + (self.y - y) ** 2))

    def to_coordinate(self) -> Dict[str, Any]:
        return device.assemble_coordinate(self.x, self.y, self.z)

    def merge(self, other: Dict[str, int]):
        return Coordinate({
            'x': other['x'] if 'x' in other else self.x,
            'y': other['y'] if 'y' in other else self.y,
            'z': other['z'] if 'z' in other else self.z
        })


class Device(Identifiable):
    fbos_version: Optional[str]
    last_saw_api: Optional[datetime]
    last_saw_mq: Optional[datetime]
    mounted_tool_id: Optional[int]
    name: str
    serial_number: str
    throttled_at: Optional[datetime]
    throttled_until: Optional[datetime]


class Tool(Identifiable):
    name: str
    status: str


__point_types: Optional[Dict[str, type]] = None


def get_point_type(pointer_type: str) -> Type['Point']:
    """
    Get the class type for a pointer_type
    """
    global __point_types
    if not __point_types:
        __point_types = {}
        queue = [Point]
        while queue:
            parent = queue.pop()
            for child in parent.__subclasses__():
                if child not in __point_types:
                    __point_types[child.__name__] = child
                    queue.append(child)
    return __point_types.get(pointer_type) or Point


def get_pointer_type(point_type: Type['Point']) -> str:
    if point_type is Point:
        return 'GenericPointer'
    return point_type.__name__


class Point(Identifiable, Coordinate):
    device_id: int
    pointer_type: str
    name: str
    meta: Dict[str, Any]
    radius: float
    discarded_at: Optional[datetime]

    def __new__(cls, __dict__: Dict[str, Any] = None, *args, **kwargs):
        if cls is Point and (__dict__ or kwargs):
            cls = get_point_type((__dict__ or kwargs).get('pointer_type'))
        return super().__new__(cls, __dict__, *args, **kwargs)

    def __init__(self, __dict__: Dict[str, Any] = None, *args, **kwargs):
        super().__init__(__dict__, *args, **kwargs)
        if not self.pointer_type:
            typ = type(self)
            self.pointer_type = get_pointer_type(typ)
        if self.meta is None:
            self.meta = {}
        if self.x is None:
            self.x = 0
        if self.y is None:
            self.y = 0
        if self.z is None:
            self.z = 0
        if self.radius is None:
            self.radius = 0.0

    def apply(self, updates: Dict[str, Any]):
        for key, value in updates.items():
            if isinstance(value, str):
                offset = parse_offset(value)
                if offset:
                    value = dump_datetime(utc_now() + offset)
            if key.startswith("+"):
                append = True
                key = key[1:]
            else:
                append = False
            ismeta = (key in ('meta', 'id')) or (key not in get_factory(type(self)).__self__.get_props())
            data = self.meta if ismeta else self.__dict__
            if append:
                data[key] += value
            elif value is None and ismeta:
                del data[key]
            else:
                data[key] = value
        device.log(f"Applied data update to point: {json.dumps(self)}", "debug")


class Plant(Point):
    openfarm_slug: str
    plant_stage: str
    planted_at: Optional[datetime]

    def plant_age(self) -> int:
        if self.plant_stage in ('planned', 'harvested'):
            return 0
        if self.planted_at is None:
            return 0
        return (utc_now() - self.planted_at).days + 1


class ToolSlot(Point):
    tool_id: int
    pullout_direction: Optional[int]


class Command(Entity):
    kind: str
    args: Dict[str, Any]


class Sequence(Identifiable):
    args: Dict[str, Any]
    color: str
    name: str
    kind: str
    body: List[Command]


class LocationData(Entity):
    position: Coordinate


class InformationalSettings(Entity):
    busy: bool
    locked: bool
    commit: str
    firmware_commit: str
    target: str
    env: str
    node_name: str
    currently_on_beta: Optional[bool]
    update_available: Optional[bool]
    memory_usage: Optional[int]
    disk_usage: Optional[int]
    soc_temp: Optional[int]
    wifi_level: Optional[int]
    controller_version: str
    firmware_version: str
    throttled: str
    private_ip: str
    sync_status: str
    uptime: Optional[int]


class Pin(Entity):
    mode: int
    value: float


class ProcessInfo(Entity):
    farmwares: Dict[str, Entity]


class Alert(Entity):
    id: Optional[int]
    created_at: datetime
    problem_tag: str
    priority: int
    slug: str


class BotStateTree(Entity):
    location_data: LocationData
    informational_settings: InformationalSettings
    pins: Dict[int, Pin]
    configuration: Entity
    user_env: Dict[str, Optional[str]]
    jobs: Dict[str, Optional[str]]
    process_info: ProcessInfo
    alerts: Optional[Dict[str, Alert]]


TPoint = TypeVar("TPoint", bound=Point)


class PointQuery(Generic[TPoint]):
    __rxdate = re.compile('(?:(before|after)\\s+)?([0-9]+\\s[a-z]+\\s+ago|in\\s+[0-9]+\\s+[a-z]+|now|20[1-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9](?:\\.[0-9]+)Z)')
    __rxnum = re.compile('at\\s+(least|most)\\s+(-?[0-9]+(?:\\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)')

    predicates: List[Callable[[Point], bool]]
    filter: Dict[str, Any]
    factory: Callable[[Any], TPoint]

    def __init__(self, point_type: Type[TPoint], query: Union[str, List[Tuple[str, Any]]]):
        factory = get_factory(point_type)
        props: List[str] = factory.__self__.get_props()
        if isinstance(query, str):
            query = ast.literal_eval(query)
        self.predicates = []
        self.filter = {
            'pointer_type': get_pointer_type(point_type),
            'meta': {}
        }
        for key, value in cast(List[Tuple[str, Any]], query):
            if key.startswith('!'):
                negate = True
                key = key[1:]
            else:
                negate = False
            ismeta = (key in ('meta', 'id')) or (key not in props)
            get = (lambda v, key=key: v['meta'][key]) if ismeta else (lambda v, key=key: v[key])
            if isinstance(value, str):
                # special date handling
                match = PointQuery.__rxdate.fullmatch(value.strip())
                if match:
                    date = parse_datetime(match.group(2))
                    if not match.group(1):
                        # exact date match, no local filtering required, but normalize the date format
                        value = dump_datetime(date)
                    else:
                        if match.group(1) == 'before':
                            op = operator.lt if not negate else operator.ge
                        else:  # match.group(1) == 'after':
                            op = operator.gt if not negate else operator.le
                        self.predicates.append(lambda v, get=get, op=op, date=date: op(parse_datetime(get(v)), date))
                        continue
                # special number handling
                match = PointQuery.__rxnum.fullmatch(value.strip())
                if match:
                    number = float(match.group(2))
                    if match.group(1) == 'least':
                        op = operator.ge if not negate else operator.lt
                    else:  # match.group(1) == 'most':
                        op = operator.le if not negate else operator.gt
                    self.predicates.append(lambda v, get=get, op=op, number=number: op(float(get(v)), number))
                    continue
            if negate:
                self.predicates.append(lambda v, get=get, value=value: get(v) != value)
            elif ismeta:
                self.filter['meta'][key] = value
            else:
                self.filter[key] = value
        if not self.filter['meta']:
            del self.filter['meta']
        device.log(f"Built remote filter {json.dumps(self.filter)} and {len(self.predicates)} local predicates", message_type='debug')

    def execute(self) -> List[TPoint]:
        result: List[TPoint] = []
        for point in (self.factory(data) for data in app.search_points(self.filter)):
            for predicate in self.predicates:
                if not predicate(point):
                    continue
            result.append(point)
        return result


TConfig = TypeVar("TConfig", bound=Entity)


def deserialize(typ: Type[TAny], data: Any) -> TAny:
    try:
        return get_factory(typ)(data)
    except Exception as ex:
        device.log(f"Failed to deserialize {typ.__name__} from data {json.dumps(data)}")
        raise ex


class Farmware(Generic[TConfig]):
    __sequences: Optional[List[Sequence]]
    __tools: Optional[List[Tool]]
    config: TConfig
    debug: bool
    app_name: str

    def __init__(self, config_type: Type[TConfig], manifest_name: Optional[str]):
        self.__sequences = None
        self.__tools = None
        self.debug = False
        self.local = False
        self.app_name = manifest_name or type(self).__name__
        device.log(f"Initializing farmware {type(self).__name__} with manifest name {self.app_name}", "debug")
        rx = re.compile(f"^{re.escape(self.app_name.replace('-', '_'))}_([a-z_]+)$", re.IGNORECASE)
        config = {}
        for env in os.environ.items():
            match = rx.match(env[0])
            if match:
                name = match.group(1).lower()
                if name == 'action':
                    if env[1].lower() != 'real':
                        self.debug = True
                        device.log('TEST MODE, NO sequences or movement will be run, plants will NOT be updated', 'warn')
                else:
                    config[name] = env[1]
        device.log(f"Farmware raw config: {json.dumps(config)}", 'debug')
        try:
            self.config = deserialize(config_type, config)
        except Exception as e:
            raise ValueError('Error getting farmware config: ' + str(e))
        import utils
        utils.tz = int(app.get('device')['tz_offset_hrs'])

    @abstractmethod
    def execute(self):
        pass

    def sync(self):
        if not self.debug:
            time.sleep(1)  # wait a bit for previously send requests to settle
            device.sync()
        sync: str
        for cnt in range(1, 30):
            sync = self.bot_state().informational_settings.sync_status
            device.log(f"interim status {sync}")
            if sync == "synced":
                break
            if sync == "sync_error":
                raise ValueError('Sync error, bot failed to complete syncing')
            time.sleep(0.5)
        else:
            raise ValueError('Sync timeout, bot failed to complete syncing')

    def bot_state(self) -> BotStateTree:
        """Get the device state."""
        return deserialize(BotStateTree, device.get_bot_state())

    def sequences(self) -> List[Sequence]:
        """Get the available sequences."""
        if self.__sequences is None:
            self.__sequences = deserialize(List[Sequence], app.get('sequences'))
        return self.__sequences

    def tools(self) -> List[Tool]:
        """Get the available tools."""
        if self.__tools is None:
            self.__tools = deserialize(List[Tool], app.get('tools'))
        return self.__tools

    def get_points(self, **kwargs) -> List[Point]:
        """Query point data from the web app.

        Args:
            **kwargs filters, allowed keys include: pointer_type, name, meta, radius, x, y, z
        """
        return deserialize(List[Point], app.search_points(dict(**kwargs)))

    def get_genericpointers(self, **kwargs) -> List[Point]:
        """Query generic pointers from the web app.

        Args:
            **kwargs filters, allowed keys include: name, meta, radius, x, y, z
        """
        return self.get_points(pointer_type='GenericPointer', **kwargs)

    def get_plants(self, **kwargs) -> List[Plant]:
        """Query plants from the web app.

        Args:
            **kwargs filters, allowed keys include: name, plant_stage, openfarm_slug, meta, radius, x, y, z
        """
        return cast(List[Plant], self.get_points(pointer_type='Plant', **kwargs))

    def get_toolslots(self, **kwargs) -> List[ToolSlot]:
        """Query tool slot data from the web app.

        Args:
            **kwargs filters, allowed keys include: name, tool_id, pullout_direction, meta, radius, x, y, z
        """
        return cast(List[ToolSlot], self.get_points(pointer_type='ToolSlot', **kwargs))

    def put_point(self, point: TPoint) -> TPoint:
        """
        Store an existing or new point
        """
        device.log(f"Sending point {json.dumps(point)}", 'debug')
        if self.debug:
            return point
        result = app.put('points', point.id, point) if point.id is not None else app.post('points', cast(Any, point))
        return deserialize(Point, result)

    def add_plant(self, x: float, y: float, **kwargs) -> Plant:
        """Add a plant to the garden map.

        Args:
            x (int): X Coordinate.
            y (int): Y Coordinate.
            **kwargs: name, openfarm_slug, radius, z, planted_at, plant_stage
        """
        payload = {
            'pointer_type': 'Plant',
            'x': x,
            'y': y
        }
        for key, value in kwargs.items():
            if value is not None:
                payload[key] = value
        return deserialize(Plant, app.post('points', payload))

    def get_sequence_by_name(self, name: str) -> Sequence:
        """Find the sequence_id for a given sequence name.

        Args:
            name (str): Sequence name.
        """
        for sequence in self.sequences():
            if sequence.name == name:
                return sequence
        raise ValueError(f'Sequence `{name}` not found.')

    def lookup_openfarm(self, plant: Union[Plant, str]) -> OpenFarm.Crop:
        """Look the plant up on OpenFarm.cc

        Args:
            plant (Plant | str): Plant | plant slug.
        """
        slug = plant.openfarm_slug if plant is Plant else plant
        response = requests.get('https://openfarm.cc/api/v1/crops?' + urllib.parse.urlencode({
            'include': 'pictures',
            'filter': slug
        }))
        response.raise_for_status()
        for data in response.json()["data"]:
            if data["type"] == "crops" and data["attributes"]["slug"] == slug:
                return deserialize(OpenFarm.Crop, data)
        raise ValueError(f'Crop `{slug}` not found.')

    def moveto_smart(self, target: Union[Coordinate, Tool, Dict[str, int]], speed: int = 100, offset_x: int = 0, offset_y: int = 0, offset_z: int = 0, travel_height: Optional[int] = 0,
                     proximity_range: int = 20) -> Coordinate:
        """
        Perform a smart movement to the given point.
        :returns The previous position
        """
        position = self.bot_state().location_data.position
        if not self.debug:
            if isinstance(target, Tool):
                target = self.get_toolslots(tool_id=target.id)[0]
            if not isinstance(target, Coordinate):
                target = position.merge(target)
            if (travel_height is not None) and (travel_height > position.z or travel_height > target.z) and (
                    abs(position.x - target.x) > proximity_range or abs(position.y - target.y) > proximity_range):
                # travel height must be respected
                if target.z + offset_z > travel_height:
                    travel_height = target.z + offset_z
                device.move_relative(0, 0, travel_height - position.z, speed)
                device.move_absolute(target.merge({'z': travel_height}).to_coordinate(), speed, device.assemble_coordinate(offset_x, offset_y, 0))
                if abs((target.z + offset_z) - travel_height) <= 2:
                    return position
            device.move_absolute(target.to_coordinate(), speed, device.assemble_coordinate(offset_x, offset_y, offset_z))
        return position
