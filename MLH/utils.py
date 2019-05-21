import ast
import json
from typing import *
from datetime import *
import re

# timezone
tz = 0


def parse_offset(offset: Optional[str], days: bool = False) -> Optional[timedelta]:
    """Parse a date offset, such as "in x days" or "x days ago" """
    if offset:
        offset = re.sub('\\s+', ' ', offset.strip().lower())
        if days:
            if offset == 'today':
                return timedelta()
            if offset == 'yesterday':
                return timedelta(days=-1)
            if offset == 'tomorrow':
                return timedelta(days=1)
            pattern = 'year|week|day'
        else:
            if offset == 'now':
                return timedelta()
            pattern = 'year|week|day|hour|minute|second'
        match = re.fullmatch(f'in ([0-9]+) ({pattern})s?', offset)
        if match:
            return timedelta(**{match.group(2) + 's': int(match.group(1))})
        match = re.fullmatch(f'([0-9]+) ({pattern})s? ago', offset)
        if match:
            return timedelta(**{match.group(2) + 's': -int(match.group(1))})


def parse_datetime(val: Union[datetime, str, int, float, None], default: Optional[datetime] = None) -> Optional[datetime]:
    """Parse an ISO datetime ("YYYY-MM-DDThh:mm:ss.fffZ") in UTC. May be an offset relative to datetime.utcnow (see parse_offset)."""
    if not val:
        return default
    if isinstance(val, datetime):
        return val
    if isinstance(val, int) or isinstance(val, float):  # unix or JS timestamp
        return datetime.utcfromtimestamp(val if val < 10000000000 else val / 1000)
    offset = parse_offset(val, False)
    if offset:
        return datetime.utcnow() + offset
    return datetime.strptime(val.strip(), "%Y-%m-%dT%H:%M:%S.%fZ")


def parse_date(val: Union[date, str, int, float, None], default: Optional[date] = None) -> Optional[date]:
    """Parse a ISO date ("YYYY-MM-DD") in UTC. May be an offset relative to datetime.utcnow (see parse_offset)."""
    if not val:
        return default
    if isinstance(val, date):
        return val
    if isinstance(val, int) or isinstance(val, float):  # unix or JS timestamp
        return datetime.utcfromtimestamp(val if val < 10000000000 else val / 1000).date()
    offset = parse_offset(val, True)
    if offset:
        return datetime.utcnow().date() + offset
    return datetime.strptime(val.strip(), "%Y-%m-%d").date()


def dump_datetime(val: Optional[datetime]) -> Optional[str]:
    """Dump an instant as ISO string ("YYYY-MM-DDThh:mm:ss.fffZ")"""
    if val:
        return val.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def dump_date(val: Optional[date]) -> Optional[str]:
    """Dump an instant as ISO date ("YYYY-MM-DD")"""
    if val:
        return val.strftime("%Y-%m-%d")


def local_to_utc(date: datetime) -> datetime:
    """Convert a local datetime to a UTC datetime"""
    return date - timedelta(hours=tz)


def utc_to_local(date: datetime) -> datetime:
    """Convert a UTC datetime to a local datetime"""
    return date + timedelta(hours=tz)


def utc_now() -> datetime:
    """Get the current datetime as UTC"""
    return datetime.utcnow()


def local_now() -> datetime:
    """Get the current datetime as local"""
    return utc_to_local(utc_now())


class Entity(object):
    def __new__(cls, __dict__: Dict[str, Any] = None, *args, **kwargs):
        self = super().__new__(cls)
        if __dict__:
            self.__dict__ = __dict__
        for key in get_factory(cls).__self__.get_props():
            self.__dict__.setdefault(key)
        return self

    def __init__(self, __dict__: Dict[str, Any] = None, *args, **kwargs):
        super().__init__()
        if kwargs:
            self.__dict__.update(**kwargs)

    def __str__(self):
        return self.__dict__.__str__()

    def __repr__(self):
        return self.__dict__.__repr__()


TEntity = TypeVar("TEntity", bound=Entity)

factories: Dict[type, Callable[[Any], Any]] = {
    Any: lambda val: val,
    bool: lambda val: bool(ast.literal_eval(val) if isinstance(val, str) else val),
    str: str,
    int: lambda val: int(ast.literal_eval(val) if isinstance(val, str) else val),
    float: lambda val: float(ast.literal_eval(val) if isinstance(val, str) else val),
    date: parse_date,
    datetime: parse_datetime
}


class EntityFactory(object):
    __slots__ = ('cls', 'props')

    cls: Type[TEntity]
    props: Optional[Dict[str, Callable[[Any], Any]]]

    def __init__(self, cls: Type[TEntity]):
        self.cls = cls
        self.props = None

    def get_props(self):
        if not self.props:
            self.props = {item[0]: get_factory(item[1]) for item in get_type_hints(self.cls).items()}
        return self.props

    def make(self, data: Dict[str, Any]) -> TEntity:
        for key, factory in self.get_props().items():
            data[key] = factory(data.get(key))
        return self.cls(__dict__=data)


TAny = TypeVar("TAny")


def get_factory(expected_type: Type[TAny]) -> Callable[[Any], TAny]:
    global factories
    factory: Callable[[Any], Any] = factories.get(expected_type)
    if factory:
        return factory
    origin_type = getattr(expected_type, '__origin__', None)
    if origin_type == Union:
        nullable: bool = False
        for type_ in expected_type.__args__:
            if type_ == type(None):
                nullable = True
            elif not factory:
                factory = get_factory(type_)
            else:
                raise TypeError('Union with multiple non-None types is not supported')
        if nullable:
            factory = (lambda val: None if val in (None, 'None') else factory(val)) if factory else (lambda val: None)
    elif origin_type in (List, list):
        item_type = getattr(expected_type, '__args__', expected_type.__parameters__)[0]
        item_factory: Callable[[Any], Any] = get_factory(item_type)
        factory = lambda val: [item_factory(item) for item in (ast.literal_eval(val) if isinstance(val, str) else val)]
    elif origin_type in (Dict, dict):
        key_type, value_type = getattr(expected_type, '__args__', expected_type.__parameters__)
        key_factory: Callable[[Any], Any] = get_factory(key_type)
        value_factory: Callable[[Any], Any] = get_factory(value_type)
        factory = lambda val: {key_factory(item[0]): value_factory(item[1]) for item in (ast.literal_eval(val) if isinstance(val, str) else val).items()}
    elif issubclass(expected_type, Entity):
        factory = EntityFactory(cast(Any, expected_type)).make
    if not factory:
        raise TypeError('Unsupported type ' + str(expected_type))
    factories[expected_type] = factory
    return factory


def __json_encode_entity(self, obj: Any) -> Any:
    """Function used to monkey-patch json.JSONEncoder.default to add support for serialization"""
    if isinstance(obj, Entity):
        return json.dumps(obj.__dict__)
    if isinstance(obj, datetime):
        return dump_datetime(obj)
    if isinstance(obj, date):
        return dump_date(obj)
    return __json_encode_entity.original(self, obj)


__json_encode_entity.original = json.JSONEncoder.default
json.JSONEncoder.default = __json_encode_entity
