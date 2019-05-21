import datetime
import itertools
from abc import abstractmethod

from Farmbot import *
from typing import *
from utils import Entity, dump_datetime, parse_datetime
from farmware_tools import app


class HourlyWeather(Entity):
    rain: float
    sun: float
    temperature: float
    wind: float


class Config(Entity):
    location: str
    maxage_hours: int


def get_weather_point(farmware: Farmware) -> Point:
    """
    Get the weather GenericPointer, or an empty Point instance
    """
    try:
        return farmware.get_genericpointers(name='Weather')[0]
    except IndexError:
        return Point(pointer_type='GenericPointer', name='Weather', meta={})


def load_weather(farmware: Farmware) -> Dict[str, HourlyWeather]:
    """
    Return the weather data currently stored
    """
    return get_factory(Dict[str, HourlyWeather])(get_weather_point(farmware).meta)


class Weather(Farmware[Config]):
    zip: int

    def __init__(self, manifest_name: Optional[str] = None):
        super().__init__(Config, manifest_name)

    def execute(self):
        """
        Refresh the weather data from the source and return it
        """
        point = get_weather_point(self)
        weather = get_factory(Dict[str, HourlyWeather])(point.meta)
        self.update_weather(weather)
        limit = dump_datetime(datetime.utcnow() - timedelta(hours=(self.config.maxage_hours or 96) + 1))  # The dates are in a sortable format
        for key in [key for key in weather if key <= limit]:  # clone keys since we're going to remove items
            del weather[key]
        point.meta = weather
        app.log(f"Storing {len(weather)} hourly weather records...")
        self.put_point(point)

    @abstractmethod
    def update_weather(self, weather):
        """
        Query the weather source and add/update the weather information
        """
        pass


class MeteoswissWeather(Weather):
    def update_weather(self, weather):
        zip = int(self.config.location)
        if not (1000 <= zip <= 9999):
            raise ValueError("Invalid Swiss ZIP code")
        date = datetime.utcnow()
        app.log(f"Fetching weather data...")
        for ix in range(60):
            url = f"https://www.meteoschweiz.admin.ch/product/output/forecast-chart/version__{(date - timedelta(minutes=ix)).strftime('%Y%m%d_%H%M')}/de/{zip}00.json"
            response = requests.get(url)
            if response.status_code != 404:
                break
        else:
            raise ValueError("No JSON data found within an hour")
        response.raise_for_status()
        data = response.json()
        for key, instant, value in itertools.chain(
                (('rain', dump_datetime(parse_datetime(hour[0])), hour[1]) for day in data for hour in day['rainfall']),
                (('sun', dump_datetime(parse_datetime(hour[0])), hour[1] / 100) for day in data for hour in day['sunshine']),
                (('temperature', dump_datetime(parse_datetime(hour[0])), hour[1]) for day in data for hour in day['temperature']),
                (('wind', dump_datetime(parse_datetime(hour[0])), hour[1]) for day in data for hour in day['wind']['data'])):
            hour_weather = weather.get(instant)
            if not hour_weather:
                hour_weather = HourlyWeather()
                weather[instant] = hour_weather
            setattr(hour_weather, key, value)
