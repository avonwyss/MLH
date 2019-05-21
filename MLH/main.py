import sys
import traceback

import requests
from typing import *
from farmware_tools import device
from Farmbot import Farmware
from Weather import MeteoswissWeather

if __name__ == '__main__':

    device.log(f'Args: {str(sys.argv)}', 'debug')
    try:
        app_name = None if len(sys.argv) < 2 else sys.argv[1].lower()
        manifest_name = None if len(sys.argv) < 3 else sys.argv[2].lower()
        app: Optional[Farmware] = None
        if app_name == 'meteoswissweather':
            app = MeteoswissWeather(manifest_name)
        if not app:
            device.log(f'Farmware not found: {str(app_name)}', 'error')
            sys.exit(2)
        app.execute()
        sys.exit(0)

    except requests.exceptions.HTTPError as error:
        device.log(f'HTTP error {error.response.status_code} {error.response.text[0:100]} ', 'error')
    except Exception as ex:
        device.log(f"Something went wrong: {''.join(traceback.format_exception(etype=type(ex), value=ex, tb=ex.__traceback__))}", 'error')
    sys.exit(1)
