from .action import Action
from typing import Iterator
from meteo_jobs.models import Station


class ActionStation(Action):

    def __init__(self):
        """"""

    def __launch_meteo_extract():
        """Launch a process to start extract meteo for one station"""


    def execute(self, records: Iterator[Station]) -> Iterator[Station]:
        for station in records:
            print(f"execute for {station.id_nom}")
            yield station
