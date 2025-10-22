
from dataclasses import dataclass

@dataclass
class Station:
    id_numero: int
    id_nom: str
    longitude: float
    latitude: float
    altitude: float
    emission: str
    installation: str
    type_stati: str
    lcz: int
    ville: str
    bati: str
    veg_haute: str
    geopoint: str