

class Meteo:
    def __init__(self,
                 data,
                 id,
                 humidite,
                 pluie_intensite_max,
                 pression,
                 type_de_station,
                 pluie,
                 direction_du_vecteur_de_vent_max,
                 direction_du_vecteur_de_rafale_de_vent_max,
                 direction_du_vecteur_vent_moyen,
                 force_moyenne_du_vecteur_vent,
                 force_rafale_max,
                 temperature,
                 heure_de_paris,
                 heure_utc
                 ):
        self.data = data
        self.id = id
        self.humidite = humidite
        self.pluie_intensite_max = pluie_intensite_max
        self.pression = pression
        self.type_de_station = type_de_station
        self.pluie = pluie
        self.direction_du_vecteur_de_vent_max = direction_du_vecteur_de_vent_max
        self.direction_du_vecteur_de_rafale_de_vent_max=(
            direction_du_vecteur_de_rafale_de_vent_max)
        self.direction_du_vecteur_vent_moyen = direction_du_vecteur_vent_moyen
        self.force_moyenne_du_vecteur_vent = force_moyenne_du_vecteur_vent
        self.force_rafale_max = force_rafale_max
        self.temperature = temperature
        self.heure_de_paris = heure_de_paris
        self.heure_utc = heure_utc
