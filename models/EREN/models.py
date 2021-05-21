from cassandra.cqlengine.models import Model
from cassandra.cqlengine.columns import *

from settings import KEY_SPACE, CONNECTION_NAME


class Building(Model):
    __table_name__ = 'eren_building'
    __keyspace__ = KEY_SPACE
    __connection__ = CONNECTION_NAME

    registration_number = Text(primary_key=True)
    registration_date = Text()
    registration_year = Integer()
    registration_month = Integer()
    registration_day = Integer()
    municipality = Text()
    building_use = Text()
    building_use_encoded = Integer()
    province = Text()
    province_encoded = Integer()
    direction = Text()
    longitude = Float()
    longitude_scaled = Float()
    latitude = Float()
    latitude_scaled = Float()


class BuildingCo2Emission(Model):
    __table_name__ = 'eren_building_co2_emission'
    __keyspace__ = KEY_SPACE
    __connection__ = CONNECTION_NAME

    registration_number = Text(primary_key=True)
    co2_emission_rating = Text()
    co2_emission_rating_encoded = Integer()
    co2_emission_ratio = Float()
    co2_emission_ratio_scaled = Float()


class BuildingPrimaryConsumption(Model):
    __table_name__ = 'eren_building_prim_cons'
    __keyspace__ = KEY_SPACE
    __connection__ = CONNECTION_NAME

    registration_number = Text(primary_key=True)
    primary_energy_rating = Text()
    primary_energy_rating_encoded = Integer()
    primary_energy_ratio = Float()
    primary_energy_ratio_scaled = Float()


class BuildingHeatingDemand(Model):
    __table_name__ = 'eren_building_heating_demand'
    __keyspace__ = KEY_SPACE
    __connection__ = CONNECTION_NAME

    registration_number = Text(primary_key=True)
    heating_demand_rating = Text()
    heating_demand_rating_encoded = Integer()
    heating_demand_ratio = Float()
    heating_demand_ratio_scaled = Float()


class BuildingCoolingDemand(Model):
    __table_name__ = 'eren_building_cooling_demand'
    __keyspace__ = KEY_SPACE
    __connection__ = CONNECTION_NAME

    registration_number = Text(primary_key=True)
    cooling_demand_rating = Text()
    cooling_demand_rating_encoded = Integer()
    cooling_demand_ratio = Float()
    cooling_demand_ratio_scaled = Float()
