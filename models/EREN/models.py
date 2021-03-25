from cassandra.cqlengine.models import Model
from cassandra.cqlengine.columns import *

from settings import KEY_SPACE, CONNECTION_NAME


class Building(Model):
    __table_name__ = 'building'
    __keyspace__ = KEY_SPACE
    __connection__ = CONNECTION_NAME

    registration_number = Text(primary_key=True)
    registration_date = Text()
    registration_year = Integer()
    registration_month = Integer()
    registration_day = Integer()
    building_use = Text()
    building_use_encoded = Integer()
    direction = Text()
    longitude = Float()
    longitude_scaled = Float()
    latitude = Float()
    latitude_scaled = Float()
