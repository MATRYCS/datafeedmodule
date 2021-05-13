from cassandra.cqlengine.models import Model
from cassandra.cqlengine.columns import *

from settings import KEY_SPACE, CONNECTION_NAME


class PalmelaHourlyProduction(Model):
    __table_name__ = 'palmela_hourly_production'
    __keyspace__ = KEY_SPACE
    __connection__ = CONNECTION_NAME

    Timestamp = Text(primary_key=True)
    Year = Integer()
    Month = Integer()
    Day = Integer()
    Hour = Integer()
    Produced = Float()
    Produced_scaled = Float()
    Specific = Float()
    AvoidedCO2 = Float()
