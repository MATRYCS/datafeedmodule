from cassandra.cqlengine.models import Model
from cassandra.cqlengine.columns import *

from settings import KEY_SPACE, CONNECTION_NAME


class BtcTower(Model):
    __table_name__ = 'btc_tower'
    __keyspace__ = KEY_SPACE
    __connection__ = CONNECTION_NAME

    Timestamp = Text()
    Year = Integer(primary_key=True)
    Month = Integer(primary_key=True)
    Day = Integer()
    Hour = Integer()
    Location = Text()
    energy_source = Text()
    measure = Text()
    unit_of_measure = Text()
    interval = Text()
    value = Float()
