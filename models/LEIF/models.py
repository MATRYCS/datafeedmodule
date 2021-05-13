from cassandra.cqlengine.models import Model
from cassandra.cqlengine.columns import *

from settings import KEY_SPACE, CONNECTION_NAME


class LeifProject(Model):
    __table_name__ = 'leif_project'
    __keyspace__ = KEY_SPACE
    __connection__ = CONNECTION_NAME

    project_no = Text(primary_key=True)
    project_co2_reduction = Float()
    start_date = Text()
    end_date = Text()
    total_project_costs = Float()
    total_project_costs_scaled = Float()
    grant_financing = Float()
    grant_financing_scaled = Float()
    start_year = Integer()
    start_month = Integer()
    start_day = Integer()
    start_hour = Integer()
    end_year = Integer()
    end_month = Integer()
    end_day = Integer()
    end_hour = Integer()