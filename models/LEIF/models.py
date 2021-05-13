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


class LeifActivity(Model):
    __table_name__ = 'leif_activity'
    __keyspace__ = KEY_SPACE
    __connection__ = CONNECTION_NAME

    project_no = Text(primary_key=True)
    project_activity = Text(primary_key=True)
    type_of_activity = Text(primary_key=True, clustering_order='DESC')
    type_of_building = Text(primary_key=True, clustering_order='DESC')
    co2_reduction = Float()
    co2_reduction_scaled = Float()
    energy_reduction = Float()
    energy_reduction_scaled = Float()
    co2_emission_factor = Float()
    co2_emission_factor_scaled = Float()
