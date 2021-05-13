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


# LEIF MAIN DATASET TABLES BELLOW
class LeifBuilding(Model):
    __table_name__ = 'leif_building'
    __keyspace__ = KEY_SPACE
    __connection__ = CONNECTION_NAME

    house_nr = Integer(primary_key=True)
    year_of_building = Integer(primary_key=True, clustering_order='DESC')
    number_of_floors = Integer()
    total_heating_area = Float()
    total_heating_area_scaled = Float()
    house_functions = Text()
    heating_source_of_heat = Text()
    heating_co2_emission_factor = Float()
    heating_co2_emission_factor_scaled = Float()
    heating_co2_emission_factor_audit = Float()
    heating_co2_emission_factor_audit_scaled = Float()
    hot_water_source_of_heat = Text()
    hot_water_co2_emission_factor = Float()
    hot_water_co2_emission_factor_scaled = Float()
    electricity_co2_emission_factor = Float()
    electricity_co2_emission_factor_scaled = Float()


class LeifBeforeProjectAvg(Model):
    __table_name__ = 'leif_before_project_avg'
    __keyspace__ = KEY_SPACE
    __connection__ = CONNECTION_NAME

    house_nr = Integer(primary_key=True)
    project_no = Text(primary_key=True, clustering_order='DESC')
    heating_total_consumption = Float()
    heating_total_consumption_scaled = Float()
    heating_co2_emission = Float()
    heating_co2_emission_scaled = Float()
    hot_water_total_consumption = Float()
    hot_water_total_consumption_scaled = Float()
    hot_water_co2_emission = Float()
    hot_water_co2_emission_scaled = Float()
    electricity_total_consumption = Float()
    electricity_total_consumption_scaled = Float()
    electricity_co2_emission = Float()
    electricity_co2_emission_scaled = Float()


class ReportingYearData(Model):
    __table_name__ = 'leif_reporting_year_data'
    __keyspace__ = KEY_SPACE
    __connection__ = CONNECTION_NAME

    house_nr = Integer(primary_key=True)
    project_no = Text(primary_key=True, clustering_order='DESC')
    heating_total_consumption = Float()
    heating_total_consumption_scaled = Float()
    heating_co2_emission = Float()
    heating_co2_emission_scaled = Float()
    hot_water_total_consumption = Float()
    hot_water_total_consumption_scaled = Float()
    hot_water_co2_emission = Float()
    hot_water_co2_emission_scaled = Float()
    electricity_total_consumption = Float()
    electricity_total_consumption_scaled = Float()
    electricity_co2_emission = Float()
    electricity_co2_emission_scaled = Float()
    renewable_energy_produced_energy = Float()
    renewable_energy_produced_energy_scaled = Float()
    renewable_energy_co2_emission = Float()
    renewable_energy_co2_emission_scaled = Float()


class TotalAfterProject(Model):
    __table_name__ = 'leif_totals_after_project'
    __keyspace__ = KEY_SPACE
    __connection__ = CONNECTION_NAME

    house_nr = Integer(primary_key=True)
    project_no = Text(primary_key=True, clustering_order='DESC')
    total_energy_consumption_mwh = Float()
    total_energy_consumption_mwh_scaled = Float()
    total_energy_consumption_kwh_m2 = Float()
    total_energy_consumption_kwh_m2_scaled = Float()
    co2_emission = Float()
    co2_emission_scaled = Float()
    co2_emission_reduction = Float()
    co2_emission_reduction_scaled = Float()