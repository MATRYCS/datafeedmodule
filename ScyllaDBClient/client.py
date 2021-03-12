from cassandra import ConsistencyLevel
from cassandra.cluster import ExecutionProfile, Cluster, EXEC_PROFILE_DEFAULT


class ScyllaClient(object):
    """This object is for handling connection with ScyllaDB"""

    def __init__(self):
        self.exec_profile = ExecutionProfile(
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
            request_timeout=90
        )
        self.cluster = Cluster(
            ['matrycs.epu.ntua.gr'],
            port=9042,
            execution_profiles={EXEC_PROFILE_DEFAULT: self.exec_profile}
        )
        self.session = self.cluster.connect()
        self.session.set_keyspace("matrycs")

    def alter_table(self, **kwargs):
        """This function is used to alter table columns"""
        table = kwargs['table']
        column_name = kwargs['column_name']
        type = kwargs['type']
        try:
            self.session.execute(
                "ALTER TABLE {table} ADD {column_name} {type} ".format(table=table, column_name=column_name, type=type)
            )
        except Exception as ex:
            print(ex)

    def update_table(self, **kwargs):
        """This function is used to update information in cassandra tables"""
        sql_command = kwargs['sql_command']
        self.session.execute(sql_command)
