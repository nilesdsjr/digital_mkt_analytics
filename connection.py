from psycopg2 import connect, Error

'''Responsable for getting connections with databases.'''
class Connection:

    def __init__(self):
        self.host="localhost"
        self.port=15432
        self.database="postgres"
        self.user="postgres"
        self.pswd="Postgres"

    def psql_conn(self):
        return connect(
        host=self.host,
        port=self.port,
        database=self.database,
        user=self.user,
        password=self.pswd)
