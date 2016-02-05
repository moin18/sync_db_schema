import db_conf
import subprocess
import os
from sqlparser import SQLParser


class DBUtils(object):

    def __init__(self, db1=db_conf.STAGING_DB, db2=db_conf.PRODUCTION_DB, db1_sql_file='/tmp/db1_sql_file.sql', db2_sql_file='/tmp/db2_sql_file.sql'):
        self.db1 = db1
        self.db2 = db2
        self.db1_sql_file = db1_sql_file
        self.db2_sql_file = db2_sql_file
        self.schema1 = {}
        self.schema2 = {}

    def create_backup_file(self):
        subprocess.check_call(['mysqldump', '-h', self.db1['host'],
                                            ' -u', self.db1['user'],
                                            '-p%s' % self.db1['password'],
                                            '--skip-comments', '--skip-extended-insert', '--skip-data',
                                            self.db1['name'], '>', self.db1_sql_file])
        subprocess.check_call(['mysqldump', '-h', self.db2['host'],
                                            ' -u', self.db2['user'],
                                            '-p%s' % self.db2['password'],
                                            '--skip-comments', '--skip-extended-insert', '--skip-data',
                                            self.db2['name'], '>', self.db1_sql_file])

    def remove_backup_file(self):
        os.remove(self.db1_sql_file)
        os.remove(self.db1_sql_file)

    def show_sql_file_diff(self):
        subprocess.check_call(['diff', self.db1_sql_file, self.db2_sql_file])

    def _get_db_diff(self):
        self.schema1 = SQLParser(self.db1_sql_file).get_db_schema()
        self.schema2 = SQLParser(self.db2_sql_file).get_db_schema()
        schema_diff = {}
        for table1 in self.schema1:
            if table1 in self.schema2:
                schema_diff[table1] = (True, [])
            else:
                schema_diff[table1] = (False, [])

