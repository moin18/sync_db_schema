import re


class SQLParser(object):

    def __init__(self, sql_file):
        self.sql_file = sql_file
        self._file_content = None

        # Load sql file
        with open(self.sql_file, 'r') as f:
            self._file_content = f.read()

    def get_tables(self):
        tables = re.findall('CREATE TABLE (.*\n?)\(', self._file_content, re.MULTILINE)
        return [table.strip('` ') for table in tables]

    def get_table_info(self, table):
        table_data = re.find('CREATE TABLE {table_name} (.*\n?)\)\n\))'.format(table), self._file_content, re.MULTILINE)
        return table_data

    def get_db_schema(self):
        schema = {}
        for table in self.get_tables():
            schema[table] = []
            for column in self.get_table_info(table):
                schema[table].append(column)
        return schema