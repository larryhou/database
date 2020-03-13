#!/usr/bin/env python3
import mysql.connector
import xlrd
from mysql.connector.cursor import MySQLCursor
from mysql.connector.connection import MySQLConnection

class TranslationSynchronizer(object):
    def __init__(self, database, name='language'):
        self.name = name
        self.database = database # type: MySQLConnection
        self.cursor = database.cursor() # type: MySQLCursor
        self.create_database(database_name=self.name)
        self.active_database(database_name=self.name)

    def active_database(self, database_name):
        self.cursor.execute('USE {}'.format(database_name))
        self.cursor.execute('SELECT DATABASE()')
        self.cursor.fetchall()

    def create_database(self, database_name):
        command = 'CREATE DATABASE IF NOT EXISTS {}'.format(database_name)
        print('+ {}'.format(command))
        self.cursor.execute(command)

    def create_table(self, table_name, schemas):  # type: (str)->None
        command = 'CREATE TABLE IF NOT EXISTS {} ({})'.format(table_name, ','.join(schemas))
        print('+ {}'.format(command))
        self.cursor.execute(command)

    def import_excel(self, excel, table_name):  # type: (xlrd.book.Book, str)->None
        self.create_table(table_name, schemas=['label VARCHAR(128) PRIMARY KEY', 'chinese TEXT CHARSET utf8mb4', 'translation TEXT CHARSET utf8mb4'])
        for sheet_name in excel.sheet_names(): # type: str
            if not sheet_name.isupper(): continue
            sheet = excel.sheet_by_name(sheet_name)
            records = []
            for r in range(1, sheet.nrows):
                data = []
                for c in range(3):
                    cell = sheet.cell(r, c)
                    data.append(cell.value)
                records.append(data)
            self.cursor.executemany('INSERT INTO {} VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE chinese=VALUES(chinese),translation=VALUES(translation)'.format(table_name), records)
        self.database.commit()

def main():
    import argparse, sys, os
    arguments = argparse.ArgumentParser()
    arguments.add_argument('--host', default='localhost')
    arguments.add_argument('--username', '-u', default='root')
    arguments.add_argument('--password', '-p', required=True)
    arguments.add_argument('--scanpath', '-s', required=True)
    options = arguments.parse_args(sys.argv[1:])
    assert os.path.isdir(options.scanpath)
    database = mysql.connector.connect(host=options.host, user=options.username, password=options.password)
    assert database.is_connected()
    synchronizer = TranslationSynchronizer(database)
    for language in os.listdir(options.scanpath):
        filepath = '{}/{}/library.xls'.format(options.scanpath, language)
        if not os.path.isfile(filepath): continue
        excel = xlrd.open_workbook(filepath)
        synchronizer.import_excel(excel, table_name=language)
    database.commit()
    database.close()




if __name__ == '__main__':
    main()