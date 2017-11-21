#!/usr/bin/env python3

from os import walk, path, remove
from time import time
from hashlib import sha256
from sqlite3 import connect
import sys
import logging


def options_parser():
    from argparse import ArgumentParser
    parser = ArgumentParser(usage=None)
    parser.add_argument('folder1')
    parser.add_argument('folder2')
    args = parser.parse_args()
    return args


def filelist(folder):
    file_list = list()
    for root, subfolders, file_name in walk(folder):
        if file_name:
            for fn in file_name:
                current_object = root + '/' + fn
                file_list.append(current_object)
    return file_list


def hasher(filepath):
    with open(filepath, 'rb') as stream:
        digest = sha256(stream.read()).hexdigest()
    return digest


def database(db_file=None, names=None):
    def create_tables():
        cursor = db.cursor()
        for name in names:
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS \"{0}\" (path TEXT NOT NULL, digest VARCHAR NOT NULL)""".format(name))
            cursor.execute("""CREATE INDEX IF NOT EXISTS \"{0}\" ON \"{1}\" (digest)""".format(
                name + "_index", name))
        db.commit()
        cursor.close()
    if db_file:
        db = connect(db_file)
    else:
        db = connect(':memory:')
    create_tables()
    return db


def walker(filelist, db, table):
    print(db)
    cursor = db.cursor()
    for path in filelist:
        digest = hasher(path)
        cursor.execute(
            """INSERT INTO '{0}' (path, digest) VALUES (?,?)""".format(table), [path, digest])
    db.commit()
    cursor.close()


def main():
    bin_path = path.abspath(path.dirname(__file__))
    folders = options_parser()
    current_time = "table_" + str(int(time())) + ":"
    path1 = path.abspath(folders.folder1)
    path2 = path.abspath(folders.folder2)
    table1 = current_time + path1
    table2 = current_time + path2
    # print([table1,table2])
    db = database(bin_path + '/1.db', [table1, table2])
    # sys.exit()
    files1 = filelist(path1)
    files2 = filelist(path2)
    walker(files2, db, table2)
    walker(files1, db, table1)
    data = db.execute(
        """select '{0}'.path as path1 from '{0}' join '{1}' on '{0}'.digest = '{1}'.digest""".format(table2, table1))
    for filepath in data.fetchall():
        remove(filepath[0])
    new_files=


print(main())
