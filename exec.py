#!/usr/bin/env python3

from os import walk, path, remove
from time import time
from hashlib import sha256
from sqlite3 import connect
import sys
import logging


bin_path = path.abspath(path.dirname(__file__)) + "/"

logging.basicConfig(filename=bin_path + str(time()) +
                    ".log", level=logging.DEBUG)


def options_parser():
    from argparse import ArgumentParser
    parser = ArgumentParser(usage=None)
    parser.add_argument('--src1', dest='folder1')
    parser.add_argument('--src2', dest='folder2')
    parser.add_argument('--no-delete', dest='test', action='store_true')
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
    try:
        with open(filepath, 'rb') as stream:
            digest = sha256(stream.read()).hexdigest()
        return digest
    except:
        return


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
    cursor = db.cursor()
    for path in filelist:
        logging.info(path)
        digest = hasher(path)
        if digest:
            cursor.execute(
                """INSERT INTO '{0}' (path, digest) VALUES (?,?)""".format(table), [path, digest])
    db.commit()
    cursor.close()


def files_deduplicator(bin_path):
    # global bin_path
    # current_time = "table_" + str(int(time())) + ":"
    options = options_parser()
    current_time = int(time())
    path1, path2 = [path.abspath(options.folder1),
                    path.abspath(options.folder2)]
    table1, table2 = [str(current_time), str(current_time+1)]
    files1, files2 = [filelist(path1), filelist(path2)]
    # print(table1,table2)
    # print(options)
    # sys.exit()
    db = database(bin_path + '/1.db', [table1, table2])
    walker(files2, db, table2)
    walker(files1, db, table1)
<<<<<<< HEAD
    data = db.execute(
        """select '{0}'.path as path1 from '{0}' join '{1}' on '{0}'.digest = '{1}'.digest""".format(table2, table1))
    for filepath in data.fetchall():
        logging.debug(filepath)
        try:
            remove(filepath[0])
        except:
            continue
=======
    db_cur=db.cursor()
    data = db_cur.execute('select path from ? join ?  on ?.digest = ?.digest',(table2,table1,table2,table1,))
    if options.test:
        print(1)
        for filepath in data.fetchall():
            logging.debug(filepath)
    else:
        print(2)
        # for filepath in data.fetchall():
        #     logging.debug(filepath)

            # remove(filepath[0])
>>>>>>> 7a612acc5914db910277e39428677dfba7a2fdb7


if __name__ == "__main__":
    try:
        files_deduplicator(bin_path)
    except KeyboardInterrupt:
        sys.exit()
