from os import walk, path
from hashlib import sha256
from sqlite3 import connect


def options_parser():
    from argparse import ArgumentParser
    parser = ArgumentParser(usage=None)
    parser.add_argument('folder1')
    parser.add_argument('folder2')
    args = parser.parse_args()
    return args


def filelist(folder):
    file_list = list()
    for root, subdirs, file_name in walk(path.abspath(folder)):
        if file_name:
            for fn in file_name:
                current_object = root + '/' + fn
                file_list.append(current_object)
    return file_list


def hasher(filepath):
    with open(filepath, 'rb') as stream:
        digest = sha256(stream.read()).hexdigest()
    return digest


def bd(db_file=None):
    def sub_routine():
        cursor = db.cursor()
        cursor.execute(
            'CREATE TABLE files (path TEXT NOT NULL, digest VARCHAR(65) NOT NULL)')
        cursor.execute('CREATE INDEX files_index ON files (path, digest)')
        db.commit()
        cursor.close()
    if db_file:
        return connect(filename)
    db = connect(':memory:')
    sub_routine()
    return db


def walker(filelist, db):
    cursor = db.cursor()
    for path in filelist:
        print(path)
        digest = hasher(path)
        cursor.execute('INSERT INTO files (path, digest) VALUES (?,?)',[path,digest])
    db.commit()
    cursor.close()
    print(1)
    print(db.execute('select * from files').fetchall())



def main():
    d = bd()
    folders = options_parser()
    files1 = filelist(folders.folder1)
    files2 = filelist(folders.folder2)
    walker(files2,d)
    # print (files2)


main()
