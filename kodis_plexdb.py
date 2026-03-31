import sqlite3


def dict_factory(cursor, row):
    result = {}
    for idx, col in enumerate(cursor.description):
        result[col[0]] = row[idx]
    return result


class KodisPlexDBHandle(object):
    @classmethod
    def select(cls, query, db_file, timeout=30):
        con = None
        try:
            con = sqlite3.connect(db_file, timeout=timeout)
            con.row_factory = dict_factory
            cur = con.cursor()
            cur.execute(query)
            return cur.fetchall()
        finally:
            if con is not None:
                con.close()

    @classmethod
    def select_arg(cls, query, args, db_file, timeout=30):
        con = None
        try:
            con = sqlite3.connect(db_file, timeout=timeout)
            con.row_factory = dict_factory
            cur = con.cursor()
            if args is None:
                cur.execute(query)
            else:
                cur.execute(query, args)
            return cur.fetchall()
        finally:
            if con is not None:
                con.close()
 
