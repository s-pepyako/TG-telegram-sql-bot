# -*- coding: utf-8 -*-
from config import shelve_req_ids, db_host, db_read_user, db_read_pass, db_write_user, db_write_pass, log_file
import psycopg2
import psycopg2.extras
import shelve
import logging



FORMAT = "%(asctime)-15s - %(levelname)s - %(message)s"
logger = logging.getLogger('Tbot')
handler = logging.FileHandler(log_file)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter(FORMAT))
logger.addHandler(handler)


def do_pg_select(query, db):
    conn_string = 'host=' + db_host + \
                  ' dbname=' + db + \
                  ' user=' + db_read_user + \
                  ' password=' + db_read_pass + ''
    try:
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    except:
        return ['error'],['Ask','your','system','administrator']
    try:
        cursor.execute(query.replace('\n', ' '))
        records = cursor.fetchall()
        lines_num = cursor.rowcount
        print("rowcount: " + str(lines_num))
        column_names = [desc[0] for desc in cursor.description]
        if lines_num < 1:
            return column_names, ['0']
        print(column_names, records)
        return column_names, records
    except psycopg2.Error as e:
        return e, e
    finally:
        conn.close()

def do_update_query(query, db):
    conn_string = 'host=' + db_host + ' dbname=' + db + ' user=' + db_write_user + ' password=' + db_write_pass + ''
    try:
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
    except:
        return 'error'
    try:
        cursor.execute(query)
        conn.commit()
        return cursor.rowcount
    except psycopg2.Error as e:
        return e
    finally:
        conn.close()


def add_request_id(req, database):
    db = shelve.open(shelve_req_ids)
    try:
        if len(db) > 1:
            last_id = db['counter']
            id = last_id + 1
            db[str(id)] = [req, database]
            db['counter'] = id
        elif len(db) == 1:
            id = 1
            db[str(id)] = [req, database]
            db['counter'] = 1
        elif len(db) == 0:
            db['counter'] = 1
            id = 1
            db[str(id)] = [req, database]
    except:
        return None
    finally:
        db.close()
    return str(id)

def remove_request(id):
    key_id = str(id)
    try:
        db = shelve.open(shelve_req_ids)
        if db[key_id] is not None:
            del db[key_id]
            return key_id + ': Removed'
        else:
            return key_id + ': No such request'
    except:
        return 'Something goes wrong'
    finally:
        db.close()

def confirm_request(id):
    key_id = str(id)
    try:
        db = shelve.open(shelve_req_ids)
        if db[key_id] is not None:
            query = db[key_id]
            upd_row = do_update_query(query[0], query[1])
            del db[key_id]
            if not upd_row: upd_row = 0
            return key_id + ': updated ' + str(upd_row) + ' record(s)'
        else:
            return key_id + ': No such request'
    except:
        return 'Something goes wrong'
    finally:
        db.close()

def list_queries():
    db = shelve.open(shelve_req_ids)
    query_list = dict()
    try:
        key_list = list(db.keys())
        key_list.sort()
        if 'counter' in key_list:
            key_list.remove('counter')
        for key in key_list:
            query_list[key] = db[key]
        if len(query_list) > 0:
            return query_list
            db.close()
        else:
            return {'0': ['Queue is empty', ' :( ']}
            db.close()
    except:
        return {'Response': 'Error'}
    finally:
        db.close()
