__author__ = 'aso'

import logging, threading, functools

# global engine object:
engine = None

class _Engine(object):
    def __init__(self, connect):
        self._connect = connect

    def connect(self):
        return self._connect()

def creat_engine(user, password, database, host='127.0.0.1', port='3306', **kw):
    global engine
    import mysql.connector
    if engine is not None:
        raise DBError('Engine is already initialized.')
    params = dict(user=user, password=password, database=database, host=host, port=port)
    defaults = dict(use_unicode=True, charset='utf8', collation='utf8_general_ci', autocommit=False)
    for k, v in defaults.iteritems():
        params[k] = kw.pop(k, v)
    params.update(kw)
    params['buffered'] = True
    # unix_socket='/tmp/mysql.sock'
    params['unix_socket'] = '/tmp/mysql.sock'
    # mysql.connector.connect(**params)
    engine = _Engine(lambda : mysql.connector.connect(**params))

    # test connection...
    logging.info('Init mysql engine <%s> ok.' % hex(id(engine)))

class _LasyConnection(object):

    def __init__(self):
        self.connection = None

    def cursor(self):
        if self.connection is None:
            connection = engine.connect()
            logging.info('open connection <%s>...' % hex(id(connection)))
            self.connection = connection
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        if self.connection:
            connnection = self.connection
            self.connection = None
            logging.info('close connection <%s>...' % hex(id(connnection)))
            connnection.close()

class _DBCtx(threading.local):

    def __init__(self):
        self.connection = None

    def is_inited(self):
        return not self.connection is None

    def init(self):
        self.connection = _LasyConnection()

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        return self.connection.cursor()

_dbctx = _DBCtx()

class _ConnectionCtx(object):
    def __enter__(self):
        global _dbctx
        self.should_cleanup = False
        if not _dbctx.is_inited():
            _dbctx.init()
            self.should_cleanup = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global _dbctx
        if self.should_cleanup:
            _dbctx.cleanup()

def connection():

    return _ConnectionCtx()

def with_connection(func):
    @functools.wraps(func)
    def wrapper(*args, **kw):
        with connection():
            return func(*args, **kw)
    return wrapper

@with_connection
def _update(sql, *args):
    global _dbctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('SQL: %s, ARGS: %s' % (sql, args))
    try:
        cursor = _dbctx.connection.cursor()
        cursor.execute(sql, args)
        r = cursor.rowcount
        logging.info('r=%s' % r)
        _dbctx.connection.commit()
        return r
    finally:
        if cursor:
            cursor.close()

def update(sql, *args):
    return _update(sql, *args)


class DBError(Exception):
    pass

if __name__== '__main__':
    logging.basicConfig(level=logging.DEBUG)
    creat_engine('aso', '1234', 'my_test')
    update("update student set name=? where id=?", 'hahhaha', '1')