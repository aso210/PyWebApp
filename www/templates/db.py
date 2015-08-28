#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'aso'

import logging, threading, functools, time

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
        self.transactions = 0

    def is_inited(self):
        return not self.connection is None

    def init(self):
        self.connection = _LasyConnection()
        self.transactions = 0

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

class _TransactionCtx(object):

    def __enter__(self):
        global _dbctx
        self.should_cleanup = False
        if not _dbctx.is_inited():
            _dbctx.init()
            self.should_cleanup = True
        _dbctx.transactions = _dbctx.transactions + 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global _dbctx
        _dbctx.transactions = _dbctx.transactions - 1
        try:
            if _dbctx.transactions == 0:

                if exc_type is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_cleanup:
                _dbctx.cleanup()

    def commit(self):
        global _dbctx
        logging.info('commit transaction...')
        try:
            _dbctx.connection.commit()
            logging.info('commit transaction successed...')
        except:
            logging.info('commit transaction failed...')
            _dbctx.connection.rollback()
            logging.info('rollback transaction successed...')
            raise

    def rollback(self):
        global _dbctx
        logging.info('rollback transaction ...')
        _dbctx.connection.rollback()
        logging.info('rollback transaction successed...')


def transaction():
    return _TransactionCtx()

def _profiling(start, sql=''):
    t = time.time() - start
    if t > 0.1:
        logging.warning('[PROFILING] [DB] %s: %s' % (t, sql))
    else:
        logging.info('[PROFILING] [DB] %s: %s' % (t, sql))

def with_transation(func):
    @functools.wraps(func)
    def wrapper(*args, **kw):
        _start = time.time()
        with transaction():
            return func(*args, **kw)
        _profiling(_start)
    return wrapper

class Dict(dict):

    def __init__(self, names=(), values=(), **kw):
        super(Dict, self).__init__(**kw)
        for k,v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __set__(self, key, value):
        self[key] = value

def _select(sql, isFirst, *args):
    global _dbctx
    sql = sql.replace('?', '%s')
    logging.info('sql= %s  args= %s' % (sql, args))
    cursor = None
    try:
        cursor = _dbctx.connection.cursor()
        cursor.execute(sql, args)
        if cursor.description:
            names = [x[0] for x in cursor.description]
            logging.info('names=%s' % names)
        if isFirst:
            values = cursor.fetchone()
            if not values:
                return None
            else:
                return Dict(names, values)
        values = cursor.fetchall()
        logging.info('values= %s' % values)
        return [Dict(names, x) for x in values]
    finally:
        if cursor:
            cursor.close()

@with_connection
def select(sql, *args):
    return _select(sql, False, *args)

@with_connection
def select_one(sql, *args):
    return _select(sql, True, *args)

@with_connection
def insert(sql, *args):
    return _update(sql, *args)

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

def delete(sql, *args):
    return _update(sql, *args)

class DBError(Exception):
    pass

if __name__== '__main__':
    logging.basicConfig(level=logging.DEBUG)
    creat_engine('aso', '1234', 'mytest')
    # row = insert('insert into student(id, name, age, score) values (?,?,?,?)', 4,'明明',14, 80)
    # with transaction():
    #     insert('insert into student(id, name, age, score) values (?,?,?,?)', 5,'hehe',14, 80)
    #     insert('insert into student(id, name, age, score) values (?,?,?,?)', 6,'hehe',14, 80)
    #     insert('insert into student(id, name, age, score) values (?,?,?,?)', 7,'hehe',14, 80)
    #     insert('insert into student(id, name, age, score) values (?,?,?,?)', 8,'hehe',14, 80)
    # row = delete('delete from student where id=?', 1)
    # print 'row=', row
    result = select('select * from student')
    for s in result:
        print type(s)
        print 'student %s = %s' % (s.id, s)