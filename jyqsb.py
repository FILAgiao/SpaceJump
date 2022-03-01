# -*- coding:utf-8 -*-
# project:Spacejump!居间人设备（手动狗头)用于数据库之间的定时自动传输
# user:GIAO
# Author: FILAgiao
# createtime: 2021/12/23 14:25
# DONE:为了避免传参出问题，其实可以把与连接有关的属性和操作封装成为一个类
import configparser
import datetime
import time
import traceback
import pymysql
import time
import os
import sys

class Logger(object):

    def __init__(self, stream=sys.stdout):
        output_dir = "log"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        log_name = '{}.log'.format(time.strftime('%Y-%m-%d-%H-%M'))
        filename = os.path.join(output_dir, log_name)

        self.terminal = stream
        self.log = open(filename, 'a+')

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        pass

def ins2db(conn, datalist, tablename, ziduan_len):
    cursor = conn.cursor()
    sql = ins2big_sql(tablename, ziduan_len)
    # 批量插入
    sql_fk = 'SELECT @@FOREIGN_KEY_CHECKS;'
    cursor.execute(sql_fk)
    fk_flag = cursor.fetchone()[0]
    try:
        if fk_flag == 1:
            sql_fk = 'SET foreign_key_checks = 0;'
            cursor.execute(sql_fk)
            conn.commit()
            cursor.executemany(sql, datalist)
            conn.commit()
            sql_fk = 'SET foreign_key_checks = 1;'
            cursor.execute(sql_fk)
            conn.commit()
        elif fk_flag == 0:
            cursor.executemany(sql, datalist)
            conn.commit()
    except:
        print(traceback.format_exc())


def ins2big_sql(tablename, ziduan_length):
    sql = "INSERT INTO {} VALUES".format(tablename)
    jjrsb = []
    for i in range(ziduan_length):
        jjrsb.append('%s')
    sql = sql + "(" + ','.join(jjrsb) + ")"
    return sql


def truncate_all(conn):
    try:
        cursor = conn.cursor()
        sql = "show tables"
        cursor.execute(sql)
        a = cursor.fetchall()
        sql_fk = 'SELECT @@FOREIGN_KEY_CHECKS;'
        cursor.execute(sql_fk)
        fk_flag = cursor.fetchone()[0]
        if fk_flag == 1:
            sql_fk = 'SET foreign_key_checks = 0;'
            cursor.execute(sql_fk)
            conn.commit()
            for table in a:
                sql = 'TRUNCATE TABLE ' + table[0]
                cursor.execute(sql)
            sql_fk = 'SET foreign_key_checks = 1;'
            cursor.execute(sql_fk)
            conn.commit()
        elif fk_flag == 0:
            for table in a:
                sql = 'TRUNCATE TABLE ' + table[0]
                cursor.execute(sql)
    except Exception as e:
        print(traceback.format_exc(e))
    finally:
        now = datetime.datetime.now()
        print(now.strftime("%Y-%m-%d %H:%M:%S") + ':完成了一次TRUNCATE')

sys.stdout = Logger(sys.stdout)  #  将输出记录到log
sys.stderr = Logger(sys.stderr)  # 将错误信息记录到log
config = configparser.ConfigParser()
# config.read('table_name.ini', encoding='utf-8')
config.read('/home/code/jyqsb/table_name.ini', encoding='utf-8')
# 基础config

# config_login = config['local_test']  # fixme:only in test
base_config = config['base_config']
do_flag = base_config['write1transfer2']
# 具体读取的表单
config_name = config['table_name']
resr_cfg = config['resource']
conn = pymysql.connect(**config._sections['resource'])
conn2c = pymysql.connect(**config._sections['target'])

#测试的话上面的要加test
tablename_sure = config_name['tablename_sure']  # 完全知道名字的表单
tablename_un_sure = config_name['tablename_un_sure']  # 名字后面仍然有后缀的表单
flag = base_config['write1transfer2']
nonce = int(base_config['nonce'])

sure_list = tablename_sure.split(',')
un_sure_list = tablename_un_sure.split(',')

# todo:如果后续的活动

cur = conn.cursor()
# print(truncate_all(conn2c))
try:
    truncate_all(conn2c)  # 检查一下这个应该是target!库
    serch_data = """show tables;"""
    result = cur.execute(serch_data)
    # print(result)  # 默认不返回查询结果集,返回数据记录数
    namesindb = cur.fetchall()  # 获取所有的查询结果,当前记录数和剩下所有记录数
    # print(cur.rowcount)  # 返回执行sql语句影响的行数
    serch_data = "select count(*) from %s;"
    length_dic = {}
    if do_flag == 1:  # 是1代表只读取想要的
        un22sure = []
        for i in namesindb:
            for unsure in un_sure_list:
                if i[0].find(unsure) != -1:
                    un22sure.append(i[0])  # 得到所有的有关名字的表单

        for name in sure_list:  # 完全确定的
            cur.execute(serch_data % name)
            result = cur.fetchone()
            length_dic[name] = int(result[0])
        for name in un22sure:
            cur.execute(serch_data % name)
            result = cur.fetchone()
            length_dic[name] = int(result[0])
        # print(0)#到此为止获取到所有的表单的长度，0为确认，1为尚未确认
    else:
        for name in namesindb:
            cur.execute(serch_data % name[0])
            result = cur.fetchone()
            length_dic[name[0]] = int(result[0])
    serch_get = '''select * from {} limit {} offset {}'''
    ziduan_cnt = '''SELECT COUNT(*) FROM information_schema. COLUMNS WHERE table_name = "{0}" and table_schema="{1}";'''
    log_str = '{}//现在开始插入{:25}，共{}行(共需要{}次)'
    for name, length in length_dic.items():
        n = length // nonce
        now = datetime.datetime.now()
        print(log_str.format(now.strftime("%Y-%m-%d %H:%M:%S"), str(name), length, str(n + 1)))
        cur.execute(ziduan_cnt.format(name, resr_cfg['db']))
        ziduan_length = cur.fetchone()[0]
        # print(name+str(ziduan_length)+'总共需要'+str(n+1)+'次')
        for i in range(n + 1):
            if n + 1 == 1:
                pass
            else:
                print(i + 1)
            cur.execute(serch_get.format(name, nonce, i * nonce))
            answer = cur.fetchall()
            ins2db(conn2c, answer, name, ziduan_length)
    conn.close()
    conn2c.close()
except Exception as e:
    print(traceback.format_exc())
else:
    print("******************本次传输完成*********************!")
