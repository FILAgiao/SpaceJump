# -*- coding:utf-8 -*-
# project:Spacejump!居间人设备（手动狗头）
# user:GIAO
# Author: FILAgiao
# createtime: 2021/12/23 14:25
#TODO:为了避免传参出问题，其实可以把与连接有关的属性和操作封装成为一个类
import configparser
import traceback
import pymysql

def ins2db(conn,datalist,tablename,ziduan_len):
    cursor=conn.cursor()
    sql = ins2big_sql(tablename,ziduan_len)
    # 批量插入
    try:
        cursor.executemany(sql, datalist)
        conn.commit()
    except:
        print(traceback.format_exc())

def ins2big_sql(tablename,ziduan_length):
    sql = "INSERT INTO {} VALUES".format(tablename)
    jjrsb=[]
    for i in range(ziduan_length[0]):
        jjrsb.append('%s')
    sql=sql+"("+','.join(jjrsb)+")"
    return sql


config = configparser.ConfigParser()
config.read('table_name.ini', encoding='utf-8')
# 基础config
config_name = config['table_name']
config_login=config['Database2connect_ini']
# config_login = config['local_test']  # fixme:only in test
base_config = config['base_config']
c2c_config=config['c2c']#将数据插入此处
# 具体读取的表单
tablename_sure = config_name['tablename_sure']  # 完全知道名字的表单
tablename_un_sure = config_name['tablename_un_sure']  # 名字后面仍然有后缀的表单
host = config_login['ip']
user = config_login['user']
password = config_login['password']
db = config_login['db']
flag = base_config['write1transfer2']
nonce = int(base_config['nonce'])
c2c_host=c2c_config['ip']
c2c_user = c2c_config['user']
c2c_password = c2c_config['password']
c2c_db = c2c_config['db']


sure_list = tablename_sure.split(',')
un_sure_list = tablename_un_sure.split(',')
conn = pymysql.connect(
    host=host,  # 连接主机名,也可以用ip地址，例如127.0.0.1
    user=user,  # 连接用户名
    passwd=password,  # 用户密码
    db=db,  # 要连接的数据库名
    charset="utf8mb4",  # 指定编码格式
    autocommit=True,  # 如果插入数据，， 是否自动提交? 和conn.commit()功能一致。
)
conn2c = pymysql.connect(
    host=c2c_host,  # 连接主机名,也可以用ip地址，例如127.0.0.1
    user=c2c_user,  # 连接用户名
    passwd=c2c_password,  # 用户密码
    db=c2c_db,  # 要连接的数据库名
    charset="utf8mb4",  # 指定编码格式
    autocommit=True,  # 如果插入数据，， 是否自动提交? 和conn.commit()功能一致。
)

cur = conn.cursor()
try:
    serch_data = """show tables;"""
    result = cur.execute(serch_data)
    print(result)  # 默认不返回查询结果集,返回数据记录数

    namesindb = cur.fetchall()  # 获取所有的查询结果,当前记录数和剩下所有记录数
    print(cur.rowcount)  # 返回执行sql语句影响的行数
    un22sure = []
    for i in namesindb:
        for unsure in un_sure_list:
            if i[0].find(unsure) != -1:
                un22sure.append(i[0])  # 得到所有的有关名字的表单
    serch_data = "select count(*) from %s;"
    length_dic = {}
    for name in sure_list:#完全确定的
        cur.execute(serch_data%name)
        result = cur.fetchone()
        length_dic[name]=int(result[0])
    for name in un22sure:
        cur.execute(serch_data%name)
        result = cur.fetchone()
        length_dic[name]=int(result[0])
    print(0)#到此为止获取到所有的表单的长度，0为确认，1为尚未确认
    serch_get='''select * from {} limit {} offset {}'''
    ziduan_cnt='''SELECT COUNT(*) FROM information_schema. COLUMNS WHERE table_name = "%s";'''
    for name,length in length_dic.items():
        print('现在开始插入'+name)
        n=length//nonce
        cur.execute(ziduan_cnt%name)
        ziduan_length=cur.fetchone()
        print('总共需要'+str(n+1)+'次')
        for i in range(n+1):
            print(i)
            cur.execute(serch_get.format(name,nonce,i*nonce))
            answer=cur.fetchall()
            ins2db(conn2c,answer,name,ziduan_length)

except Exception as e:
    print(traceback.format_exc())
else:
    print("数据查询成果，jyqsb!")


