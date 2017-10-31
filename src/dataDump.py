# -*- coding:utf-8 -*- 
# 
import sys
import os
import collections
import subprocess
import argparse

d = collections.OrderedDict()

def parse_args():
    parser = argparse.ArgumentParser(description='Dump Data from Oracle to Hive')
    parser.add_argument(
        '--basePath',
        type=str,
        help='Path of this tools')
    parser.add_argument(
        '--hdfsPath',
        type=str,
        help='Path of hdfs')
    parser.add_argument(
        '--localConfPath',
        type=str,
        help='Path of local configuration')
    parser.add_argument(
        '--localTempPath',
        type=str,
        help='Path of local temp')
    parser.add_argument(
        '--DATAX_HOME',
        type=str,
        help='Path of datax')
    parser.add_argument(
        '--PYTHON_HOME',
        type=str,
        help='Path of python')
    parser.add_argument(
        '--HADOOP_HOME',
        type=str,
        help='Path of hadoop')
    parser.add_argument(
        '--HIVE_HOME',
        type=str,
        help='Path of hive')


    return parser.parse_args()
    pass

def createDumpTablesJson(args):
    # transform conf/tables.conf to tables.json
    tablesConfFile = open(args.localConfPath + '/tables.conf','r')
    jsonfile = open(args.localConfPath + '/model.json','r')
    tablesJsonFile = open(args.localTempPath + '/dump_tables.json','w')

    tablenames = ""
    for line in tablesConfFile:
        line = line.strip()
        if len(line):
            tablenames += "'"+line+"'"+","

    tablenames = tablenames.strip().strip(",")

    sql = 'select c.TABLE_NAME,c.COLUMN_NAME, c.DATA_TYPE from user_tab_columns c where c.TABLE_NAME in ( '+tablenames+' ) order by c.table_name,c.column_id'
    col = '''{
                    "name": "columnname01",
                    "type": "string"
                },
                {
                    "name": "columnname02",
                    "type": "string"
                },
                {
                    "name": "columnname03",
                    "type": "string"
                }'''
    tname = 'dump_tables'

    sql_tag = '$(sql)'
    col_tag = '$(col)'
    tname_tag = '$(tname)'
    hdfsPath_tag = '$(hdfsPath)'

    for line in jsonfile:
        if sql_tag in line:

            line = line.replace(sql_tag, sql)

        if col_tag in line:

            line = line.replace(col_tag, col)

        if tname_tag in line:
            line = line.replace(tname_tag, tname)

        if hdfsPath_tag in line:
            line = line.replace(hdfsPath_tag, args.hdfsPath)

        tablesJsonFile.write(line.rstrip()+'\n')

    tablesConfFile.close()
    jsonfile.close()
    tablesJsonFile.close()
    pass

def getColumnsOfTables(args):
    cmd = args.PYTHON_HOME+"/python "+args.DATAX_HOME+"/bin/datax.py "+ args.localTempPath +"/dump_tables.json"
    printInfo(cmd)
    subp = subprocess.Popen(cmd,shell=True)
    subp.wait()
    cmd = args.HADOOP_HOME+"/bin/hadoop fs -get " + args.hdfsPath + '/dump_tables.txt__* ' + args.localTempPath + '/dump_tables.txt'
    printInfo(cmd)
    subp2 = subprocess.Popen(cmd,shell=True)
    subp2.wait()
    pass


def createAllJsonFiles(tname, args):
    jsonfile = open(args.localConfPath + '/model.json','r')
    jsonfile_new = open(args.localTempPath + '/'+tname+'.json','w')

    whitelist = ['CUST_UID','OLDSID']

    sql_tag = '$(sql)'
    col_tag = '$(col)'
    tname_tag = '$(tname)'
    hdfsPath_tag = '$(hdfsPath)'
    str = ''
    for c in d[tname].keys():
        if 'NUMBER' in d[tname][c] and c not in whitelist:
            str += ',to_char(' + c + ',\'fm9999999990.0999999\')'
        elif 'DATE' in d[tname][c]:
            str += ',to_char(' + c + ',\'yyyy/mm/dd\')'
        else:
            str += ',' + c
    sql = 'select ' + str.strip().strip(',') + ' from ' + tname


    col = ''
    for c in d[tname].keys():
        col += '''{
                            "name": "'''+ c + '''",
                            "type":"string"
                       },'''
    col = col[:-1]

    for line in jsonfile:
        if sql_tag in line:

            line = line.replace(sql_tag, sql)

        if col_tag in line:

            line = line.replace(col_tag, col)

        if tname_tag in line:
            line = line.replace(tname_tag, tname)

        if hdfsPath_tag in line:
            line = line.replace(hdfsPath_tag, args.hdfsPath)

        jsonfile_new.write(line.rstrip()+'\n')
        #printInfo(line)
    jsonfile.close()
    jsonfile_new.close()
    pass

def generatesql(args):
    tohivefile = open(args.localTempPath+'/tohive.sql','w')

    for tname in d.keys():
        createsql = 'drop table if exists ' + tname + ';\n'
        createsql += 'create table ' + tname + ' ( ' + ' STRING,'.join(d[tname].keys()) + ' STRING ) \n' 
        createsql += '''ROW FORMAT DELIMITED FIELDS TERMINATED BY '&'
STORED AS TEXTFILE
LOCATION '/hive/'''
        createsql += tname + '\';\n'

        impdatasql = 'load data inpath \''+ args.hdfsPath + '/' + tname + '.txt\' into table ' + tname + ';'

        tohivefile.write(createsql + '\n' + impdatasql + '\n')

        #printInfo (createsql + '\n' + impdatasql + '\n')
    pass

def oracleToHdfs(args):

    for tname in d.keys():
        cmd = args.PYTHON_HOME+"/python "+args.DATAX_HOME+"/bin/datax.py "+ args.localTempPath +"/"+tname+".json"
        printInfo(cmd)
        subp = subprocess.Popen(cmd,shell=True)
        subp.wait()
        cmd = args.HADOOP_HOME+"/bin/hadoop fs -mv " + args.hdfsPath + "/" + tname + '.txt__* ' + args.hdfsPath + '/' + tname + '.txt'
        printInfo(cmd)
        subp2 = subprocess.Popen(cmd,shell=True)
        subp2.wait()
        #printInfo(subp)
        
    pass

def printInfo(info):
    print('[Dump Tables] INFO : ' + info)
    pass

if __name__ == "__main__":

    args=parse_args()
    createDumpTablesJson(args)
    getColumnsOfTables(args)

    try:
        f = open(args.localTempPath+'/dump_tables.txt', 'r')

        for line in f:
            tmp = {}
            data = line.strip('\n').split('&')
            if data[0] not in d.keys():
                d[data[0]] = collections.OrderedDict()
            tmp = d[data[0]]
            tmp[data[1]] = data[2]
            d[data[0]] = tmp

    finally:
        if f:
            f.close()

    for tname in d.keys():
        printInfo ('generate json file : ' + tname + '.json')
        createAllJsonFiles(tname, args)

    printInfo('generated ' + str(len(d.keys())) + ' files!')
    
    oracleToHdfs(args)
    generatesql(args)

    cmd = args.HIVE_HOME+"/bin/hive -f "+args.localTempPath+"/tohive.sql"
    printInfo(cmd)
    subp = subprocess.Popen(cmd,shell=True)
    subp.wait()
