# DumpTables
Dump Tables From Oracle To Hive by using Datax


# This tools is designed for dumping data from Oracle to Hive.
## usage:

1. save all table names that you want load to hive from Oracle into conf/tables.conf.

2. then, excute this shell script.

# notice:
when you excute this script, you must make sure that DATAX_HOME, PYTHON_HOME, HADOOP_HOME and HIVE_HOME already been set. 

# exit code list:

2 : DATAX_HOME not found, Please set environment argument DATAX_HOME

3 : PYTHON_HOME not found, Please set environment argument PYTHON_HOME

4 : HADOOP_HOME not found, Please set environment argument HADOOP_HOME

5 : HIVE_HOME not found, Please set environment argument HIVE_HOME

6 : tables.conf not found, Please configure ./conf/tables.conf
