#! /bin/bash

# This script is designed for dumping data from Oracle to Hive.
# usage:
# 1. save all table names that you want load to hive from Oracle into conf/tables.conf.
# 2. then, excute this shell script.

# notice:
# when you excute this script, you must make sure that DATAX_HOME, PYTHON_HOME, HADOOP_HOME and HIVE_HOME already been set. 

# exit code list:
# 2 : DATAX_HOME not found, Please set environment argument DATAX_HOME
# 3 : PYTHON_HOME not found, Please set environment argument PYTHON_HOME
# 4 : HADOOP_HOME not found, Please set environment argument HADOOP_HOME
# 5 : HIVE_HOME not found, Please set environment argument HIVE_HOME
# 6 : tables.conf not found, Please configure ./conf/tables.conf

# arguments for this tool
basePath=$(cd `dirname $0`; pwd)
hdfsPath='/user/test/datax'
localConfPath=$basePath/conf
localTempPath=$basePath/temp

echo '[Dump Tables] INFO : basePath = '$basePath
echo '[Dump Tables] INFO : hdfsPath = '$hdfsPath
echo '[Dump Tables] INFO : localConfPath = '$localConfPath
echo '[Dump Tables] INFO : localTempPath = '$localTempPath

if [ -z $DATAX_HOME ];
then
	echo '[Dump Tables] ERROR : error code 2: DATAX_HOME not found, Please set environment argument DATAX_HOME'
	exit 2
else
	echo '[Dump Tables] INFO : DATAX_HOME = '$DATAX_HOME
fi

if [ -z $PYTHON_HOME ];
then
	echo '[Dump Tables] ERROR : error code 3: PYTHON_HOME not found, Please set environment argument PYTHON_HOME'
	exit 3
else
	echo '[Dump Tables] INFO : PYTHON_HOME = '$PYTHON_HOME
fi

if [ -z $HADOOP_HOME ];
then
        echo '[Dump Tables] ERROR : error code 4: HADOOP_HOME not found, Please set environment argument HADOOP_HOME'
        exit 4
else
        echo '[Dump Tables] INFO : HADOOP_HOME = '$HADOOP_HOME
fi

if [ -z $HIVE_HOME ];
then
        echo '[Dump Tables] ERROR : error code 5: HIVE_HOME not found, Please set environment argument HIVE_HOME'
        exit 5
else
        echo '[Dump Tables] INFO : HIVE_HOME = '$HIVE_HOME
fi

tablesConf=$localConfPath/tables.conf
if [ -f $tablesConf ];
then 
	echo '[Dump Tables] INFO : tables.conf = '$tablesConf
else
	echo '[Dump Tables] ERROR : error code 6: tables.conf not found, Please configure '$tablesConf
	exit 6
fi


# rm target dir, both hdfs and local
echo '[Dump Tables] INFO : hadoop fs -rm -r '$hdfsPath 
hadoop fs -rm -r $hdfsPath
hadoop fs -mkdir $hdfsPath
echo '[Dump Tables] INFO : rm -R '$localTempPath
rm -R $localTempPath 
mkdir $localTempPath

python $basePath/src/dataDump.py --basePath=$basePath \
				--localConfPath=$localConfPath \
				--localTempPath=$localTempPath \
				--hdfsPath=$hdfsPath \
				--DATAX_HOME=$DATAX_HOME \
				--PYTHON_HOME=$PYTHON_HOME \
				--HADOOP_HOME=$HADOOP_HOME \
				--HIVE_HOME=$HIVE_HOME

if [ $? -eq 0 ]
then
	echo 'Done'
else
	echo 'Error'
	exit 9
fi
