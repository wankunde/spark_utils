#!/usr/bin/env bash

BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
. "${BASEDIR}/bin/env.sh"

function print_usage(){
  echo "Usage: spark_utils.sh COMMAND"
  echo "     where COMMAND is one of:"
  echo "catlog               print the spark log file content in json format."
  echo "help                 print tool usage."
}

function check_environment() {
  # check for hadoop in the path
  HADOOP_IN_PATH=`which hadoop 2>/dev/null`
  if [ -f "${HADOOP_IN_PATH}" -a "${HADOOP_IN_PATH}" != "" ]; then
    HADOOP_DIR=`dirname "$HADOOP_IN_PATH"`/..
  fi
  # HADOOP_HOME env variable overrides hadoop in the path
  HADOOP_HOME=${HADOOP_HOME:-$HADOOP_DIR}
  if [ "$HADOOP_HOME" == "" ]; then
    echo "ERROR: Cannot find hadoop installation: \$HADOOP_HOME must be set or hadoop must be in the path";
    exit 4;
  else
    export HADOOP_HOME
  fi

  # check for spark in the path
  SPARK_IN_PATH=`which spark-submit 2>/dev/null`
  if [ -f "${SPARK_IN_PATH}" -a "${SPARK_IN_PATH}" != "" ]; then
    SPARK_DIR=`dirname "$SPARK_IN_PATH"`/..
  fi
  # SPARK_HOME env variable overrides hadoop in the path
  SPARK_HOME=${SPARK_HOME:-$SPARK_DIR}
  if [ "$SPARK_HOME" == "" ]; then
    echo "ERROR: Cannot find spark installation: \$SPARK_HOME must be set or spark-submit must be in the path";
    exit 4;
  else
    export SPARK_HOME
  fi
}

if [ $# == 0 ]; then
  print_usage
  exit
fi

check_environment

CMD=$1
shift
if [ "${CMD}" == "catlog" ];then
  if [ $# == 0 ]; then
    echo "Usage: catlog need a log file path!"
    exit
  fi

  file_name=$1
  echo "Run command: spark_utils.sh catlog ${file_name}"
  exec ${SPARK_HOME}/bin/spark-submit \
    --master local \
    --files ${BASEDIR}/conf/sink.jdbc.properties \
    --class org.apache.spark.loganalyze.CatLog \
    ${BASEDIR}/libs/spark_utils-1.0.jar ${file_name}
elif [ "${CMD}" == "custom_class" ]; then
  custom_class=$1
  echo "Run command: spark_utils.sh custom_class ${custom_class}"
  exec ${SPARK_HOME}/bin/spark-submit --master yarn --driver-memory 8G \
     --executor-memory 7G \
     --class ${custom_class} \
     --conf spark.driver.memoryOverhead=0 \
     --conf spark.driver.extraJavaOptions="" \
     --conf spark.memory.offHeap.enabled=false \
     --conf spark.memory.offHeap.size=0 \
     --conf spark.executor.cores=5 \
     --conf spark.executor.instances=180 \
     --conf spark.executor.memory=7g \
     --conf spark.executor.extraJavaOptions="" \
     --conf spark.executor.memoryOverhead=0 \
     --conf spark.yarn.queue=hdmi-staging \
    ${BASEDIR}/libs/spark_utils-1.0.jar
elif [ "${CMD}" == "tpcds" ]; then
  echo "Run command: spark_utils.sh tpcds"
  exec ${SPARK_HOME}/bin/spark-submit --master yarn --driver-memory 8G \
     --executor-memory 7G \
     --class org.apache.spark.tpcds.CostBasedReorderTest \
     --conf spark.driver.memoryOverhead=0 \
     --conf spark.driver.extraJavaOptions="" \
     --conf spark.memory.offHeap.enabled=false \
     --conf spark.memory.offHeap.size=0 \
     --conf spark.executor.cores=5 \
     --conf spark.executor.instances=100 \
     --conf spark.executor.memory=7g \
     --conf spark.executor.extraJavaOptions="" \
     --conf spark.executor.memoryOverhead=0 \
     --conf spark.yarn.queue=hdmi-staging \
    ${BASEDIR}/libs/spark_utils-1.0.jar
elif [ "${CMD}" == "collect_result" ]; then
  applicationId=$1
  echo "Run command: spark_utils.sh applicationId ${applicationId}"
  yarn logs -applicationId $applicationId -log_files_pattern stdout | grep -vE 'End of LogType:stdout|^$' | grep -v "\*\*\*" > /tmp/AnalyzeBase.log
  export CLASSPATH=$(echo ${SPARK_HOME}/jars/*.jar | tr ' ' ':'):${BASEDIR}/libs/spark_utils-1.0.jar:${CLASSPATH}
  exec java org.apache.spark.loganalyze.UniqQuery > analyze.log
else
  print_usage
fi
