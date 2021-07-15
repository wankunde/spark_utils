# Read and analyze spark job logs

* Read local log file and debug program: `org.apache.spark.loganalyze.LocalLogReader`
* Read all log events: `bin/spark_utils.sh catlog [file_path]`
* Search custom plan pattern:
  * Write a object which extends `AnalyzeBase`.
  * Run custom analyze job: `bin/spark_utils.sh custom_class [custom_class]`
  * Collect parse result: `yarn logs -applicationId $yarn_job_id -log_files_pattern stdout | grep -vE 'End of LogType:stdout|^$' | grep -v "\*\*\*"`
  * Uniq result by sql:`org.apache.spark.loganalyze.UniqQuery`

Example shell script:
```shell
tar -zxvf spark_utils-1.0-bin.tar.gz
export SPARK_HOME=/mnt/b_carmel/wakun/spark_releases/CARMEL-4978/spark
spark_utils-1.0/bin/spark_utils.sh custom_class org.apache.spark.loganalyze.SearchJoinPattern
spark_utils-1.0/bin/spark_utils.sh collect_result application_1626012606118_2172
```
