# Read and analyze spark job logs

* Read local log file and debug program: `org.apache.spark.loganalyze.LocalLogReader`
* Read all log events: `bin/spark_utils.sh catlog [file_path]`
* Search custom plan pattern:
  * Write `object custom_class extends AnalyzeBase`
  * Analyze logs `bin/spark_utils.sh custom_class [custom_class]`
  * Read Logs: `yarn logs -applicationId $yarn_job_id -log_files_pattern stdout | grep -vE 'End of LogType:stdout|^$' | grep -v "\*\*\*"`