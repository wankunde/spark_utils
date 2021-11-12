import java.io.File

import scala.collection.mutable
import scala.io.Source

import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.FormattedMode

val referenceRegex = "#\\d+".r
val normalizeRegex = "#\\d+L?".r

Logger.getLogger("org.apache.spark.sql.hive.client").setLevel(Level.ERROR)
Logger.getLogger("org.apache.spark.sql.execution.window").setLevel(Level.ERROR)
Logger.getLogger("org.apache.spark.scheduler.cluster").setLevel(Level.ERROR)

def normalizeIds(plan: String): String = {
  val map = new mutable.HashMap[String, String]()
  normalizeRegex.findAllMatchIn(plan).map(_.toString)
    .foreach(map.getOrElseUpdate(_, (map.size + 1).toString))
  normalizeRegex.replaceAllIn(plan, regexMatch => s"#${map(regexMatch.toString)}")
}

def traceQueryWithoutRunning(qName: String, statement: String, joinReorderEnable: Boolean)
              (spark: SparkSession): String = {
  spark.conf.set("spark.sql.cbo.joinReorder.enabled", joinReorderEnable)
  val qe = spark.sql(statement).queryExecution
  normalizeIds(qe.explainString(FormattedMode))
}

def traceQueryWithRunning(qName: String, statement: String, joinReorderEnable: Boolean)
                            (spark: SparkSession): (Long, String) = {
  val res = Range(0, 3).map{i =>
    val startTime = System.currentTimeMillis()
    spark.conf.set("spark.sql.cbo.joinReorder.enabled", joinReorderEnable)
    spark.sparkContext.setJobDescription(s"$qName-$i-${if(joinReorderEnable) "reorder" else "noreorder" }")
    val df = spark.sql(statement)
    df.take(10)
    val qe = df.queryExecution
    val plan = normalizeIds(qe.explainString(FormattedMode))
    val elapsedTime = System.currentTimeMillis() - startTime
    //  print(s"Query $qName elapsed $elapsedTime with join reorder = $joinReorderEnable")
    //  println(s"FormattedPlan: $plan")
    (elapsedTime, plan)
  }.sorted.sortBy(tup => tup._1).toList
  res(1)
}

val res = mutable.Map[String, (Long, Long, Boolean)]()
def printPlan(qName: String, statement: String)(spark: SparkSession): Unit = {
  val plan1 = traceQueryWithoutRunning(qName, statement, false)(spark)
  val plan2 = traceQueryWithoutRunning(qName, statement, true)(spark)
  val (ts1, _) = traceQueryWithRunning(qName, statement, false)(spark)
  val (ts2, _) = traceQueryWithRunning(qName, statement, true)(spark)
  res += qName -> (ts1, ts2, plan1 != plan2)
}

val spark = SparkSession
  .builder
  .appName("CostBasedReorderTest")
  .getOrCreate()


// ============= Workaround for Carmel Spark Release ===========================
val stateCls = spark.sessionState.getClass
val authorizerField = stateCls.getDeclaredField("authorizer")
authorizerField.setAccessible(true)
val authorizer = authorizerField.get(spark.sessionState)
val authorizerSetCurrentUserMethod =
  authorizer.getClass.getDeclaredMethod("setCurrentUser", classOf[String])
authorizerSetCurrentUserMethod.invoke(authorizer, spark.sparkContext.sparkUser)
val catalogSetCurrentUserMethod =
  spark.sessionState.catalog.getClass.getMethod("setCurrentUser", classOf[String])
catalogSetCurrentUserMethod.invoke(spark.sessionState.catalog, spark.sparkContext.sparkUser)
spark.sql("set role admin")
// ============= Workaround for Carmel Spark Release ===========================

spark.sql("use hermes_tpcds5t")
val queryPaths ="/home/b_carmel/wakun/spark_utils/tpcds"

val queries = new File(queryPaths).listFiles()

def checkChanged(qName: String, statement: String)(spark: SparkSession): Boolean = {
  val plan1 = traceQueryWithoutRunning(qName, statement, false)(spark)
  val plan2 = traceQueryWithoutRunning(qName, statement, true)(spark)
  plan1 != plan2
}
queries.foreach { query =>
  val qName = query.getName.stripSuffix(".sql")
  val statement = Source.fromFile(query).mkString
  val changed = checkChanged(qName, statement)(spark)
  if(changed) {
    println(s"Query $qName Plan changed")
  } else {
    println(s"Query $qName Plan not changed")
  }
}

val qName = "q39b"
val statement = Source.fromFile(s"$queryPaths/$qName.sql").mkString
printPlan(qName, statement)(spark)
