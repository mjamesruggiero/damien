package com.mjamesruggiero.damien

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object CountParamVals {

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      System.err.println("Usage: CountParamVals <master> [<s3_key>] [<filter_string>] [<param_regex>] [<num_reducers>]")
      System.err.println("")
      System.err.println("  s3_key: The S3 key to read")
      System.err.println("  filter_string: the Filter for desired rows")
      System.err.println("  param_regex: regex (with one group) for the value")
      System.err.println("  num_reducers: Total number of reducers")
      System.exit(1)
    }

    val sc = new SparkContext(args(0), "CountParamVals",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))

    val pattern = """.*ploc=([^\&]+).*""".r
    val filepath = args(1)
    val reducers = 40
    val valueCounts = new CountParamVals(sc, "impressions", filepath, pattern, reducers).exec()
    valueCounts.map(println)

    sc.stop()
  }
}

class CountParamVals(sc: SparkContext,
                     filepath: String,
                     filterString: String,
                     pattern: scala.util.matching.Regex,
                     reducers: Int = 40) extends Serializable {

  def exec(): RDD[(Option[String], Int)] = {
    val filepath_ = filepath
    val filterString_ = filterString
    val pattern_ = pattern
    val reducers_ = reducers

    val rdda = sc.textFile(filepath_)
    val lines = rdda.filter(l => l.contains(filterString_))
    lines.map(l => Utils.regexer(l, pattern_)).map((_, 1)).reduceByKey((a: Int, b: Int) => a+b, reducers)
  }
}
