package com.mjamesruggiero.damien

import org.scalatest.FunSuite
import org.scalatest.ShouldMatchers
import org.scalatest.BeforeAndAfter
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object SparkTest extends org.scalatest.Tag("com.qf.test.tags.SparkTest")

trait SparkTestUtils extends FunSuite {
  var sc: SparkContext = _

  /**
   * convenience method for tests that use spark.  Creates a local spark context, and cleans
   * it up even if your test fails.  Also marks the test with the tag SparkTest, so you can
   * turn it off
   *
   * By default, it turn off spark logging, b/c it just clutters up the test output.  However,
   * when you are actively debugging one test, you may want to turn the logs on
   *
   * @param name the name of the test
   * @param silenceSpark true to turn off spark logging
   */
  def sparkTest(name: String, silenceSpark : Boolean = true)(body: => Unit) {
    test(name, SparkTest){
      //val origLogLevels = if (silenceSpark) SparkUtil.silenceSpark() else null
      sc = new SparkContext("local[4]", name)
      try {
        body
      }
      finally {
        sc.stop
        sc = null
        // To avoid rebinding to the same port; doesn't unbind immediately on shutdown
        System.clearProperty("spark.master.port")
        //if (silenceSpark) Logging.setLogLevels(origLogLevels)
      }
    }
  }
}


class CountParamValsSpec extends SparkTestUtils with ShouldMatchers {
  sparkTest("spark filter") {
    val currentDirectory = new java.io.File( "." ).getCanonicalPath
    val filepath = s"${currentDirectory}/src/test/scala/com/mjamesruggiero/damien/data/test_log.txt"
    val pattern = """.*oid=([^\&]+).*""".r
    val reducers = 10

    val data = new CountParamVals(sc, filepath, "impression", pattern, reducers).exec()
    val output = data.flatMap (e => Map(e._1 -> e._2))

    // there are three unique OIDs for all impressions
    output.count should be (3)
  }
}
