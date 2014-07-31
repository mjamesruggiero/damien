package com.mjamesruggiero.damien

import org.scalatest.FunSpec
import org.scalatest.BeforeAndAfter
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

class CountParamvalsSpec extends FunSpec with BeforeAndAfter{

  val sc = new SparkContext("local", "CountParamvals",
             System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))

  describe("CountParamvals") {
    it("returns None, count when the params are not set"){
      assert(true == true)
    }
  }
}
