package com.mjamesruggiero.damien

import org.scalatest.FunSpec
import org.scalatest.BeforeAndAfter

class UtilsSpec extends FunSpec {
  describe("regexer") {
    it("returns a match when there's a match"){
      val pat = """.*baz=([^\&]+).*""".r
      val testString = "foo=bar&baz=quux"
      val result = Utils.regexer(testString, pat)
      assert(result == Some("quux"))
    }

    it("returns None when there's a match"){
      val pat = """.*baz=([^\&]+).*""".r
      val testString = "foo=bar&zed=quux"
      val result = Utils.regexer(testString, pat)
      assert(result == None)
    }
  }
}
