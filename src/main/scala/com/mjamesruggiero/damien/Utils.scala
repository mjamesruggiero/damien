package com.mjamesruggiero.damien

object Utils {
  def regexer(word: String, pattern: scala.util.matching.Regex): Option[String] = {
      val matched = pattern.findFirstMatchIn(word)
      matched match {
        case Some(m) => Some(m.group(1))
        case None => None
      }
  }
}
