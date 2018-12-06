package comp4651.project.spark

import org.rogach.scallop._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val input = opt[String](required = true)
  val output = opt[String]()
  verify()
}