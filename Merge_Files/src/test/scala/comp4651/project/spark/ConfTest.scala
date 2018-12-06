package comp4651.project.spark

import org.scalatest.FunSuite

class ConfTest extends FunSuite {
  test("verify Conf correctness") {
    val arg = "--input /some/root/<key1>/*/ --output /another/root/<key1>/"
    val args = arg.split("\\s+")
    println(args.mkString("\n"))
    val conf = new Conf(args)

    assert(conf.input() == "/some/root/<key1>/*/")
    assert(conf.output() == "/another/root/<key1>/")
  }
}