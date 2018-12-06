package comp4651.project.spark

import org.scalatest.FunSuite

class OutputPathPatternTest extends FunSuite {
  test("test generateOutputPath") {
    val inputPath = "//some/root///<key>/<>/<>/<key1>/*/"
    val outputPath = "/another/root/<>/<key1>/<>/<key>"
    val input = new PathPattern(inputPath)
    val output = new OutputPathPattern(outputPath, input)
    assert(output.generateOutputPath("KEY/empty1/empty2/KEY1") == "/another/root/empty1/KEY1/empty2/KEY/result.txt")
  }

  test("test generateOutputPath2") {
    val inputPath = "//some/root////*/"
    val outputPath = "/another/root/"
    val input = new PathPattern(inputPath)
    val output = new OutputPathPattern(outputPath, input)
    assert(output.generateOutputPath("") == "/another/root/result.txt")
  }

  test("test generateOutputPath3") {
    val inputPath = "//some/root///<key1>/<key2>/<key3>/*/"
    val outputPath = "/another/root/<key3>/<key1>/<key2>"
    val input = new PathPattern(inputPath)
    val output = new OutputPathPattern(outputPath, input)
    assert(output.generateOutputPath("1/2/3") == "/another/root/3/1/2/result.txt")
  }
}