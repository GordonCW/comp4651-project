package comp4651.project.spark

import org.scalatest.FunSuite

class PathPatternTest extends FunSuite {
  test("test pathSegments") {
    val path = "//some/root///<key>/*/"
    val result = new PathPattern(path).pathSegments
    assert(result.deep == Array("some", "root", "<key>", "*").deep)
  }

  test("test pathSegments2") {
    val path = "//some/root///<key>/*"
    val result = new PathPattern(path).pathSegments
    assert(result.deep == Array("some", "root", "<key>", "*").deep)
  }

  test("test rootDirPath") {
    val path = "//some/root///<key>/*"
    val result = new PathPattern(path).rootDirPath
    assert(result == "/some/root")
  }

  test("test rootDirPath2") {
    val path = "///<key>/*"
    val result = new PathPattern(path).rootDirPath
    assert(result == "/")
  }

  test("test rootDirPath3") {
    val path = "////*/<key>/123"
    val result = new PathPattern(path).rootDirPath
    assert(result == "/")
  }

  test("test rootDirPath4") {
    val path = "//some/root/*/<key>/123"
    val result = new PathPattern(path).rootDirPath
    assert(result == "/some/root")
  }

  test("test matchedKeyVariables") {
    val path1 = "//some/root/*/<key>/<>/<>/123"
    val pp1 = new PathPattern(path1)
    val path2 = "//another//root/<>/<key>/<>/456"
    val pp2 = new PathPattern(path2)
    assert(pp1.matchedKeyVariables(pp2))
  }

  test("test matchedKeyVariables2") {
    val path1 = "//some/root/*//123"
    val pp1 = new PathPattern(path1)
    val path2 = "//another//root//456"
    val pp2 = new PathPattern(path2)
    assert(pp1.matchedKeyVariables(pp2))
  }

  test("test isCurrentLevelConstant") {
    val path = "//some/root/*//123"
    val pp = new PathPattern(path)
    assert(pp.isCurrentLevelConstant(3))
  }

  test("test isCurrentLevelConstant2") {
    val path = "//some/root/*//123"
    val pp = new PathPattern(path)
    assert(!pp.isCurrentLevelConstant(2))
  }

  test("test getLevel") {
    val path = "//some/root/*//123"
    val pp = new PathPattern(path)
    assert(pp.getLevel(2) == "*")
  }

  test("test extractKey") {
    val path = "//some/root/<key1>/456/<>/<key3>/123"
    val pp = new PathPattern(path)
    assert(pp.extractKey("//some/root/key11/456/key22/key33/123") == "key11/key22/key33")
  }
}