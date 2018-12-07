package comp4651.project.spark

import scala.collection.mutable.ArrayBuffer

class PathPattern(pattern: String) extends java.io.Serializable {

  def computePathSegmentsClosure(): String => Array[String] = {
    path: String => {
      var pathSeg = path.split("/+")

      // remove leading and trailing empty segments
      if (pathSeg(0).length == 0) {
        pathSeg = pathSeg.drop(1)
      }
      val len = pathSeg.length
      if (pathSeg(len - 1).length == 0) {
        pathSeg = pathSeg.dropRight(1)
      }
      pathSeg
    }
  }

  val computePathSegments: String => Array[String] = computePathSegmentsClosure()

  val pathSegments: Array[String] = computePathSegments(pattern)

  val rootDirPath: String = "/" + computeRootDirSegments(pathSegments).mkString("/")

  val numOfRootDirSegments: Int = computeRootDirSegments(pathSegments).length

  val pathSegmentsBelowRoot: Array[String] = computePathSegmentsBelowRoot(pathSegments)

  val keyVariables: Array[String] = pathSegments.filter(isKey)

  val keyVariableLevels: Array[Int] = computeKeyVariableLevels(pathSegments)

  // assume that the path passed in this function and the constructor have
  // the same number of levels
  def getExtractKeyFunc(computePathSegments: String => Array[String] = computePathSegmentsClosure(),
                        keyVariableLevels: Array[Int] = keyVariableLevels.clone()): String => String = {
    // extract key function here
    path: String => {
      val pS = computePathSegments(path)
      val keyVars = ArrayBuffer.empty[String]
      for (l <- keyVariableLevels) {
        keyVars += pS(levelToIndex(l))
      }

      // use for group by operation
      keyVars.mkString("/")
    }
  }

  def levelToIndex(l: Int): Int = {
    if (l < 2) {
      throw new Exception("Pls enter level greater than or equal to 2.")
    }
    l-2+numOfRootDirSegments
  }

  def computeKeyVariableLevels(pS: Array[String]): Array[Int] = {
    val pSBelowRoot = computePathSegmentsBelowRoot(pathSegments)
    val levels = ArrayBuffer.empty[Int]
    var level = 2
    for (seg <- pSBelowRoot) {
      if (isKey(seg)) {
        levels += level
      }
      level += 1
    }
    levels.toArray
  }

  def computePathSegmentsBelowRoot(pS: Array[String]): Array[String] = {
    val numOfRootSegments = computeRootDirSegments(pS).length
    pS.slice(numOfRootSegments, pS.length)
  }

  def computeRootDirSegments(pS: Array[String]): Array[String] = {
    var firstKeyIndex = 0
    for (seg <- pS) {
      if (isNotConstant(seg)) {
        // excluding the key variable
        return pS.slice(0, firstKeyIndex)
      }
      firstKeyIndex += 1
    }

    // the case that the whole input pattern is the root directory path
    pS.slice(0, firstKeyIndex)
  }

  private[this] def isKey(seg: String): Boolean = {
    seg.matches("^<.*>$")
  }

  private[this] def isNotConstant(seg: String): Boolean = {
    seg.matches("(^<.*>$|^\\*$)")
  }

  def matchedKeyVariables(otherPP: PathPattern): Boolean = {
    val keyVariable1 = keyVariables.sortWith(_ < _)
    val keyVariable2 = otherPP.keyVariables.sortWith(_ < _)
    return keyVariable1.deep == keyVariable2.deep
  }

  def hasNextLevel(currentLevel: Int): Boolean = {
    // currentLeve = 1 means it is currently at the root directory
    currentLevel <= pathSegmentsBelowRoot.length
  }

  def getLevel(currentLevel: Int): String = {
    if (currentLevel < 2) {
      throw new Exception("Pls enter level greater than or equal to 2.")
    }
    pathSegmentsBelowRoot(currentLevel-2)
  }

  def isCurrentLevelConstant(currentLevel: Int): Boolean = {
    // level 1 is root node
    // level 2 is the first level below root node

    !isNotConstant(getLevel(currentLevel))
  }
}