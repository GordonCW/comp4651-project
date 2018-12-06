package comp4651.project.spark

class PathPattern(path: String) {

  val pathSegments: Array[String] = {
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

  val rootDirPath: String = computeRootDirPath()

  val pathSegmentsBelowRoot: Array[String] = computePathSegmentsBelowRoot()

  val keyVariables: Array[String] = {
    pathSegments.filter(isKey)
  }

  def computePathSegmentsBelowRoot(): Array[String] = {
    var firstKeyIndex = 0
    for (seg <- pathSegments) {
      if (isNotConstant(seg)) {
        // excluding the key variable
        return pathSegments.slice(firstKeyIndex, pathSegments.length)
      }
      firstKeyIndex += 1
    }

    // throw exception here
    Array("")
  }

  def computeRootDirPath(): String = {
    var firstKeyIndex = 0
    for (seg <- pathSegments) {
      if (isNotConstant(seg)) {
        // excluding the key variable
        return "/" + pathSegments.slice(0, firstKeyIndex).mkString("/")
      }
      firstKeyIndex += 1
    }

    // throw exception here
    ""
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