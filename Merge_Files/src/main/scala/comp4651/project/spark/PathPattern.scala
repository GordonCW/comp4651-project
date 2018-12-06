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

  val keyVariables: Array[String] = {
    pathSegments.filter(isKey)
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
}