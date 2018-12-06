package comp4651.project.spark

class OutputPathPattern(pattern: String, inputPattern: PathPattern) extends PathPattern(pattern) {

  if (!matchedKeyVariables(inputPattern)) {
    throw new Exception("Input pattern and output pattern should have the same number of key variables.")
  }

  private val inputKeyToPathSegmentsIndexMap = acceptInputPathPattern(inputPattern)

  def acceptInputPathPattern(input: PathPattern): Map[Int, Int] = {
    val uniqueInputKeyVariables = generateUniqueKeyVars(input.keyVariables)

    val uniqueOuputKeyVariables = generateUniqueKeyVars(keyVariables)

    val inputKeyIndexToUniqueKey = uniqueInputKeyVariables.zipWithIndex
      .map{case (k, i) => (i, k)}

    val outputUniqueKeyToIndex = uniqueOuputKeyVariables.zipWithIndex.toMap

    inputKeyIndexToUniqueKey.map{case (i, k) =>
      (i, levelToIndex(outputUniqueKeyToIndex(k)+2))
    }.toMap
  }

  def generateUniqueKeyVars(keyVars: Array[String]): Array[String] = {

    // compute a permutation of key variables from input.
    val keyVarIndexMap = keyVars.toSet[String].map(key => (key, 0)).toMap
    val keyVarIndexMapMut = collection.mutable.Map(keyVarIndexMap.toSeq: _*)

    // this is needed for duplicate key and ensure the ordering
    val uniqueKeyVariables = keyVars.clone()
    var i = 0
    for (k <- uniqueKeyVariables) {
      val index = keyVarIndexMapMut(k)
      uniqueKeyVariables(i) = k + index

      // update index in the map
      keyVarIndexMapMut(k) = index + 1
      i += 1
    }
    uniqueKeyVariables
  }

  def generateOutputPath(key: String): String = {
    if (key.length == 0) return "/" + pathSegments.mkString("/") + "/result.txt"

    val outputPath = pathSegments.clone()
    val keyArray = key.split("/+").zipWithIndex
      .map{case (k, i) => (k, inputKeyToPathSegmentsIndexMap(i))}

    for ((k, i) <- keyArray) {
      outputPath(i) = k
    }

    "/" + outputPath.mkString("/") + "/result.txt"
  }
}