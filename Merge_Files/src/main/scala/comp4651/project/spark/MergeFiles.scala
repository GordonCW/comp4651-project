package comp4651.project.spark

object MergeFiles {

  def main(args: Array[String]): Unit = {

    // parse arguments
    val conf = new Conf(args)

    val inputPathPattern = new PathPattern(conf.input())
    val outputPathPattern = new PathPattern(conf.output())

    // condition checking
    if (!inputPathPattern.matchedKeyVariables(outputPathPattern)) {
      // throw exception here
    }

    // get root which indicates where to start searching
    val rootDir = inputPathPattern

    // build a data structure that can input level and output level type (key, *, or constant).



    // use iterative BFS to get all matched directory path loaded into rdd

    // map into key value pair and group by key

    // sort each group by modification time

    // for each group create new file and append all small files into it
    // also deleted successful group

    // use BFS with backtracking to delete "useless" directories that matches input directory pattern
  }
}