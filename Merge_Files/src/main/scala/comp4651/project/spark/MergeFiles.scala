package comp4651.project.spark

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.{FileStatus, FileSystem, LocatedFileStatus, Path, RemoteIterator}

import scala.collection.mutable.ArrayBuffer

object MergeFiles {

  def getDirListPair(fs: FileSystem, parent: String): ArrayBuffer[(String, String)] = {
    val children = ArrayBuffer.empty[(String, String)]
    val allStatus: Array[FileStatus] = fs.listStatus(new Path(parent))
    for (fSta <- allStatus) {
      if (fSta.isDirectory) {
        val childPath = fSta.getPath
        children += ((childPath.toString, childPath.getName))
      }
    }

    children
  }

  def getDirList(fs: FileSystem, parent: String): ArrayBuffer[String] = {
    val children = ArrayBuffer.empty[String]
    val allStatus: Array[FileStatus] = fs.listStatus(new Path(parent))
    for (fSta <- allStatus) {
      if (fSta.isDirectory) {
        val childPath = fSta.getPath
        children += (childPath.toString)
      }
    }

    children
  }

  def iterativeBFSLoadDirectories(sc: SparkContext, inputPathPattern: PathPattern): RDD[String] = {

    // get root which indicates where to start searching
    val rootDir = inputPathPattern.rootDirPath
    var pathRDD = sc.parallelize(Array(rootDir))

    // rootDir is the root node of the directory tree
    // so level is 1
    var currentLevel = 1

    while(inputPathPattern.hasNextLevel(currentLevel)) {

      currentLevel += 1

      // expand one level down
      // two possible cases: constant or non constant
      // constant case requires filtering
      if (inputPathPattern.isCurrentLevelConstant(currentLevel)) {
        val currentLevelConstant = inputPathPattern.getLevel(currentLevel)

        // use map partitions to save the object creation of fs
        pathRDD = pathRDD.mapPartitions(it => {
          val fs = FileSystem.get(new Configuration())

          it.flatMap(path => getDirListPair(fs, path))

            // constant case requires filtering
            .filter { case (path, name) =>
              currentLevelConstant == name
            }
            .map{ case (path, name) => path }
        })
      } else {
        // non constant level case

        // use map partitions to save the object creation of fs
        pathRDD = pathRDD.mapPartitions(it => {
          val fs = FileSystem.get(new Configuration())
          it.flatMap(path => getDirList(fs, path))
        })
      }
    }
    pathRDD
  }

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("clean up latest directory")
    val sc = new SparkContext(sparkConf)

    // parse arguments
    val conf = new Conf(args)

    val inputPathPattern = new PathPattern(conf.input())
    val outputPathPattern = new PathPattern(conf.output())

    // condition checking
    if (!inputPathPattern.matchedKeyVariables(outputPathPattern)) {
      throw new Exception("Input pattern and output pattern should have the same number of key variables.")
    }


    // build a data structure that can input level and output level type (key, *, or constant).



    // use iterative BFS to get all "matched" directory path loaded into RDD
    val directoryPathsRDD = iterativeBFSLoadDirectories(sc, inputPathPattern).persist()
    print("[Output] Number of directories matched: "+directoryPathsRDD.count())

    // map into key value pair and group by key
    val keyDirectoryPathPairRDD = directoryPathsRDD.map(path => (inputPathPattern.extractKey(path), path))
      .groupByKey()

    // load files path for each group and sort by modification time
    val keyFilePairRDD = keyDirectoryPathPairRDD.mapPartitions(it => {
      val fs = FileSystem.get(new Configuration())

      // each key will form one group
      it.map{case (key, dirPaths) => {
        // in each group list its immediate children files
        val filePaths = dirPaths.flatMap(p => {
          val allStatus: RemoteIterator[LocatedFileStatus] = fs.listFiles(new Path(p), false)
          val timePathPair = ArrayBuffer.empty[(Long, String)]
          while (allStatus.hasNext) {
            val status = allStatus.next()
            timePathPair += ((status.getModificationTime, status.getPath.toString))
          }
          timePathPair
        })
          .toArray

          // sort by modification time
          .sortWith(_._1 < _._1)

          // discard the time
          .map {case (time, path) => path }

        (key, filePaths)
      }}
    })

    // for each group create new file and append all small files into it
    // also deleted successful group
    keyFilePairRDD.foreachPartition{case (key, filePaths) => {
      
    }}

    // use BFS with backtracking to delete "useless" directories that matches input directory pattern
  }
}