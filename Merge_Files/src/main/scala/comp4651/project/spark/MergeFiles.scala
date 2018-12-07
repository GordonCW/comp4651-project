package comp4651.project.spark

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.{FileStatus, FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.hadoop.io.IOUtils

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

    val pathSegmentsLen = inputPathPattern.pathSegments.length
    // convert the url like path to absolute path on HDFS to ease the processing
    pathRDD.map(p => {
      var pS = p.split("/+")
      val originlLen = pS.length
      if (pS(originlLen-1).length == 0) {
        "/" + pS.slice(originlLen-1-pathSegmentsLen,originlLen-1).mkString("/")
      } else {
        "/" + pS.slice(originlLen-pathSegmentsLen,originlLen).mkString("/")
      }
    })
  }

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("clean up latest directory")
    val sc = new SparkContext(sparkConf)

    // parse arguments
    val conf = new Conf(args)

    val inputPathPattern = new PathPattern(conf.input())
    val outputPathPattern = new OutputPathPattern(conf.output(), inputPathPattern)

    // use iterative BFS to get all "matched" directory path loaded into RDD
    val directoryPathsRDD = iterativeBFSLoadDirectories(sc, inputPathPattern).persist()
    println(directoryPathsRDD.collect().mkString("\n"))
    println("[Output] Number of directories matched: "+directoryPathsRDD.count())

    val extractKeyFunc = inputPathPattern.getExtractKeyFunc()

    // map into key value pair and group by key
    val keyDirectoryPathPairRDD = directoryPathsRDD.map(path => (extractKeyFunc(path), path))
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

    val generateOutputPathFunc = outputPathPattern.getGenerateOutputPathFunc()

    // for each group create new file and append all small files into it
    // also deleted successful group
    keyFilePairRDD.foreachPartition(it => {
      val fs = FileSystem.get(new Configuration())
      it.foreach{case (k, filePaths) => {

        // create an empty file based on output directory pattern
        val targetFilePath = generateOutputPathFunc(k)

        if (filePaths.length != 0) {
          val out = fs.create(new Path(targetFilePath), true)

          // append all the small files into the target file
          for (s <- filePaths) {
            val in = fs.open(new Path(s))
            try {
              IOUtils.copyBytes(in, out, fs.getConf, false)
              // can comment if every file end with a newline character
              out.write("\n".getBytes("UTF-8"))
            } finally {
              in.close()
            }
          }

          out.close()

          // delete small files here...
          for (s <- filePaths) {
            fs.delete(new Path(s), false)
          }
        } else {
          // do nothing
        }
      }}
    })

    // use BFS with backtracking to delete "useless" directories that matches input directory pattern


    sc.stop()
    println("[Output] Program finished successfully~")
  }
}