package org.hammerlab.spark

import org.apache.hadoop.io.{ BytesWritable, NullWritable }
import org.apache.spark.rdd.GetFileSplit
import org.apache.spark.{ SparkConf, SparkContext }
import org.hammerlab.hadoop.splits.UnsplittableSequenceFileInputFormat
import org.hammerlab.test.Suite

class HadoopPartitionTest
  extends Suite {

  test("hadoop rdd") {
    val conf =
      new SparkConf()
        .setMaster("local[4]")
        .setAppName(getClass.getCanonicalName)

    val sc = new SparkContext(conf)
    val path = tmpPath()
    val pathStr = path.toString

    sc.parallelize(1 to 1000, 4).saveAsObjectFile(pathStr)

    val rdd =
      sc
        .hadoopFile[
          NullWritable,
          BytesWritable,
          UnsplittableSequenceFileInputFormat[
            NullWritable,
            BytesWritable
          ]
        ](
          pathStr
        )

    rdd
      .partitions
      .map(
        GetFileSplit(_).path
      ) should be(
      Array(
        path / "part-00000",
        path / "part-00001",
        path / "part-00002",
        path / "part-00003"
      )
    )
  }
}
