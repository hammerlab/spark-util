package org.hammerlab.spark

import org.apache.hadoop.io.{ BytesWritable, NullWritable }
import org.apache.hadoop.mapred
import org.apache.hadoop.mapreduce.lib.input
import org.apache.spark.rdd.{ GetFileSplit, NonHadoopPartition }
import org.apache.spark.{ SparkConf, SparkContext }
import org.hammerlab.hadoop.splits.{ FileSplit, UnsplittableNewSequenceFileInputFormat, UnsplittableSequenceFileInputFormat }
import org.hammerlab.test.Suite

class HadoopPartitionTest
  extends Suite {

  val conf =
    new SparkConf()
      .setMaster("local[4]")
      .setAppName(getClass.getCanonicalName)

  lazy val sc = new SparkContext(conf)

  override def afterAll(): Unit = {
    super.afterAll()
    sc.stop()
  }

  val path = tmpPath()
  val pathStr = path.toString

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sc
      .parallelize(
        1 to 1000,
        4
      )
      .saveAsObjectFile(pathStr)
  }

  test("old hadoop rdd") {

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

  test("new hadoop rdd") {

    val rdd =
      sc
        .newAPIHadoopFile[
          NullWritable,
          BytesWritable,
          UnsplittableNewSequenceFileInputFormat[
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

  test("non-hadoop rdd") {
    intercept[NonHadoopPartition] {
      GetFileSplit(sc.parallelize(1 to 1000).partitions.head)
    }
  }

  test("casts") {
    val split = FileSplit(tmpPath(), 123, 456, Array("abc", "def"))
    ((split: input.FileSplit): FileSplit) should be(split)
    ((split: mapred.FileSplit): FileSplit) should be(split)
    split.end should be(579)
  }
}
