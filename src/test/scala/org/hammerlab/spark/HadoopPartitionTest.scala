package org.hammerlab.spark

import org.apache.hadoop.io.{ BytesWritable, NullWritable }
import org.apache.hadoop.mapred
import org.apache.hadoop.mapreduce.lib.input
import org.apache.spark.rdd.{ AsHadoopPartition, AsNewHadoopPartition, GetFileSplit, NonHadoopPartition }
import org.hammerlab.hadoop.splits.{ FileSplit, UnsplittableNewSequenceFileInputFormat, UnsplittableSequenceFileInputFormat }

class HadoopPartitionTest
  extends ContextSuite {

  val path = tmpPath()
  val pathStr = path.toString

  import org.hammerlab.kryo._

  register(cls[Range])

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

    val partitions = rdd.partitions

    partitions
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

    partitions.map(AsHadoopPartition(_).index) should be(0 to 3)
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

    val partitions = rdd.partitions

    partitions
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

    partitions.map(AsNewHadoopPartition(_).index) should be(0 to 3)
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
