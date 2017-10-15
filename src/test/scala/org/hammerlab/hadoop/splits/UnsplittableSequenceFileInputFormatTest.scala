package org.hammerlab.hadoop.splits

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred
import org.apache.hadoop.mapred.{ FileInputFormat, JobConf }
import FileInputFormat.setInputPaths
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class UnsplittableSequenceFileInputFormatTest
  extends Suite {

  test("part files") {
    val ifmt = new UnsplittableSequenceFileInputFormat[NullWritable, NullWritable]

    val jc = new JobConf()
    setInputPaths(jc, File("rdd"))

    val paths =
      ifmt
        .getSplits(jc, 2)
        .map(_.asInstanceOf[mapred.FileSplit])
        .map(FileSplit(_).path)

    paths should be(
      0 to 5 map(
        File("rdd") / PartFileBasename(_)
      )
    )
  }

  test("non-part file error") {
    val ifmt = new UnsplittableSequenceFileInputFormat[NullWritable, NullWritable]

    val jc = new JobConf()
    setInputPaths(jc, File("bad"))

    intercept[IllegalArgumentException] {
      ifmt.getSplits(jc, 2)
    }
    .getMessage should be(s"Bad partition file: error")

  }
}
