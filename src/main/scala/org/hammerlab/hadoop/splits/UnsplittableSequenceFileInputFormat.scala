package org.hammerlab.hadoop.splits

import java.io.IOException
import java.util

import org.apache.hadoop.fs.{ FileStatus, FileSystem, Path ⇒ HPath }
import org.apache.hadoop.mapred.{ JobConf, SequenceFileInputFormat }
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.lib.input

import scala.collection.JavaConverters._

/**
 * [[SequenceFileInputFormat]] that guarantees loading the same splits it was written with.
 */
class UnsplittableSequenceFileInputFormat[K, V]
  extends SequenceFileInputFormat[K, V] {

  override def isSplitable(fs: FileSystem, filename: HPath): Boolean = false

  /**
   * Ensure that partitions are read back in in the same order they were written; should be unnecessary as of Hadoop 2.8
   * / 3.x. See https://issues.apache.org/jira/browse/HADOOP-10798
   */
  override def listStatus(job: JobConf): Array[FileStatus] =
    super
      .listStatus(job)
      .sortBy {
        _.getPath.getName match {
          case PartFileBasename(idx) ⇒
            idx
          case basename ⇒
            throw new IOException(s"Bad partition file: $basename")
        }
      }
}

class UnsplittableNewSequenceFileInputFormat[K, V]
  extends input.SequenceFileInputFormat[K, V] {

  override def isSplitable(context: JobContext, filename: HPath): Boolean = false

  /**
   * Ensure that partitions are read back in in the same order they were written; should be unnecessary as of Hadoop 2.8
   * / 3.x. See https://issues.apache.org/jira/browse/HADOOP-10798
   */

  override def listStatus(job: JobContext): util.List[FileStatus] =
    super
      .listStatus(job)
      .asScala
      .sortBy {
        _.getPath.getName match {
          case PartFileBasename(idx) ⇒
            idx
          case basename ⇒
            throw new IOException(s"Bad partition file: $basename")
        }
      }
      .asJava
}
