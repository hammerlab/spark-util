package org.hammerlab.hadoop.splits

import java.io.IOException

import org.apache.hadoop.fs.{ FileStatus, FileSystem, Path ⇒ HPath }
import org.apache.hadoop.mapred.{ JobConf, SequenceFileInputFormat }

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
