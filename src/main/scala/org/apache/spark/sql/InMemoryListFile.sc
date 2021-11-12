

import java.io.FileNotFoundException

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocatedFileStatus, Path}
import org.apache.hadoop.fs.viewfs.ViewFileSystem
import org.apache.hadoop.fs.{BlockLocation, FileStatus, PathFilter}
import org.apache.hadoop.hdfs.DistributedFileSystem

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.InMemoryFileIndex.{SerializableBlockLocation, SerializableFileStatus, bulkListLeafFiles, listLeafFiles, logTrace, logWarning, shouldFilterOut}

val spark = SparkSession
  .builder
  .appName("CostBasedReorderTest")
  .getOrCreate()

val numParallelism = 10
val paths = Seq("")
val serializedPaths = Seq("")

val sparkContext = spark.sparkContext

spark.

def listLeafFiles(
                   path: Path,
                   hadoopConf: Configuration,
                   filter: PathFilter,
                   sessionOpt: Option[SparkSession],
                   ignoreMissingFiles: Boolean,
                   ignoreLocality: Boolean,
                   isRootPath: Boolean,
                   maxLeafFile: Int): Seq[FileStatus] = {
  logInfo(s"Listing $path")
  val fs = path.getFileSystem(hadoopConf)

  // Note that statuses only include FileStatus for the files and dirs directly under path,
  // and does not include anything else recursively.
  val statuses: Array[FileStatus] = try {
    fs match {
      // DistributedFileSystem overrides listLocatedStatus to make 1 single call to namenode
      // to retrieve the file status with the file block location. The reason to still fallback
      // to listStatus is because the default implementation would potentially throw a
      // FileNotFoundException which is better handled by doing the lookups manually below.
      case (_: DistributedFileSystem | _: ViewFileSystem) if !ignoreLocality =>
        val remoteIter = fs.listLocatedStatus(path)
        new Iterator[LocatedFileStatus]() {
          def next(): LocatedFileStatus = remoteIter.next
          def hasNext(): Boolean = remoteIter.hasNext
        }.toArray
      case _ => fs.listStatus(path)
    }
  } catch {
    // If we are listing a root path (e.g. a top level directory of a table), we need to
    // ignore FileNotFoundExceptions during this root level of the listing because
    //
    //  (a) certain code paths might construct an InMemoryFileIndex with root paths that
    //      might not exist (i.e. not all callers are guaranteed to have checked
    //      path existence prior to constructing InMemoryFileIndex) and,
    //  (b) we need to ignore deleted root paths during REFRESH TABLE, otherwise we break
    //      existing behavior and break the ability drop SessionCatalog tables when tables'
    //      root directories have been deleted (which breaks a number of Spark's own tests).
    //
    // If we are NOT listing a root path then a FileNotFoundException here means that the
    // directory was present in a previous level of file listing but is absent in this
    // listing, likely indicating a race condition (e.g. concurrent table overwrite or S3
    // list inconsistency).
    //
    // The trade-off in supporting existing behaviors / use-cases is that we won't be
    // able to detect race conditions involving root paths being deleted during
    // InMemoryFileIndex construction. However, it's still a net improvement to detect and
    // fail-fast on the non-root cases. For more info see the SPARK-27676 review discussion.
    case _: FileNotFoundException if isRootPath || ignoreMissingFiles =>
      logWarning(s"The directory $path was not found. Was it deleted very recently?")
      Array.empty[FileStatus]
  }

  val filteredStatuses = statuses.filterNot(status => shouldFilterOut(status.getPath.getName))

  val allLeafStatuses = {
    val (dirs, topLevelFiles) = filteredStatuses.partition(_.isDirectory)
    val nestedFiles: Seq[FileStatus] = sessionOpt match {
      case Some(session) =>
        bulkListLeafFiles(
          dirs.map(_.getPath),
          hadoopConf,
          filter,
          session,
          areRootPaths = false,
          maxLeafFile
        ).flatMap(_._2)
      case _ =>
        dirs.flatMap { dir =>
          listLeafFiles(
            dir.getPath,
            hadoopConf,
            filter,
            sessionOpt,
            ignoreMissingFiles = ignoreMissingFiles,
            ignoreLocality = ignoreLocality,
            isRootPath = false,
            maxLeafFile)
        }
    }
    val allFiles = topLevelFiles ++ nestedFiles
    if (filter != null) allFiles.filter(f => filter.accept(f.getPath)) else allFiles
  }

  val missingFiles = mutable.ArrayBuffer.empty[String]
  val filteredLeafStatusesLimited = if (maxLeafFile > 0) {
    allLeafStatuses.filterNot(status => shouldFilterOut(status.getPath.getName))
      .take(maxLeafFile)
  } else {
    allLeafStatuses.filterNot(status => shouldFilterOut(status.getPath.getName))
  }
  val resolvedLeafStatuses = filteredLeafStatusesLimited.flatMap {
    case f: LocatedFileStatus =>
      Some(f)

    // NOTE:
    //
    // - Although S3/S3A/S3N file system can be quite slow for remote file metadata
    //   operations, calling `getFileBlockLocations` does no harm here since these file system
    //   implementations don't actually issue RPC for this method.
    //
    // - Here we are calling `getFileBlockLocations` in a sequential manner, but it should not
    //   be a big deal since we always use to `bulkListLeafFiles` when the number of
    //   paths exceeds threshold.
    case f if !ignoreLocality =>
      // The other constructor of LocatedFileStatus will call FileStatus.getPermission(),
      // which is very slow on some file system (RawLocalFileSystem, which is launch a
      // subprocess and parse the stdout).
      try {
        val locations = fs.getFileBlockLocations(f, 0, f.getLen).map { loc =>
          // Store BlockLocation objects to consume less memory
          if (loc.getClass == classOf[BlockLocation]) {
            loc
          } else {
            new BlockLocation(loc.getNames, loc.getHosts, loc.getOffset, loc.getLength)
          }
        }
        val lfs = new LocatedFileStatus(f.getLen, f.isDirectory, f.getReplication, f.getBlockSize,
          f.getModificationTime, 0, null, null, null, null, f.getPath, locations)
        if (f.isSymlink) {
          lfs.setSymlink(f.getSymlink)
        }
        Some(lfs)
      } catch {
        case _: FileNotFoundException if ignoreMissingFiles =>
          missingFiles += f.getPath.toString
          None
      }

    case f => Some(f)
  }

  if (missingFiles.nonEmpty) {
    logWarning(
      s"the following files were missing during file scan:\n  ${missingFiles.mkString("\n  ")}")
  }

  resolvedLeafStatuses
}

val statusMap = try {
  val description = paths.size match {
    case 0 =>
      s"Listing leaf files and directories 0 paths"
    case 1 =>
      s"Listing leaf files and directories for 1 path:<br/>${paths(0)}"
    case s =>
      s"Listing leaf files and directories for $s paths:<br/>${paths(0)}, ..."
  }
  sparkContext.setJobDescription(description)
  sparkContext
    .parallelize(serializedPaths, numParallelism)
    .mapPartitions { pathStrings =>
      val hadoopConf = serializableConfiguration.value
      pathStrings.map(new Path(_)).toSeq.map { path =>
        val leafFiles = listLeafFiles(
          path,
          hadoopConf,
          filter,
          None,
          ignoreMissingFiles = ignoreMissingFiles,
          ignoreLocality = ignoreLocality,
          isRootPath = areRootPaths,
          maxLeafFiles)
        (path, leafFiles)
      }.iterator
    }.map { case (path, statuses) =>
    val serializableStatuses = statuses.map { status =>
      // Turn FileStatus into SerializableFileStatus so we can send it back to the driver
      val blockLocations = status match {
        case f: LocatedFileStatus =>
          f.getBlockLocations.map { loc =>
            SerializableBlockLocation(
              loc.getNames,
              loc.getHosts,
              loc.getOffset,
              loc.getLength)
          }

        case _ =>
          Array.empty[SerializableBlockLocation]
      }

      SerializableFileStatus(
        status.getPath.toString,
        status.getLen,
        status.isDirectory,
        status.getReplication,
        status.getBlockSize,
        status.getModificationTime,
        status.getAccessTime,
        blockLocations)
    }
    (path.toString, serializableStatuses)
  }.collectAsIterator((pair : (String, Seq[SerializableFileStatus])) => Iterator(pair))
} finally {
  sparkContext.setJobDescription(previousJobDescription)
}