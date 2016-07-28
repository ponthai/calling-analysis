package org.util

import java.io.BufferedInputStream
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

object HDFSFileService {

  val conf = new Configuration()
  val hdfsCoreSitePath = new Path("/etc/hadoop/conf/core-site.xml")
  val hdfsHDFSSitePath = new Path("/etc/hadoop/conf/hdfs-site.xml")

  conf.addResource(hdfsCoreSitePath)
  conf.addResource(hdfsHDFSSitePath)

  val fileSystem = FileSystem.get(conf)

  def saveFile(filepath: String): Unit = {
    val file = new File(filepath)
    val out = fileSystem.create(new Path(file.getName))
    val in = new BufferedInputStream(new FileInputStream(file))
    var b = new Array[Byte](1024)
    var numBytes = in.read(b)
    while (numBytes > 0) {
      out.write(b, 0, numBytes)
      numBytes = in.read(b)
    }
    in.close()
    out.close()
  }

  def writeIterate(path: String, iter: java.util.Iterator[String]) = {
  removeFile(path)
	val file = new File(path)
	val out = fileSystem.create(new Path(file.getName), true)
	val br = new BufferedWriter(new OutputStreamWriter(out))
 
	//br.write("# Node Property Value List\n")
	
	while(iter.hasNext()){
		br.write(iter.next())
	}

	br.flush()
	br.close()
  }

  def removeFile(filename: String): Boolean = {
    val path = new Path(filename)
    fileSystem.delete(path, true)
  }

  def getFile(filename: String): InputStream = {
    val path = new Path(filename)
    fileSystem.open(path)
  }

  def createFolder(folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
  }
}
