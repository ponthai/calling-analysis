package org.core.louvain

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import scala.Array.canBuildFrom

import org.util._

/**
 * Execute the louvain algorithim and save the vertices and edges in hdfs at each level.
 * Can also save locally if in local mode.
 * 
 * See LouvainHarness for algorithm details
 */
class HDFSLouvainRunner(minProgress:Int,progressCounter:Int,outputdir:String) extends LouvainHarness(minProgress:Int,progressCounter:Int){

  var qValues = Array[(Int,Double)]()
      
  override def saveLevel(sc:SparkContext,level:Int,q:Double,graph:Graph[VertexState,Long]) = {
      graph.vertices.sortBy( _._2.asInstanceOf[VertexState].community , ascending=true).saveAsTextFile(outputdir+"/level_"+level+"_vertices")
      graph.edges.saveAsTextFile(outputdir+"/level_"+level+"_edges")
      graph.edges.map( e => e.srcId + " " + e.dstId).saveAsTextFile(outputdir+"/level_"+level+"_MapEdges")
      //graph.vertices.map( {case (id,v) => ""+id+","+v.internalWeight+","+v.community }).saveAsTextFile(outputdir+"/level_"+level+"_vertices")
      //graph.edges.mapValues({case e=>""+e.srcId+","+e.dstId+","+e.attr}).saveAsTextFile(outputdir+"/level_"+level+"_edges")  
      qValues = qValues :+ ((level,q))
      println(s"qValue: $q")
        
      // overwrite the q values at each level
      sc.parallelize(qValues, 1).saveAsTextFile(outputdir+"/qvalues_"+level)
  }

  override def deletePath() = {
     HDFSFileService.removeFile(outputdir)
    /*
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://172.16.2.1:9000"), hadoopConf)   
    try { hdfs.delete(new org.apache.hadoop.fs.Path(outputdir), true) } catch { case _ : Throwable => { } }
    */
  }
  
}