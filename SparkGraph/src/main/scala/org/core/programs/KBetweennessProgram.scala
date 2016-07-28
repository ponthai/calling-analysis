package org.core.programs

import scala.reflect.ClassTag
import org.apache.spark.Logging
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.graphx.Pregel
import org.apache.spark.graphx.EdgeDirection
import scala.collection.mutable.Stack
import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

object KBetweennessProgram 
{
  def run[VD: ClassTag, ED: ClassTag](
            graph: Graph[VD, ED], k: Int): Graph[Double, Double] =
  {
      val kGraphletsGraph =
        createKGraphlets(graph, k)
	//createKGraphletsByPartition(graph, k)	

      println("DONE ... kGraphletsGraph ")
        
      val vertexKBcGraph =
        kGraphletsGraph
        .mapVertices((id, attr) => (id, computeVertexBetweenessCentrality(id, attr._2, attr._3)))
	
      println("DONE ... kGraphletsGraph.mapVertices ")
    
      val kBCGraph: Graph[Double, Double] = 
        aggregateGraphletsBetweennessScores(vertexKBcGraph)
	
      println("DONE ... kGraphletsGraph.mapVertices.aggregateGraphlet ")
     
      kBCGraph
  }

  def createKGraphletsByPartition[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], k: Int): Graph[(Double, List[VertexId], List[(VertexId, VertexId)]), Double] = 
  { 
    var graphContainingGraphlets : Graph[(Double, List[VertexId], List[(VertexId, VertexId)]), Double] =
      graph
      // Init edges to hold - Edge Betweenness to 0.0
      .mapTriplets[Double]({x: EdgeTriplet[VD, ED] => (0.0) })
	      
      // Init vertices to hold - Vertex betweenness (0.0), and K distance Edge list (empty)
      .mapVertices( (id, attr) => (0.0, List[VertexId](id), List[(VertexId, VertexId)]()))
      //.reverse // Because GraphX is directed and we want graphlets containing all vertices a vertex might effect 
      //.cache()	
	

      def vertexProgram(id: VertexId, attr: (Double, List[VertexId], List[(VertexId, VertexId)]), msgSum: (List[VertexId], List[(VertexId, VertexId)])): (Double, List[VertexId], List[(VertexId, VertexId)]) =
        (attr._1, attr._2.union(msgSum._1).distinct, attr._3.union(msgSum._2).distinct)
      def sendMessage(edge: EdgeTriplet[(Double, List[VertexId], List[(VertexId, VertexId)]), Double]) : Iterator[(VertexId, (List[VertexId], List[(VertexId, VertexId)]))]=
        Iterator((edge.dstId, (edge.srcAttr._2.:+(edge.srcId) , edge.srcAttr._3.+:(edge.srcId, edge.dstId))),
                 (edge.srcId, (edge.dstAttr._2.:+(edge.dstId) , edge.dstAttr._3.+:(edge.srcId, edge.dstId)))
                )
      def messageCombiner(a: (List[VertexId], List[(VertexId, VertexId)]), b: (List[VertexId], List[(VertexId, VertexId)])): (List[VertexId], List[(VertexId, VertexId)]) = 
        (a._1.union(b._1) , a._2.union(b._2) )
      // The initial message received by all vertices in PageRank
      val initialMessage = (List[VertexId](), List[(VertexId, VertexId)]())


    //Process Pregel with partition
    var ROUND = 9 
    println("Pregel perform ... " + ROUND + " Round")
    var sGraph : Graph[(Double, List[VertexId], List[(VertexId, VertexId)]), Double] = null
    var count = ROUND
	
    println("Start Pregel ...")
    println("Total Vertices = " + graphContainingGraphlets.vertices.count)
    println("Total Edges = " + graphContainingGraphlets.edges.count)   	
	
	  while(count >= 0){
     		sGraph =
             	graphContainingGraphlets
            	.subgraph(
              		epred = e => true,
              		vpred = ( id, attr ) => ( (id%10) == count)
            	)
 
      println("Round  %"+count+ " : Total Vertices = " +sGraph.vertices.count + " , Total Edges = " + sGraph.edges.count)
  		
  		// Execute pregel for k iterations, get all vertices/edges in distance k for every node
      sGraph = Pregel(sGraph, initialMessage, k, activeDirection = EdgeDirection.Both)(vertexProgram, sendMessage, messageCombiner)

  		var vUpdate = graphContainingGraphlets.vertices.innerJoin(sGraph.vertices)( (vid, attr, a) => (a._1, attr._2.union(a._2).distinct, attr._3.union(a._3).distinct))
  		
      //Merge graph with processed vertices
  		graphContainingGraphlets = Graph(
  			vUpdate.union(graphContainingGraphlets.vertices), 
  			graphContainingGraphlets.edges.union(sGraph.edges)
  		).groupEdges( (attr1, attr2) => attr1 + attr2 )
  		
      //Clear cache befor start new round
  		sGraph.unpersist(blocking=true)
  		
  		count = count - 1
	  }

  	graphContainingGraphlets
  } 


  def createKGraphlets[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], k: Int): Graph[(Double, List[VertexId], List[(VertexId, VertexId)]), Double] = 
  { 
    val graphContainingGraphlets : Graph[(Double, List[VertexId], List[(VertexId, VertexId)]), Double] =
      graph
      // Init edges to hold - Edge Betweenness to 0.0
      .mapTriplets[Double]({x: EdgeTriplet[VD, ED] => (0.0) })
      // Init vertices to hold - Vertex betweenness (0.0), and K distance Edge list (empty)
      .mapVertices( (id, attr) => (0.0, List[VertexId](id), List[(VertexId, VertexId)]()))
      //.reverse // Because GraphX is directed and we want graphlets containing all vertices a vertex might effect 
      .cache()
      //graphContainingGraphlets.checkpoint()

    
      def vertexProgram(id: VertexId, attr: (Double, List[VertexId], List[(VertexId, VertexId)]), msgSum: (List[VertexId], List[(VertexId, VertexId)])): (Double, List[VertexId], List[(VertexId, VertexId)]) =
        (attr._1, attr._2.union(msgSum._1).distinct, attr._3.union(msgSum._2).distinct)
      def sendMessage(edge: EdgeTriplet[(Double, List[VertexId], List[(VertexId, VertexId)]), Double]) : Iterator[(VertexId, (List[VertexId], List[(VertexId, VertexId)]))]=
        Iterator((edge.dstId, (edge.srcAttr._2.:+(edge.srcId) , edge.srcAttr._3.+:(edge.srcId, edge.dstId))),
                 (edge.srcId, (edge.dstAttr._2.:+(edge.dstId) , edge.dstAttr._3.+:(edge.srcId, edge.dstId)))
                )
      def messageCombiner(a: (List[VertexId], List[(VertexId, VertexId)]), b: (List[VertexId], List[(VertexId, VertexId)])): (List[VertexId], List[(VertexId, VertexId)]) = 
        (a._1.union(b._1) , a._2.union(b._2) )
      // The initial message received by all vertices in PageRank
      val initialMessage = (List[VertexId](), List[(VertexId, VertexId)]())

        // Execute pregel for k iterations, get all vertices/edges in distance k for every node
        Pregel(graphContainingGraphlets, initialMessage, k, activeDirection = EdgeDirection.Both)(
          vertexProgram, sendMessage, messageCombiner)
        //.reverse // return to originial directon
  }

  def computeVertexBetweenessCentrality(id: VertexId, vlist: List[VertexId], elist: List[(VertexId, VertexId)]):  List[(VertexId, Double)] = 
  {
    // Init data structures
    val s = Stack[VertexId]()
    val q = Queue[VertexId]()
    
    val dist = new HashMap[VertexId, Double]()
    val sigma = new HashMap[VertexId, Double]()
    val delta = new HashMap[VertexId, Double]()
    val predecessors = new HashMap[VertexId, ListBuffer[VertexId]]()
    val neighbourMap : HashMap[VertexId, List[VertexId]] = getNeighbourMap(vlist,elist)
    val medBC = new ListBuffer[(VertexId, Double)]()
      
    for (vertex <- vlist)
    {
      dist.put(vertex, Double.MaxValue)
      sigma.put(vertex, 0.0)
      delta.put(vertex, 0.0)
      predecessors.put(vertex, ListBuffer[VertexId]())
    }
    
    // Init values before first iteration
    dist(id) = 0.0
    sigma(id) = 1.0
    q.enqueue(id)
    
    // Go over all queued vertices
    while (!(q.isEmpty))
    {
      val v = q.dequeue()
      s.push(v)
      // Go over v's neighbours 
      for (w <- neighbourMap(v))
      {
        if (dist(w) == Double.MaxValue)
        {
          dist(w) = dist(v) + 1.0
          q.enqueue(w)
        }
        if(dist(w) == (dist(v) + 1.0))
        {
          sigma(w) += sigma(v)
          predecessors(w).+=(v)
        }
      }
    }
    
    while (!(s.isEmpty))
    {
      val v = s.pop()
      for (w <- predecessors(v))
      {
        delta(w) += (sigma(w) / sigma(v)) * (delta(v) + 1.0)
      }
      if (v != id)
      {
        medBC.append((v, delta(v)))
      }
    }
    
    medBC.toList
  }
  
  def getNeighbourMap(vlist: List[VertexId], elist: List[(VertexId, VertexId)]): HashMap[VertexId, List[VertexId]] =
  {
    val neighbourList = new HashMap[VertexId, List[VertexId]]()
    
    vlist.foreach { case(v) => 
      {val nlist = (elist.filter{case(e) => ((e._1 == v) || (e._2 == v))})
               .map{case(e) => {if (e._1 == v) e._2 else e._1 }}
       neighbourList.+=((v,nlist.distinct))
      }
    }
    
    neighbourList
  }
  
  def aggregateGraphletsBetweennessScores(vertexKBcGraph: Graph[(VertexId, List[(VertexId, Double)]), Double]): Graph[Double, Double] =
  {
    val DEFAULT_BC = 0.0
    
    val kBCAdditions = 
      vertexKBcGraph
      .vertices
      .flatMap{case(v, (id, listkBC)) => 
                (listkBC.map{case(w, kBC)=>(w, kBC)})}
    
    val verticeskBC =
      kBCAdditions
      .reduceByKey(_ + _)
      
    val kBCGraph = 
      vertexKBcGraph
      .outerJoinVertices(verticeskBC)({case(v_id,(g_id,g_medkBC),kBC) => (kBC.getOrElse(DEFAULT_BC))})
      
    kBCGraph
  }

}
