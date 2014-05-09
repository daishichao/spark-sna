import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import scala.annotation.tailrec

import scala.util.Try
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValue
import com.typesafe.config.ConfigValueType._
import java.util.Map.Entry
import java.io.File

import System.{currentTimeMillis => now}


object GraphTest {

  object Configuration {
    private def getPropertiesList( key: String ) = {
      val list = Try( config.getConfig( key ).entrySet().toArray ).getOrElse( Array() )
      list.map( x => {
        val p = x.asInstanceOf[Entry[String, ConfigValue]]
        val k = p.getKey

        val v = p.getValue.valueType match {
          case BOOLEAN => config.getBoolean( key + "." + k )
          case STRING => config.getString( key + "." + k )
          case NUMBER => config.getDouble( key + "." + k )
          case _ => config.getString( key + "." + k )
        }
        ( k.replace( "_", "." ), v.toString )
      } )
    }

    // Spark configuration - loading from "resources/application.conf"
    private var config = ConfigFactory.load()
    lazy val SPARK_MASTER_HOST = Try( config.getString( "spark.master_host" ) ).getOrElse( "local" )
    lazy val SPARK_MASTER_PORT = Try( config.getInt( "spark.master_port" ) ).getOrElse( 7077 )
    lazy val SPARK_HOME = Try( config.getString( "spark.home" ) ).getOrElse( "/home/spark" )
    lazy val SPARK_MEMORY = Try( config.getString( "spark.memory" ) ).getOrElse( "1g" )
    lazy val SPARK_OPTIONS = getPropertiesList( "spark.options" )
  }

  def connectToSparkCluster(): SparkContext = {
    import Configuration._
    // get the name of the packaged
    val thisPackagedJar = new File( "target/scala-2.10" ).listFiles.filter( x => x.isFile && x.getName.toLowerCase.takeRight( 4 ) == ".jar" ).toList.map( _.toString )

    // Scan for external libraries in folder 'lib'
    // All the JAR files in this folder will be shipped to the cluster
    val libs = new File( "lib" ).listFiles.filter( x => x.isFile && x.getName.toLowerCase.takeRight( 4 ) == ".jar" ).toList.map( _.toString )

    val master = if ( SPARK_MASTER_HOST.toUpperCase == "LOCAL" ) "local" else SPARK_MASTER_HOST + ":" + SPARK_MASTER_PORT

    // Spark Context configuration
    val scConf =
      SPARK_OPTIONS.fold(
        new SparkConf()
          .setMaster( master )
          .setAppName( "TemplateApplication" )
          .set( "spark.executor.memory", SPARK_MEMORY )
          .setSparkHome( SPARK_HOME )
          .setJars( libs ++ thisPackagedJar )
          )( ( c, p ) => { // apply each spark option from the configuration file in section "spark.options"
              val ( k, v ) = p.asInstanceOf[( String, String )]
              c.asInstanceOf[SparkConf].set( k, v )
            }
          ).asInstanceOf[SparkConf]
    // Create and return the spark context to be used through the entire KYC application
    new SparkContext( scConf )
  }

  def buildUndirGraph(nodeFile: String, edgeFile: String, sc: SparkContext) : Graph[String,(Double,Double)]={
    val nodes : RDD[(VertexId,String)] = sc.textFile(nodeFile).map(_.split(",")).map(x => (x(0).toLong,x(1)))
    val edges : RDD[Edge[(Double,Double)]] = sc.textFile(edgeFile).map(_.split(",")).map(x => Edge(x(0).toLong,x(1).toLong, if(x.length>2){(x(3).toDouble,x(3).toDouble)} else {(1.,1.)}))
    Graph(nodes,edges,"-1").subgraph(vpred = (id, attr) => attr != "-1")
  }

  def buildDirGraph(nodeFile: String, edgeFile: String, sc: SparkContext) : Graph[String,(Double, Double)] = {
    val nodes : RDD[(VertexId,String)] = sc.textFile(nodeFile).map(_.split(",")).map(x => (x(0).toLong,x(1)))
    val unfilteredEdges = sc.textFile(edgeFile).map(_.split(",")).map(x => Edge(x(0).toLong,x(1).toLong,if(x.length>2) {x(3).toDouble} else {1.}))
    val edges = unfilteredEdges.map(x => if(x.srcId < x.dstId) {Edge(x.srcId,x.dstId,(x.attr,0.))} else { if (x.srcId > x.dstId) {Edge(x.dstId,x.srcId,(0.,x.attr))} else {Edge(x.srcId,x.dstId,(x.attr,x.attr))}})
    val graph = Graph(nodes,edges,"-1").subgraph(vpred = (id, attr) => attr != "-1")
    
    graph.groupEdges((a,b) => (math.max(a._1,b._1),math.max(a._2,b._2)))
    
  }

  def buildGraph(nodeFile: String, edgeFile: String, sc: SparkContext) : Graph[String,Double]={
    // construction of a graph
    val nodes : RDD[(VertexId,String)] = sc.textFile(nodeFile).map(_.split(",")).map(x => (x(0).toLong,x(1)))
    val edges : RDD[Edge[Double]] = sc.textFile(edgeFile).map(_.split(",")).map(x => Edge(x(0).toLong,x(1).toLong, if(x.length>2){x(3).toDouble} else {1.}))
    Graph(nodes,edges,"-1")
  }
  
  /*def socialLeaders(graph: Graph[String,Double]) : VertexRDD[Boolean] = {
    // computation of the social leaders in the graph
    val triCountsGraph = graph.triangleCount()
    triCountsGraph.mapReduceTriplets[Boolean](
      triplet => { // Map Function
        if (triplet.srcAttr >= triplet.dstAttr) {
        // Send message to destination vertex containing counter and age
          Iterator((triplet.srcId, true))
        } else {
        // Don't send a message for this triplet
          Iterator((triplet.srcId, false))
        }
      },
      // Add counter and age
      (a, b) => a && b // Reduce Function
    )

  }

  def edgeSignificance(graph: Graph[String,Double]) = {
    // calculate degrees (will be used later on)
    val deg = graph.outDegrees
    // calculate strenghts to build a new graph, where each node has its strength as data.
    val strength = graph.mapReduceTriplets[Double](triplet => Iterator((triplet.srcId,triplet.attr)) , (a,b) => a+b)
    val gStrength = Graph(strength,graph.edges)
    // Calculate the fraction (or prob) on each edge, for this, we build a new graph that contains this information.
    val gProb = gStrength.mapTriplets(triplet => if(triplet.srcAttr>0){triplet.attr/triplet.srcAttr} else {0.})
    // we combine this prob information with the degrees earlier computed to constuct a new graph, where we have the degree of the nodes as attribute 
    // on the nodes and the probability as attribute on the edges
    val gDeg = Graph(deg,gProb.edges)
    val gSig = gDeg.mapTriplets(triplet => if(triplet.srcAttr>1){(1.-triplet.attr)/(triplet.srcAttr-1.)} else {1.})

  }*/

  def socialLeaders[VD: ClassTag,ED: ClassTag](graph: Graph[VD,ED]) : VertexRDD[Boolean] = {
    // computation of the social leaders in the graph
    val g = graph.triangleCount()
    g.mapReduceTriplets[Boolean](
      triplet => { // Map Function
        if (triplet.srcAttr > triplet.dstAttr) {
        // Send message to source and destination vertex with boolean
          Iterator((triplet.srcId, true),(triplet.dstId,false))
        } else {
          if (triplet.srcAttr < triplet.dstAttr){
            Iterator((triplet.srcId, false),(triplet.dstId,true))
          }
          else {
            Iterator((triplet.srcId,true),(triplet.dstId,true))
          }
        }
      },
      // a leader needs to be better than all his neighbours.
      (a, b) => a && b // Reduce Function
    )

  }

  def edgeSignificance[VD: ClassTag](graph: Graph[VD,(Double,Double)]) : Graph[VD,(Double,Double)] = {
    // calculate degrees (will be used later on)
    val deg = graph.degrees
    //println(deg.collect.mkString("\n"))
    // calculate strenghts to build a new graph, where each node has its strength as data.
    val strength = graph.mapReduceTriplets[Double](triplet => Iterator((triplet.srcId,triplet.attr._1),(triplet.dstId,triplet.attr._2)) , (a,b) => a+b)
    //println(streght.collect.mkString("\n"))
    val gStrength = Graph(strength,graph.edges)
    // Calculate the fraction (or prob) on each edge, for this, we build a new graph that contains this information.
    val gProb = gStrength.mapTriplets(triplet => (if(triplet.srcAttr>0){triplet.attr._1/triplet.srcAttr} else {0.},if(triplet.dstAttr>0){triplet.attr._2/triplet.dstAttr} else {0.}))
    // we combine this prob information with the degrees earlier computed to constuct a new graph, where we have the degree of the nodes as attribute 
    // on the nodes and the probability as attribute on the edges
    val gDeg = Graph(deg,gProb.edges)
    
    val oGraph = gDeg.mapTriplets(triplet => (if(triplet.srcAttr>1){math.pow(1.-triplet.attr._1,triplet.srcAttr-1.)} else {1.},if(triplet.dstAttr>1){math.pow(1.-triplet.attr._2,triplet.dstAttr-1.)} else {1.}))
    Graph(graph.vertices,oGraph.edges)
  }

  def kCoreDecomposition[VD: ClassTag,ED: ClassTag](graph: Graph[VD,ED]) : RDD[(VertexId,Long)] = {
    
    val deg = graph.degrees.map(x => (x._1,x._2.toLong))
    var gCore = Graph(deg,graph.edges.map(x => Edge(x.srcId,x.dstId,1L)))
    var newCores : RDD[(VertexId,Long)] = deg
    var modifs : Long = 1
    var core: Long = 1
    var nEdges: Long = gCore.edges.count
    var newEdgeCount: Long = 0
    while(nEdges>0) {
      modifs = 1
      println("Busy with core " + core)
      while(modifs > 0){
        // attributes on edges are 1 if both vertices have a core number higher than the current core, and 0 otherwise (simulates the effect of peeling the network)
        gCore = gCore.mapTriplets(triplet => if (triplet.srcAttr <= core || triplet.dstAttr <= core) 0 else 1 )
        // compute cores based on number of active edges (need to keep all edges to still have a result for vertices with no active edge)
        newCores = gCore.mapReduceTriplets[Long]( triplet => Iterator((triplet.srcId,triplet.attr),(triplet.dstId,triplet.attr)) , (a,b) => a+b )
        // count how many edges are still active in the network
        newEdgeCount = newCores.map(x=>x._2).reduce((a,b) => a+b)
        modifs = nEdges - newEdgeCount
        if (modifs > 0){
          // If previous core number is already <= current core, the node is already peeled out & doesn't need to be computed anymore. Otherwise if less than core active edges, we fix it to the current core.
          gCore = Graph(gCore.vertices.innerJoin(newCores){(vid,a,b) => if(a <= core){a} else { if (b < core) core else b }},gCore.edges)
        }
        nEdges = newEdgeCount
        println("Number of links deleted : " + modifs)
        //println(gCore.vertices.collect.mkString(", "))
        
      }
      core += 1
    }
    gCore.vertices
  }

  def kCoreDecomposition2[VD: ClassTag,ED: ClassTag](graph: Graph[VD,ED]) : RDD[(VertexId,Long)] = {
    
    var gCore = graph.outerJoinVertices(graph.degrees){ (vid, vdata, deg) => deg.getOrElse(0).toLong }.mapEdges( e => 1L )
    var cores = graph.vertices.mapValues( v => -1L )


    //var newCores : RDD[(VertexId,Long)] = deg
    var modifs : Long = 1
    var core: Long = 0
    var nEdges: Long = gCore.edges.count
    var newEdgeCount: Long = 0
    while(nEdges>0) {
      modifs = 1
      println("Busy with core " + core)
      while(modifs > 0){
        println("core = "+core)
        println("inner loop")
        // attributes on edges are 1 if both vertices have a core number higher than the current core, and 0 otherwise (simulates the effect of peeling the network)
        gCore = gCore.subgraph(vpred = (v,d) => d > core)
        // compute cores based on number of active edges (need to keep all edges to still have a result for vertices with no active edge)
        def vertexFunc(vdata: Long, deg: Option[Int], core: Long) : Long = {
          println("core in func = "+core)
          //println("vdata="+vdata)
          if(vdata >= 0){
            //println("kikoulol")
            vdata
          }
          else
          {
            deg.getOrElse(-1) match {
              case -1 => core
              case _ => -1L
            }
          }
        }
        println("cores")
        println(cores.collect.mkString("\n"))
        println("degrees")
        println(gCore.degrees.collect.mkString("\n"))

        val coreTmp = core
        cores = cores.leftJoin(gCore.degrees){(vid,vdata,deg) => vertexFunc(vdata,deg,coreTmp)}

        println("cores after leftJoin")
        println(cores.collect.mkString("\n"))
        gCore = gCore.outerJoinVertices(gCore.degrees){ (vid,vdata,deg) => deg.getOrElse(0).toLong}

        // count how many edges are still active in the network
        newEdgeCount = gCore.edges.count
        modifs = nEdges - newEdgeCount
        nEdges = newEdgeCount
        println("Number of links deleted : " + modifs)
        //println(gCore.vertices.collect.mkString(", "))
        
      }
      core += 1
    }
    cores
  }


  def kCoreDecomposition3[VD: ClassTag,ED: ClassTag](graph: Graph[VD,ED]) : RDD[(VertexId,Long)] = {
    
    def core(graph: Graph[Long,ED],coreVal: Long): RDD[(VertexId,Long)] = {
      if (graph.numEdges==0)
      {
        graph.vertices.mapValues(v => coreVal)
      }
      else
      {
        val g = graph.outerJoinVertices(graph.degrees){ (vid,vdata,deg) => deg.getOrElse(0).toLong}
        val c = coreVal
        if (g.subgraph(vpred = (vid,deg) => deg <= c).numVertices == 0)
        {
          core(g,c+1)
        }
        else
        {
          g.vertices.leftJoin(core(g.subgraph(vpred = (vid,deg) => deg > c),c)){(vid,a,b) => if(a == c) a else b.getOrElse(c)}
        }
      }
    }

    core(graph.outerJoinVertices(graph.degrees){(vid,data,deg) => deg.getOrElse(0).toLong},0L)
  }


  def kCoreDecomposition4[VD: ClassTag,ED: ClassTag](graph: Graph[VD,ED]) : RDD[(VertexId,Long)] = {
    
    @tailrec def core(graph: Graph[Long,ED],coreVal: Long,cores: VertexRDD[Long]): RDD[(VertexId,Long)] = {
      if (graph.numEdges==0) {
        cores
      }
      else {
        val g = graph.outerJoinVertices(graph.degrees){ (vid,vdata,deg) => deg.getOrElse(0).toLong}
        val c = if (g.subgraph(vpred = (vid,deg) => deg <= coreVal).numVertices == 0) {
          coreVal+1
        }
        else {
          coreVal
        }
        val g2 = g.subgraph(vpred = (vid,deg) => deg > coreVal).cache
        val newCores = cores.leftJoin(g.vertices.filter(x => x._2 <= coreVal).mapValues((vid,d) => coreVal)){ (vid,a,b) => b.getOrElse(a) }.cache
        core(g2,c,newCores)
      }
    }

    core(graph.mapVertices((vid,d) => 0L),0L,graph.vertices.mapValues(x => 0L))
  }

  def modularity[VD: ClassTag](graph: Graph[VD,(Double,Double)], communities: RDD[(VertexId,Long)], weighed: Boolean = false) : Double = {
    // Computes the modularity of a partition on a given graph.
    //println(graph.edges.collect.mkString(", "))
    val degrees = if(weighed) graph.mapReduceTriplets[(Double,Double)]( triplet => Iterator((triplet.srcId,(triplet.attr._1,triplet.attr._2)),(triplet.dstId,(triplet.attr._2,triplet.attr._1))) , (a,b) => (a._1+b._1,a._2+b._2) )
    else graph.mapReduceTriplets[(Double,Double)]( triplet => Iterator((triplet.srcId,(if (triplet.attr._1 > 0) 1. else 0.,if (triplet.attr._2 > 0) 1. else 0.)),(triplet.dstId,(if (triplet.attr._2 > 0) 1. else 0.,if (triplet.attr._1 > 0) 1. else 0.))) , (a,b) => (a._1+b._1,a._2+b._2) )
    //println(degrees.collect.mkString(", "))
    val m = degrees.map(_._2._1).reduce((a,b) => a+b)
    //println(degrees.innerJoin(communities){(vid,a,b) => (a._1,a._2,b)}.collect.mkString(", "))
    //val comGraph = Graph(degrees.innerJoin(communities){(vid,a,b) => (a._1,a._2,b)}, graph.edges)
    val comGraph = Graph(communities, graph.edges)

    //val q = comGraph.mapReduceTriplets[Double](triplet => Iterator(triplet.srcId,if(triplet.srcAttr._3==triplet.dstAttr._3) ((triplet.attr._1-(triplet.srcAttr._1*triplet.dstAttr._2)/m)/m) else 0. ,(triplet.dstId,if(triplet.srcAttr._3==triplet.dstAttr._3) ((triplet.attr._2-(triplet.srcAttr._2*triplet.dstAttr._1)/m)/m) else 0. )),(a,b) => a+b)
    val q_link = comGraph.mapReduceTriplets[Double](triplet => Iterator((triplet.srcId,if(triplet.srcAttr==triplet.dstAttr) triplet.attr._1/m else 0. ),(triplet.dstId,if(triplet.srcAttr==triplet.dstAttr) triplet.attr._2/m else 0. )),(a,b) => a+b)
    //println(m)
    //println((degrees.innerJoin(communities){(vid,a,b) => (b,a._1,a._2)}).map(a => (a._2._1,(a._2._2,a._2._3))).reduceByKey((a,b) => (a._1+b._1,a._2+b._2)).collect.mkString(", "))
    //println(q_link.collect.mkString(", "))
    val q_com = (degrees.innerJoin(communities){(vid,a,b) => (b,a._1,a._2)}).map(a => (a._2._1,(a._2._2,a._2._3))).reduceByKey((a,b) => (a._1+b._1,a._2+b._2)).map(a => (a._2._1/m)*(a._2._2/m)).reduce((a,b) => a+b)
    q_link.map(_._2).reduce((a,b) => a+b) - q_com
    //println(q.collect.mkString(", "))
    //q.map(_._2).reduce((a,b) => a+b)
    //0.

  }


  def weightedVertexEntropy[VD: ClassTag](graph: Graph[VD,(Double,Double)]) : RDD[(VertexId,Double)] = {
    // Compute the entropy of of the vertex, as the entropy of the strength of his connections

    // calculate strenghts to build a new graph, where each node has its strength as data.
    val strength = graph.mapReduceTriplets[Double](triplet => Iterator((triplet.srcId,triplet.attr._1),(triplet.dstId,triplet.attr._2)) , (a,b) => a+b)
    //println(streght.collect.mkString("\n"))
    val gStrength = Graph(strength,graph.edges)
    // Calculate the fraction (or prob) on each edge, for this, we build a new graph that contains this information.
    gStrength.mapReduceTriplets[Double](
      triplet => {
        Iterator((triplet.srcId,if(triplet.srcAttr>0 && triplet.attr._1>0){ math.abs(-1.*(triplet.attr._1/triplet.srcAttr)*math.log10(triplet.attr._1/triplet.srcAttr)) } else {0.}), (triplet.dstId, if (triplet.dstAttr>0 && triplet.attr._2>0){ math.abs(-1.*(triplet.attr._2/triplet.dstAttr)*math.log10(triplet.attr._2/triplet.dstAttr)) } else {0.}))
      },
      (a,b) => a+b
      )
    
  }


  def degreeEntropy[VD: ClassTag](graph: Graph[VD,(Double,Double)]) : RDD[(VertexId,Double)] = {
    
    graph.degrees.mapValues((vid,x) => math.log10(x)).reduceByKey((a,b) => a+b)
    
  }


  def vertexDiversity[VD: ClassTag](graph: Graph[VD,(Double,Double)]) : RDD[(VertexId,Double)] = {
    // calculates the diversity of the vertices in the network: if 0 the vertex is focusing all his weight on one neighbor, if 1 it is maximally diversified.
    // only makes sense for a weighted network
    weightedVertexEntropy(graph).join(degreeEntropy(graph)).mapValues(x => if (x._2>0) x._1/x._2 else 1.)
  }


  def clusteringCoefficient[VD: ClassTag](graph: Graph[VD,(Double,Double)]) : RDD[(VertexId,Double)] = {
    // calculate the clustering coefficient of the vertices of the graph.
    // the clustering coefficient is calculated as the ratio between the number of triangles the vertex makes with its neighbours, and the maximal amount he could make (d*(d-1)/2)
    graph.degrees.join(graph.triangleCount().vertices).mapValues(x => if (x._1>1) 2.*x._2/(x._1*(x._1-1.)) else 1.)

  }


  def overlap[VD: ClassTag,ED: ClassTag](graph: Graph[VD,ED]) : Graph[VD,Double] = {
    val deg = graph.degrees
    val neigh = graph.mapReduceTriplets[Set[VertexId]] (triplet => Iterator((triplet.srcId,Set(triplet.dstId)),(triplet.dstId,Set(triplet.srcId))),(a,b) => a++b)

    val neighbourGraph = Graph(deg.join(neigh),graph.edges)
    val overlapGraph = neighbourGraph.mapTriplets(triplet => triplet.srcAttr._2.intersect(triplet.dstAttr._2).size.toDouble/(triplet.srcAttr._1-1+triplet.dstAttr._1-1-triplet.srcAttr._2.intersect(triplet.dstAttr._2).size).toDouble)
    Graph(graph.vertices,overlapGraph.edges)
  }


  def main(args: Array[String] ) {
    val sc = connectToSparkCluster

    // the SparkContext is now available as sc
    val graph = buildDirGraph("resources/data/karate_club_nodes.txt","resources/data/karate_club.txt",sc)
    //println(graph.edges.collect.mkString("\n"))
    //val leaders = socialLeaders(graph)

    //println(leaders.collect.mkString("\n"))
    //println(edgeSignificance(graph).edges.collect.mkString("\n"))
    
    val t = now
    println(kCoreDecomposition(graph).collect.mkString("\n"))
    println(now-t)
    val t1 = now
    println("tailrec version")
    println(kCoreDecomposition4(graph).collect.mkString("\n"))
    println(now-t1)
    //val communities: RDD[(VertexId,Long)] = sc.textFile("E:/SparkPlayGround/karate_club_communities.txt").map(_.split(",")).map(x => (x(0).toLong,x(1).toLong))
    //println("Modularity = " + modularity(graph,communities))
    
    //println(graph.degrees.join(weightedVertexEntropy(graph).join(degreeEntropy(graph))).collect.mkString("\n"))
    //println(vertexDiversity(graph).collect.mkString("\n"))

    //println(clusteringCoefficient(graph).collect.mkString("\n"))

    //println(overlap(graph).edges.collect.mkString("\n"))


  }
}
