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

package Communities

object LouvainMethod{
	def run[VD: ClassTag](graph: Graph[VD,(Double,Double)]) : VertexRDD[Long] ={


	}
}