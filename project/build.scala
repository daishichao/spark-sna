import sbt._
import Keys._
import scala.util.Properties
import xerial.sbt.Pack._

object MyBuild extends Build {

  lazy val hadoopVersion = Properties.envOrElse("SPARK_HADOOP_VERSION", "2.0.0-mr1-cdh4.4.0")

  lazy val root = Project(id = "root", base = file("."),
    settings = Project.defaultSettings
      ++ Seq(
        name := "GraphTest",
        version := "1.0",
        scalaVersion := "2.10.3",

        // Change default location of resources
        resourceDirectory in Compile := file("./resources"),
        // Change default location of unmanaged libraries
        unmanagedBase in Compile := file("./lib"),

        // Resolvers
        resolvers += "Akka Repository" at "http://repo.akka.io/releases",

        // External libraries
        libraryDependencies += "org.apache.spark" %% "spark-core" % "0.9.1",
        libraryDependencies += "org.apache.spark" %% "spark-graphx" % "0.9.1",
        libraryDependencies += "org.apache.hadoop" % "hadoop-client" % hadoopVersion
      )
      ++ packSettings
      ++ Seq(
        packMain := Map(
          "graphTest" -> "GraphTest"
        )
      )
    )

}
