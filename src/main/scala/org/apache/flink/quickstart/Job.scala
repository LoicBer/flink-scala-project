package org.apache.flink.quickstart

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



import org.apache.flink.api.common.functions._
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.examples.java.clustering.util.KMeansData

import scala.collection.JavaConverters._

/**
 * Skeleton for a Flink Job.
 *
 * For a full example of a Flink Job, see the WordCountJob.scala file in the
 * same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster. Just type
 * {{{
 *   mvn clean package
 * }}}
 * in the projects root directory. You will find the jar in
 * target/flink-quickstart-0.1-SNAPSHOT-Sample.jar
 *
 */
object Job {

  trait Coordinates extends Serializable {
    var x: Double
    var y: Double

    def add(other : Coordinates): this.type  = {
      x += other.x
      y += other.y
      this
    }

    def div(other : Long): this.type = {
      x = x/other
      y = y/other
      this
    }

    def dist(other : Coordinates): Double = {
      val d = Math.sqrt( (x - other.x)*(x - other.x) + (y - other.y)*(y - other.y))
      d
    }

    def clear() : Unit = {
      x = 0
      y = 0
    }

    override def toString: String = {
      s"$x $y"

    }

  }

  case class Point(var x: Double = 0, var y: Double = 0) extends Coordinates

  case class Centroid(var id : Int = 0, var x: Double = 0, var y: Double = 0) extends Coordinates {
    def this(id: Int, p: Point) {
      this(id, p.x, p.y)
    }

    override def toString : String = {
      s"$id ${super.toString}"
    }
  }

  def getCentroidDataSet(params: ParameterTool, env: ExecutionEnvironment) : DataSet[Centroid] = {
    env.readCsvFile[Centroid](
      params.get("centroids"),
      fieldDelimiter = " ",
      includedFields = Array(0,1,2))
  }

  def getPointDataSet(params: ParameterTool, env: ExecutionEnvironment) : DataSet[Point] = {
    env.readCsvFile[Point](
      params.get("points"),
      fieldDelimiter = " ",
      includedFields = Array(0,1)
    )
  }


  class nearestCentroids extends RichMapFunction[Point,(Int,Point,Long)] {

    private var centroids: Traversable[Centroid] = null

    override def open(config: Configuration) {
      centroids = getRuntimeContext.getBroadcastVariable[Centroid]("centroids").asScala
    }

    def map(p : Point): (Int,Point,Long) = {
      var nc = -1
      var sd = Double.MaxValue
      centroids.foreach(c =>
        if (p.dist(c) < sd) {
          sd = p.dist(c)
          nc = c.id
        }
      )
      (nc,p,1L)
    }
  }


  def main(args: Array[String]) {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val params = ParameterTool.fromArgs(args)

    val centroids = getCentroidDataSet(params, env)
    val points = getPointDataSet(params, env)

    val finalcentroids = centroids.iterate(100) {
      currentcentroids => val newcentroids = points
          .map(new nearestCentroids).withBroadcastSet(currentcentroids,"centroids")
          .groupBy(0)
          .reduce( (l,r) => (l._1, l._2.add(r._2), l._3 + r._3))
          .map( p => new Centroid(p._1,p._2.div(p._3)))
        newcentroids
    }

    finalcentroids.print()


  }


    // execute program
    //env.execute("Flink Scala API Skeleton")

}
