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

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueStateDescriptor, ValueState}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

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

  def main(args: Array[String]) {

    /**
      * Here, you can start creating your execution plan for Flink.
      *
      * Start with getting some data from the environment, like
      * env.readTextFile(textPath);
      *
      * then, transform the resulting DataSet[String] using operations
      * like:
      * .filter()
      * .flatMap()
      * .join()
      * .group()
      *
      * and many more.
      * Have a look at the programming guide:
      *
      * http://flink.apache.org/docs/latest/programming_guide.html
      *
      * and the examples
      *
      * http://flink.apache.org/docs/latest/examples.html
      *
      */

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val rides = env.addSource(
      new TaxiRideSource("nycTaxiRides.gz",60,600)
    )

    val avgSpeedRides = rides
      .filter(
        r => (GeoUtils.isInNYC(r.startLon,r.startLat)
              && GeoUtils.isInNYC(r.endLon,r.endLat))
      )
        .keyBy(r => r.rideId)
          .flatMap(new SpeedComputer)
    avgSpeedRides.print()

    env.execute("test")
  }

    class SpeedComputer extends RichFlatMapFunction[TaxiRide, (Long, Float)] {

      var state: ValueState[TaxiRide] = null

      override def open(config: Configuration): Unit = {
        state = getRuntimeContext.getState(new ValueStateDescriptor("ride", classOf[TaxiRide], null))
      }

      override def flatMap(ride: TaxiRide, out: Collector[(Long, Float)]): Unit = {

        if(state.value() == null) {
          // first ride
          state.update(ride)
        }
        else {
          // second ride
          val startEvent = if (ride.isStart) ride else state.value()
          val endEvent = if (ride.isStart) state.value() else ride

          val timeDiff = endEvent.time.getMillis - startEvent.time.getMillis
          val speed = if (timeDiff != 0) {
            (endEvent.travelDistance / timeDiff) * 60 * 60 * 1000
          } else {
            -1
          }
          // emit average speed
          out.collect( (startEvent.rideId, speed) )

          // clear state to free memory
          state.update(null)
        }
      }

  }



    // execute program
    //env.execute("Flink Scala API Skeleton")

}
