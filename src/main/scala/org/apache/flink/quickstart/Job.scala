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

import org.apache.flink.api.scala._

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

//ready to work on graphReply

    // set up the execution environment
//    val env = ExecutionEnvironment.getExecutionEnvironment
//
//    val mails = env.readCsvFile[(String, String)](
//      "flinkMails.gz",
//      lineDelimiter = "##//##",
//      fieldDelimiter = "#|#",
//      includedFields = Array(1, 2))
//
//
//
//    val counts = mails.map(m => (m._1.substring(0,7), m._2, 1))
//      .groupBy(0,1).reduce((l,r) => (l._1,r._2,l._3 + r._3))
//      //.sum(2)
//
//    counts.print()

//    val env = ExecutionEnvironment.getExecutionEnvironment
//
//    val mails = env.readCsvFile[(String, String, String)](
//      "flinkMails.gz",
//      lineDelimiter = "##//##",
//      fieldDelimiter = "#|#",
//      includedFields = Array(0, 2, 5))
//
//    val cleanmail = mails
//      .map(m => (m._1, m._2.substring(0,m._2.lastIndexOf("<")), m._3))
//
//    val replies = cleanmail
//      .join(cleanmail).where(0).equalTo(2) {(l,r) => (l._2,r._2,1) }
//      .groupBy(0,1)
//      .sum(2)
//      .print()

//seems to be working

    val env = ExecutionEnvironment.getExecutionEnvironment

//    val mails = env.readCsvFile[(String, String)](
//      "flinkMails.gz",
//      lineDelimiter = "##//##",
//      fieldDelimiter = "#|#",
//      includedFields = Array(0, 4))

    val mails = env.fromElements(
      ("1","this is a mail and this is incredible"),
      ("2","this is an other mail and it is also incredible"),
      ("3","this mail is one more mail"),
      ("4","there are so many mails"))

    //computation of the mail number
    val nb = mails.count()

    val banlist = Set("is","a")

    //definition of a function to tokenize a mail
    def wordCount(m : (String, String)) : Traversable[(String, String, Int)] = {

      val wordlist = m._2.toLowerCase.split("\\W+")
        .map(w => (m._1, w, 1) )

      return wordlist.toTraversable
    }

    //get the list of every word with its mail source
    val words = mails.flatMap(m => wordCount(m) ).filter(w => !banlist.contains(w._2))

    //count the words in a same mail to get tf
    val tf =  words.groupBy(0,1)
      .sum(2)

    val idf = words.groupBy(0,1)
        //keep only one sample of a word per mail
        .reduce((l,r) => (l._1,l._2,1))
        //group by word
        .groupBy(1)
        //count the number of word per group and keep(word,nb of docs containing this word)
        .reduce((l,r) => ("any",l._2,l._3 + r._3))
        .map(w => (w._2, w._3.toDouble / nb))

    //join tf and idf on "word" and compute tf-idf
    val tfidf = tf.join(idf).where(1).equalTo(0) {(l,r) => (l._1,l._2,l._3, r._2, l._3.toDouble * r._2)}
      .print()

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

  }
    // execute program
    //env.execute("Flink Scala API Skeleton")

}
