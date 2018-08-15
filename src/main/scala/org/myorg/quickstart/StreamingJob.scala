/*
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

package org.myorg.quickstart

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala._

/**
  * Skeleton for a Rescaling Stateful Applications.md Streaming Job.
  *
  * For a tutorial how to write a Rescaling Stateful Applications.md streaming application, check the
  * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Rescaling Stateful Applications.md Website</a>.
  *
  * To package your appliation into a JAR file for execution, run
  * 'mvn clean package' on the command line.
  *
  * If you change the name of the main class (with the public static void main(String[] args))
  * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
  */
object StreamingJob {
    def main(args: Array[String]) {
        // set up the streaming execution environment
        val env = ExecutionEnvironment.getExecutionEnvironment

        env.readTextFile("E:\\idea\\quickstart\\src\\main\\resources\\log4j.properties")
            .flatMap(_.toLowerCase.split("\\w+") filter {
                _.nonEmpty
            })
            .map((_, 1))
            .groupBy(0)
            .sum(1)
            .writeAsText("E:\\yyy.csv", WriteMode.NO_OVERWRITE)
            // 设置保存统计结果为单个线程，而不是并行的，这样结果就不会被保存在多个文件中了
            .setParallelism(1)

        env.execute("word_count")
    }
}
