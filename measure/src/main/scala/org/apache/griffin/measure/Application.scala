/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
package org.apache.griffin.measure

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import org.apache.griffin.measure.configuration.dqdefinition.{DQConfig, EnvConfig, GriffinConfig, Param}
import org.apache.griffin.measure.configuration.dqdefinition.reader.ParamReaderFactory
import org.apache.griffin.measure.configuration.enums._
import org.apache.griffin.measure.launch.DQApp
import org.apache.griffin.measure.launch.batch.BatchDQApp
import org.apache.griffin.measure.launch.streaming.StreamingDQApp


/**
  * application entrance
  * 数据指标计算的关键：关于Accuracy measuer:https://github.com/apache/griffin/blob/master/griffin-doc/measure/measures.md
  */
object Application extends Loggable {

  def main(args: Array[String]): Unit = {
    // info(args.toString)
    val args = new Array[String](2)
    args(0) = "{\n  \"spark\":{\n    \"log.level\":\"WARN\",\n    \"config\":{\n      \"spark" +
      ".master\":\"local[*]\"\n    }\n  },\n  \"sinks\":[\n    {\n      \"type\":\"CONSOLE\",\n  " +
      "    \"config\":{\n        \"max.log.lines\":10\n      }\n    },\n    {\n      " +
      "\"type\":\"HDFS\",\n      \"config\":{\n        " +
      "\"path\":\"hdfs://localhost:8020/griffin/batch/persist\",\n        \"max.persist" +
      ".lines\":10000,\n        \"max.lines.per.file\":10000\n      }\n    },\n    {\n      " +
      "\"type\":\"ELASTICSEARCH\",\n      \"config\":{\n        \"method\":\"post\",\n        " +
      "\"api\":\"http://192.168.239.171:9200/griffin/accuracy\",\n        \"connection" +
      ".timeout\":\"1m\",\n        \"retry\":10\n      }\n    }\n  ],\n  \"griffin" +
      ".checkpoint\":[\n\n  ]\n}";
    args(1) = "{\n  \"name\":\"accu_batch\",\n  \"process.type\":\"batch\",\n  \"data" +
      ".sources\":[\n    {\n      \"name\":\"source\",\n      \"baseline\":true,\n      " +
      "\"connectors\":[\n        {\n          \"type\":\"avro\",\n          \"version\":\"1.7\"," +
      "\n          \"config\":{\n            \"file.name\":\"measure/src/test/resources/users_info_src" +
      ".avro\"\n          }\n        }\n      ]\n    },\n    {\n      \"name\":\"target\",\n     " +
      " \"connectors\":[\n        {\n          \"type\":\"avro\",\n          \"version\":\"1.7\"," +
      "\n          \"config\":{\n            \"file.name\":\"measure/src/test/resources/users_info_target" +
      ".avro\"\n          }\n        }\n      ]\n    }\n  ],\n  \"evaluate.rule\":{\n    " +
      "\"rules\":[\n      {\n        \"dsl.type\":\"griffin-dsl\",\n        \"dq" +
      ".type\":\"accuracy\",\n        \"out.dataframe.name\":\"accu\",\n        \"rule\":\"source" +
      ".user_id = target.user_id AND upper(source.first_name) = upper(target.first_name) AND " +
      "source.last_name = target.last_name AND source.address = target.address AND source.email =" +
      " target.email AND source.phone = target.phone AND source.post_code = target.post_code\"\n " +
      "     }\n    ]\n  },\n  \"sinks\":[\n    \"CONSOLE\",\n    \"ELASTICSEARCH\"\n  ]\n}";
    if (args.length < 2) {
      error("Usage: class <env-param> <dq-param>")
      sys.exit(-1)
    }

    val envParamFile = args(0)
    val dqParamFile = args(1)

    info(envParamFile)
    info(dqParamFile)

    // read param files
    val envParam = readParamFile[EnvConfig](envParamFile) match {
      case Success(p) => p
      case Failure(ex) =>
        error(ex.getMessage, ex)
        sys.exit(-2)
    }
    val dqParam = readParamFile[DQConfig](dqParamFile) match {
      case Success(p) => p
      case Failure(ex) =>
        error(ex.getMessage, ex)
        sys.exit(-2)
    }
    val allParam: GriffinConfig = GriffinConfig(envParam, dqParam)

    // choose process
    val procType = ProcessType(allParam.getDqConfig.getProcType)
    val dqApp: DQApp = procType match {
      case BatchProcessType => BatchDQApp(allParam)
      case StreamingProcessType => StreamingDQApp(allParam)
      case _ =>
        error(s"${procType} is unsupported process type!")
        sys.exit(-4)
    }

    startup

    // 初始化griffin定时任务执行环境
    // 具体代码见下个代码块，主要逻辑是创建sparkSession和注册griffin自定义的spark udf
    // dq app init
    dqApp.init match {
      case Success(_) =>
        info("process init success")
      case Failure(ex) =>
        error(s"process init error: ${ex.getMessage}", ex)
        shutdown
        sys.exit(-5)
    }

    // dq app run
    val success = dqApp.run match {
      case Success(result) =>
        info("process run result: " + (if (result) "success" else "failed"))
        result

      case Failure(ex) =>
        error(s"process run error: ${ex.getMessage}", ex)

        if (dqApp.retryable) {
          throw ex
        } else {
          shutdown
          sys.exit(-5)
        }
    }

    // dq app end
    dqApp.close match {
      case Success(_) =>
        info("process end success")
      case Failure(ex) =>
        error(s"process end error: ${ex.getMessage}", ex)
        shutdown
        sys.exit(-5)
    }

    shutdown

    if (!success) {
      sys.exit(-5)
    }
  }

  private def readParamFile[T <: Param](file: String)(implicit m : ClassTag[T]): Try[T] = {
    val paramReader = ParamReaderFactory.getParamReader(file)
    paramReader.readConfig[T]
  }

  private def startup(): Unit = {
  }

  private def shutdown(): Unit = {
  }

}
