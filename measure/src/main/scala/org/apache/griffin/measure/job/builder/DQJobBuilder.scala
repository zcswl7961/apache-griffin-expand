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
package org.apache.griffin.measure.job.builder

import org.apache.griffin.measure.configuration.dqdefinition._
import org.apache.griffin.measure.context.DQContext
import org.apache.griffin.measure.job._
import org.apache.griffin.measure.step.builder.DQStepBuilder
import org.apache.griffin.measure.step.write.MetricFlushStep

/**
  * build dq job based on configuration
  */
object DQJobBuilder {

  /**
    * build dq job with rule param
    * @param context              dq context
    * @param evaluateRuleParam    evaluate rule param
    * @return       dq job
    */
  def buildDQJob(context: DQContext, evaluateRuleParam: EvaluateRuleParam): DQJob = {
    // 获取对一个dsl的配置信息
    val ruleParams = evaluateRuleParam.getRules
    buildDQJob(context, ruleParams)
  }

  /**
    * build dq job with rules in evaluate rule param or pre-proc param
    * @param context          dq context
    * @param ruleParams       rule params
    * @return       dq job
    */
  def buildDQJob(context: DQContext, ruleParams: Seq[RuleParam]): DQJob = {
    // build steps by datasources
    val dsSteps = context.dataSources.flatMap { dataSource =>
      DQStepBuilder.buildStepOptByDataSourceParam(context, dataSource.dsParam)
    }
    // build steps by rules
    /**
     * SeqDQStep(List(SparkSqlTransformStep(__missRecords,SELECT `source`.* FROM `source` LEFT JOIN `target` ON coalesce(`source`.`user_id`, '') = coalesce(`target`.`user_id`, '') AND upper(`source`.`first_name`) = upper(`target`.`first_name`) AND coalesce(`source`.`last_name`, '') = coalesce(`target`.`last_name`, '') AND coalesce(`source`.`address`, '') = coalesce(`target`.`address`, '') AND coalesce(`source`.`email`, '') = coalesce(`target`.`email`, '') AND coalesce(`source`.`phone`, '') = coalesce(`target`.`phone`, '') AND coalesce(`source`.`post_code`, '') = coalesce(`target`.`post_code`, '') WHERE (NOT (`source`.`user_id` IS NULL AND `source`.`first_name` IS NULL AND `source`.`last_name` IS NULL AND `source`.`address` IS NULL AND `source`.`email` IS NULL AND `source`.`phone` IS NULL AND `source`.`post_code` IS NULL)) AND (`target`.`user_id` IS NULL AND `target`.`first_name` IS NULL AND `target`.`last_name` IS NULL AND `target`.`address` IS NULL AND `target`.`email` IS NULL AND `target`.`phone` IS NULL AND `target`.`post_code` IS NULL),Map(),true), SparkSqlTransformStep(__missCount,SELECT COUNT(*) AS `miss` FROM `__missRecords`,Map(),false), SparkSqlTransformStep(__totalCount,SELECT COUNT(*) AS `total` FROM `source`,Map(),false), SparkSqlTransformStep(accu,
             SELECT A.total AS `total`,
                    A.miss AS `miss`,
                    (A.total - A.miss) AS `matched`,
                    coalesce( (A.total - A.miss) / A.total, 1.0) AS `matchedFraction`
             FROM (
               SELECT `__totalCount`.`total` AS total,
                      coalesce(`__missCount`.`miss`, 0) AS miss
               FROM `__totalCount` LEFT JOIN `__missCount`
             ) AS A
         ,Map(),false), MetricWriteStep(accu,accu,DefaultFlattenType,None), RecordWriteStep(__missRecords,__missRecords,None,None)))
     */
    val ruleSteps = ruleParams.flatMap { ruleParam =>
      DQStepBuilder.buildStepOptByRuleParam(context, ruleParam)
    }
    // metric flush step
    val metricFlushStep = MetricFlushStep()
    /**
     * ++ 用于连接两个集合
     * :+ 用于在集合尾部追加集合
     * +: 用于在集合头部追加集合
     */
    DQJob(dsSteps ++ ruleSteps :+ metricFlushStep)
  }

}
