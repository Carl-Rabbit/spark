/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.recorder

import java.util.regex.Pattern

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.planning.GenericStrategy
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNode

object RecordLogger extends Logging {
  val PREFIX = "[RECORD]"

  private val RULE_INFO_PATTERN = Pattern
    .compile("org\\.apache\\.spark\\.sql\\.catalyst\\.([^.\\s]+)\\.([^\\s]+)")
  private val RULE_EXECUTION_INFO_PATTERN = Pattern
    .compile("org\\.apache\\.spark\\.sql\\.([^.\\s]+)\\.([^\\s]+)")
  private val STRATEGY_INFO_PATTERN = Pattern
    .compile("org\\.apache\\.spark\\.sql\\.execution\\.([^\\s]+)")

  def logJson(data: JValue): Unit = {
    val str = RecordLogger.PREFIX + " " + compact(render(data))
    logTrace(str)
  }

  def logRule(batchName: String, rule: Rule[_],
              oldPlan: TreeNode[_], newPlan: TreeNode[_],
              effective: Boolean, runTime: Long): Unit = {
    var data = ("batchName" -> batchName) ~
      ("runTime" -> runTime) ~
      ("effective" -> newPlan.toJsonValue) ~
      ("oldPlan" -> oldPlan.toJsonValue)
    if (effective) {
      data = data ~ ("newPlan" -> newPlan.toJsonValue)
    }
    extractRuleInfo(rule).iterator.foreach {
      case (k, v) => data = data ~ (k -> v)
    }
    logJson(data)
  }

  private def extractRuleInfo(rule: Rule[_]): Map[String, String] = {
    var map: Map[String, String] = Map()
    var m = RULE_INFO_PATTERN.matcher(rule.ruleName)
    if (!m.find) {
      m = RULE_EXECUTION_INFO_PATTERN.matcher(rule.ruleName)
    }
    if (m.find) {
      map += ("type" -> f"${m.group(1)} rule")
      map += ("ruleName" -> m.group(2))
    } else {
      map += ("type" -> "unknown rule")
      map += ("ruleName" -> "Unknown")
    }
    map += ("className" -> rule.ruleName)
    map
  }

  def logStrategy(strategy: GenericStrategy[_],
                  logicalPlan: TreeNode[_],
                  physicalPlans: Seq[TreeNode[_]],
                  effective: Boolean,
                  runTime: Long,
                  invokeCnt: Int,
                  recursionCnt: Int,
                  batchCnt: Int): Unit = {
    var data = ("effective" -> effective) ~
      ("runTime" -> runTime) ~
      ("logicalPlan" -> logicalPlan.toJsonValue) ~
      ("physicalPlans" -> physicalPlans.map(_.toJsonValue)) ~
      ("invokeCnt" -> invokeCnt) ~
      ("recursionCnt" -> recursionCnt) ~
      ("batchCnt" -> batchCnt)
    extractStrategyInfo(strategy).iterator.foreach {
      case (k, v) => data = data ~ (k -> v)
    }
    logJson(data)
  }

  private def extractStrategyInfo(strategy: GenericStrategy[_]): Map[String, String] = {
    val className = strategy.getClass.getName
    var map: Map[String, String] = Map()
    val m = STRATEGY_INFO_PATTERN.matcher(className)
    if (m.find) {
      map += ("type" -> f"${m.group(1)} strategy")
      map += ("strategyName" -> m.group(2))
    } else {
      map += ("type" -> "unknown strategy")
      map += ("strategyName" -> "Unknown")
    }
    map += ("className" -> className)
    map
  }
}
