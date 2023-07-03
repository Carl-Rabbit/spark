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

import java.time.LocalDateTime
import java.util.regex.Pattern

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.MapOutputStatistics
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.planning.GenericStrategy
import org.apache.spark.sql.catalyst.plans.logical.{JoinHint, Statistics}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNode

object RecordLogger extends Logging {

  val PREFIX = "[RECORD]"
  val PREFIX_TAMP = "[RECORD_TEMP]"

  // phases
  val ANALYSIS = "analysis"
  val OPTIMIZATION = "optimization"
  val PLANNING = "planning"
  val AQE = "aqe"

  // phase flag
  val PHASE_START = "start"
  val PHASE_END = "end"

  // rTypes (record type)
  val ACTION = "action"
  val INFO = "info"

  // assign flag
  val BEFORE = "before"
  val AFTER = "after"
  val INDIVIDUAL = "ind"

  // known types
  val TYPE_ACTION = "action"
  val TYPE_JOIN_SEL = "join sel"
  val TYPE_DY_JOIN_SEL = "dy join sel"
  val TYPE_PHASE = "phase"
  val TYPE_PRUNED = "pruned"
  val TYPE_AQE_START = "aqe start"
  val TYPE_AQE_END = "aqe end"
  val TYPE_LABEL = "label"
  val TYPE_STAGE = "stage"

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

  def logAction(batchName: String, batchId: String, name: String,
                oldPlan: TreeNode[_], newPlan: TreeNode[_]): Unit = {
    val data = ("rType" -> ACTION) ~
      ("type" -> TYPE_ACTION) ~
      ("name" -> name) ~
      ("batchName" -> batchName) ~
      ("batchId" -> batchId) ~
      ("effective" -> true) ~
      ("oldPlan" -> oldPlan.toJsonValue) ~
      ("newPlan" -> newPlan.toJsonValue) ~
      ("timestamp" -> LocalDateTime.now().toString)
    logJson(data)
  }

  def logRule(batchName: String, batchId: String, rule: Rule[_],
              oldPlan: TreeNode[_], newPlan: TreeNode[_],
              effective: Boolean, runTime: Long): Unit = {
    var data = ("rType" -> ACTION) ~
      ("batchName" -> batchName) ~
      ("batchId" -> batchId) ~
      ("runTime" -> runTime) ~
      ("effective" -> effective) ~
      ("oldPlan" -> oldPlan.toJsonValue) ~
      ("timestamp" -> LocalDateTime.now().toString)
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
    if (m.find) {
      map += ("type" -> f"${m.group(1)} rule")
      map += ("ruleName" -> m.group(2))
      map += ("className" -> rule.ruleName)
      return map
    }

    m = RULE_EXECUTION_INFO_PATTERN.matcher(rule.ruleName)
    if (m.find) {
      map += ("type" -> f"${m.group(1)} rule")
      map += ("ruleName" -> m.group(2))
      map += ("className" -> rule.ruleName)
      return map
    }

    map += ("type" -> "unknown rule")
    map += ("ruleName" -> "Unknown")
    map += ("className" -> rule.ruleName)
    map
  }

  def logStrategy(strategy: GenericStrategy[_],
                  logicalPlan: TreeNode[_],
                  physicalPlans: Seq[TreeNode[_]],
                  effective: Boolean,
                  runTime: Long,
                  invokeCnt: Int,
                  rid: Int,
                  childRidSeq: Seq[Int]): Unit = {
    var data = ("rType" -> ACTION) ~
      ("effective" -> effective) ~
      ("runTime" -> runTime) ~
      ("logicalPlan" -> logicalPlan.toJsonValue) ~
      ("physicalPlans" -> physicalPlans.map(_.toJsonValue)) ~
      ("invokeCnt" -> invokeCnt) ~
      ("rid" -> rid) ~
      ("childRidSeq" -> childRidSeq) ~
      ("timestamp" -> LocalDateTime.now().toString)
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
      map += ("type" -> f"strategy")
      map += ("strategyName" -> m.group(1))
    } else {
      map += ("type" -> "strategy")
      map += ("strategyName" -> "Unknown")
    }
    map += ("className" -> className)
    map
  }

  def logInfoStrategyPruned(invokeCnt: Int, rid: Int, selectedRid: Int): Unit = {
    val data = ("rType" -> INFO) ~
      ("type" -> TYPE_PRUNED) ~
      ("assign" -> BEFORE) ~
      ("invokeCnt" -> invokeCnt) ~
      ("rid" -> rid) ~
      ("selectedRid" -> selectedRid) ~
      ("timestamp" -> LocalDateTime.now().toString)
    logJson(data)
  }

  def logInfoJoinSelection(plan: TreeNode[_],
                           leftStats: Statistics, rightStats: Statistics,
                           hint: JoinHint): Unit = {
    val data = ("rType" -> INFO) ~
      ("type" -> TYPE_JOIN_SEL) ~
      ("assign" -> AFTER) ~
      ("plan" -> plan.toJsonValue) ~
      ("leftStats" -> leftStats.toJsonValue) ~
      ("rightStats" -> rightStats.toJsonValue) ~
      ("hint" -> JString(hint.toString)) ~
      ("timestamp" -> LocalDateTime.now().toString)
    logJson(data)
  }

  def logInfoDynamicJoinSelection(plan: TreeNode[_],
                                  mapStat: MapOutputStatistics, threshold: Long): Unit = {
    val data = ("rType" -> INFO) ~
      ("type" -> TYPE_DY_JOIN_SEL) ~
      ("assign" -> AFTER) ~
      ("plan" -> plan.toJsonValue) ~
      ("shuffleId" -> JInt(mapStat.shuffleId)) ~
      ("bytes" -> JArray(mapStat.bytesByPartitionId.toList.map(JLong(_)))) ~
      ("threshold" -> threshold) ~
      ("timestamp" -> LocalDateTime.now().toString)
    logJson(data)
  }

  def logInfoPhase(phaseFlag: String, phaseName: String): Unit = {
    val data = ("rType" -> INFO) ~
      ("type" -> TYPE_PHASE) ~
      ("assign" -> (if (phaseFlag == PHASE_START) AFTER else BEFORE)) ~
      ("phaseName" -> phaseName) ~
      ("phaseFlag" -> phaseFlag) ~
      ("timestamp" -> LocalDateTime.now().toString)
    logJson(data)
  }

  def logAQEStart(logicalPlan: TreeNode[_], physicalPlan: TreeNode[_]): Unit = {
    val data = ("rType" -> INFO) ~
      ("type" -> TYPE_AQE_START) ~
      ("assign" -> AFTER) ~
      ("logicalPlan" -> logicalPlan.toJsonValue) ~
      ("physicalPlan" -> physicalPlan.toJsonValue) ~
      ("timestamp" -> LocalDateTime.now().toString)
    logJson(data)
  }

  def logFinalPlan(logicalPlan: TreeNode[_], physicalPlan: TreeNode[_]): Unit = {
    val data = ("rType" -> INFO) ~
      ("type" -> TYPE_AQE_END) ~
      ("assign" -> BEFORE) ~
      ("logicalPlan" -> logicalPlan.toJsonValue) ~
      ("physicalPlan" -> physicalPlan.toJsonValue) ~
      ("timestamp" -> LocalDateTime.now().toString)
    logJson(data)
  }

  def logInfoLabel(label: String, assign: String): Unit = {
    val data = ("rType" -> INFO) ~
      ("type" -> TYPE_LABEL) ~
      ("assign" -> assign) ~
      ("label" -> label) ~
      ("timestamp" -> LocalDateTime.now().toString)
    logJson(data)
  }

  def logInfoNewStages(stages: Seq[TreeNode[_]]): Unit = {
    val data = ("rType" -> INFO) ~
      ("type" -> TYPE_STAGE) ~
      ("assign" -> BEFORE) ~
      ("stages" -> stages.map(_.toJsonValue)) ~
      ("timestamp" -> LocalDateTime.now().toString)
    logJson(data)
  }

  def logTemp(str: String): Unit = {
    val str2 = RecordLogger.PREFIX_TAMP + " " + str
    logTrace(str2)
  }
}
