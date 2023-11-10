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

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.MapOutputStatistics
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.{JoinHint, Statistics}
import org.apache.spark.sql.catalyst.trees.TreeNode

object RecordLogger extends Logging {

  val PREFIX = "[RECORD]"
  val PREFIX_TAMP = "[RECORD_TEMP]"

  private def logJson(data: JObject): Unit = {
    val data2 = data ~ ("timestamp" -> LocalDateTime.now().toString)
    val str = RecordLogger.PREFIX + " " + compact(render(data2))
    logTrace(str)
  }

  def logPlan(plan: TreeNode[_],
              name: String,
              procType: ProcType.Value,
              procFlag: ProcFlag.Value,
              isPartial: Boolean = false,
              labels: Seq[String] = Seq.empty): Unit = {
    val data = ("type" -> RecordType.plan.toString) ~
      ("plan" -> plan.toJsonValue) ~
      ("name" -> name) ~
      ("procType" -> procType.toString) ~
      ("procFlag" -> procFlag.toString) ~
      ("isPartial" -> isPartial) ~
      ("labels" -> JArray(labels.toList.map(JString(_))))
    logJson(data)
  }

  //  def logProcStatusForRule(rule: Rule[_], procFlag: ProcFlag.Value): Unit = {
//    val seq = extractRuleName(rule)
//    logProcStatus(seq(0), seq(1), ProcType.rule, procFlag)
//  }

//  private def extractRuleName(rule: Rule[_]): Seq[String] = {
//    var m = RULE_INFO_PATTERN.matcher(rule.ruleName)
//    if (m.find) {
//      return Seq(m.group(2), rule.ruleName)
//    }
//
//    m = RULE_EXECUTION_INFO_PATTERN.matcher(rule.ruleName)
//    if (m.find) {
//      return Seq(m.group(2), rule.ruleName)
//    }
//    Seq("Unknown", rule.ruleName)
//  }

  /* info record */

  def logInfoEffective(effective: Boolean): Unit = {
    val data = ("type" -> RecordType.info.toString) ~
      ("infoType" -> InfoRecordType.effective.toString) ~
      ("effective" -> effective)
    logJson(data)
  }

  def logInfoStrategy(recursiveId: Int, childIdSeq: Seq[Int]): Unit = {
    val data = ("type" -> RecordType.info.toString) ~
      ("infoType" -> InfoRecordType.strategy.toString) ~
      ("recursiveId" -> recursiveId) ~
      ("childIdSeq" -> childIdSeq)
    logJson(data)
  }

  def logInfoPruned(recursiveId: Int, selected: Int): Unit = {
    val data = ("type" -> RecordType.info.toString) ~
      ("infoType" -> InfoRecordType.pruned.toString) ~
      ("recursiveId" -> recursiveId) ~
      ("selected" -> selected)
    logJson(data)
  }

  def logInfoJoinSel(leftStats: Statistics,
                     rightStats: Statistics,
                     hint: JoinHint): Unit = {
    val data = ("type" -> RecordType.info.toString) ~
      ("infoType" -> InfoRecordType.joinSel.toString) ~
      ("leftStats" -> leftStats.toJsonValue) ~
      ("rightStats" -> rightStats.toJsonValue) ~
      ("hint" -> JString(hint.toString))
    logJson(data)
  }

  def logInfoDyJoinSel(mapStat: MapOutputStatistics, threshold: Long): Unit = {
    val data = ("type" -> RecordType.info.toString) ~
      ("infoType" -> InfoRecordType.dyJoinSel.toString) ~
      ("shuffleId" -> JInt(mapStat.shuffleId)) ~
      ("bytes" -> JArray(mapStat.bytesByPartitionId.toList.map(JLong(_)))) ~
      ("threshold" -> threshold)
    logJson(data)
  }

  def logInfoLabel(label: String): Unit = {
    val data = ("type" -> RecordType.info.toString) ~
      ("infoType" -> InfoRecordType.label.toString) ~
      ("label" -> label)
    logJson(data)
  }

  def logInfoStageSubmit(stages: Seq[TreeNode[_]]): Unit = {
    val data = ("type" -> RecordType.info.toString) ~
      ("infoType" -> InfoRecordType.stageSubmit.toString) ~
      ("stages" -> stages.map(_.toJsonValue))
    logJson(data)
  }

  def logInfoStageComplete(stages: Seq[TreeNode[_]]): Unit = {
    val data = ("type" -> RecordType.info.toString) ~
      ("infoType" -> InfoRecordType.stageComplete.toString) ~
      ("stages" -> stages.map(_.toJsonValue))
    logJson(data)
  }

  def logTemp(str: String): Unit = {
    val str2 = RecordLogger.PREFIX_TAMP + " " + str
    logTrace(str2)
  }
}
