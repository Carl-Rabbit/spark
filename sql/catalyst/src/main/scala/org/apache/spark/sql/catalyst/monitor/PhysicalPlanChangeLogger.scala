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


package org.apache.spark.sql.catalyst.monitor

import org.apache.spark.sql.catalyst.planning.GenericStrategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNode

class PhysicalPlanChangeLogger[PhysicalPlan <: TreeNode[PhysicalPlan]] {

  def logStrategy(strategy: GenericStrategy[PhysicalPlan],
                  logicalPlan: LogicalPlan,
                  physicalPlans: Seq[PhysicalPlan],
                  branchCnt: Int): Unit = {

    if (physicalPlans.isEmpty) {
      MonitorLogger.logMsg(s"${getStrategyName(strategy)} [branchCnt=$branchCnt] has no effect.")
      return
    }

    def message(): String = {
      val start =
        s"""
           |\n=== Applying Strategy ${getStrategyName(strategy)} [branchCnt=$branchCnt] ===
           |${logicalPlan.treeString}
           |***\n""".stripMargin
      physicalPlans.map(plan => plan.treeString).mkString(start, "\n***\n", "\n")
    }

    MonitorLogger.logMsg(message)
  }

  def getStrategyName(strategy: GenericStrategy[PhysicalPlan]): String = {
    val className = strategy.getClass.getName
    if (className endsWith "$") className.dropRight(1) else className
  }

  def logStrategyJson(strategy: GenericStrategy[PhysicalPlan],
                      logicalPlan: LogicalPlan,
                      physicalPlans: Seq[PhysicalPlan],
                      branchCnt: Int): Unit = {
    MonitorLogger.logMsg("JSON\n" + logicalPlan.toJSON)
    physicalPlans.foreach(plan => MonitorLogger.logMsg("JSON\n" + plan.toJSON))
  }
}
