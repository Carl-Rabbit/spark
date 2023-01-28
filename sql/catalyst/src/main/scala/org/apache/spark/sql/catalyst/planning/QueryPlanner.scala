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

package org.apache.spark.sql.catalyst.planning

import scala.collection.mutable.ListBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.recorder.RecordLogger
import org.apache.spark.sql.catalyst.trees.TreeNode

/**
 * Given a [[LogicalPlan]], returns a list of `PhysicalPlan`s that can
 * be used for execution. If this strategy does not apply to the given logical operation then an
 * empty list should be returned.
 */
abstract class GenericStrategy[PhysicalPlan <: TreeNode[PhysicalPlan]] extends Logging {

  /**
   * Returns a placeholder for a physical plan that executes `plan`. This placeholder will be
   * filled in automatically by the QueryPlanner using the other execution strategies that are
   * available.
   */
  protected def planLater(plan: LogicalPlan): PhysicalPlan

  def apply(plan: LogicalPlan): Seq[PhysicalPlan]
}

/**
 * Abstract class for transforming [[LogicalPlan]]s into physical plans.
 * Child classes are responsible for specifying a list of [[GenericStrategy]] objects that
 * each of which can return a list of possible physical plan options.
 * If a given strategy is unable to plan all of the remaining operators in the tree,
 * it can call [[GenericStrategy#planLater planLater]], which returns a placeholder
 * object that will be [[collectPlaceholders collected]] and filled in
 * using other available strategies.
 *
 * TODO: RIGHT NOW ONLY ONE PLAN IS RETURNED EVER...
 *       PLAN SPACE EXPLORATION WILL BE IMPLEMENTED LATER.
 *
 * @tparam PhysicalPlan The type of physical plan produced by this [[QueryPlanner]]
 */
abstract class QueryPlanner[PhysicalPlan <: TreeNode[PhysicalPlan]] {
  private var invokeCnt = 0
  private var globalRid = 0
  private val ridStack = ListBuffer[Int](0)

  def initGlobalRid(): Unit = {
    globalRid = 1
  }
  def getNewRid: Int = {
    val newRid = globalRid
    globalRid += 1
    newRid
  }

  /** A list of execution strategies that can be used by the planner */
  def strategies: Seq[GenericStrategy[PhysicalPlan]]

  def plan(plan: LogicalPlan): Iterator[PhysicalPlan] = {
    // Obviously a lot to do here still...

    if (ridStack.size == 1) {
      invokeCnt += 1
      initGlobalRid()
    }
    val curRid = ridStack.head

    // Collect physical plan candidates.
    var childRidSeq = Seq[Int]()
    val candidates = strategies.iterator.flatMap(strategy => {
      val startTime = System.nanoTime()

      val newPlans = strategy(plan)

      val runTime = System.nanoTime() - startTime
      val effective = newPlans.nonEmpty
      val tempChildRidSeq = newPlans.indices.map(_ => getNewRid)
      RecordLogger.logStrategy(strategy, plan, newPlans,
        effective, runTime,
        invokeCnt, curRid, tempChildRidSeq)
      childRidSeq ++= tempChildRidSeq

      newPlans
    })

    val candidateSeq = candidates.toSeq

    // The candidates may contain placeholders marked as [[planLater]],
    // so try to replace them by their child plans.
    var candidateIdx = -1
    val plans = candidateSeq.iterator.flatMap { candidate =>
      candidateIdx += 1
      val placeholders = collectPlaceholders(candidate)
      if (placeholders.isEmpty) {
        // Take the candidate as is because it does not contain placeholders.
        Iterator(candidate)
      } else {
        ridStack.prepend(childRidSeq(candidateIdx))
        // Plan the logical plan marked as [[planLater]] and replace the placeholders.
        val retPlans = placeholders.iterator.foldLeft(Iterator(candidate)) {
          case (candidatesWithPlaceholders, (placeholder, logicalPlan)) =>
            // Plan the logical plan for the placeholder.
            val childPlans = this.plan(logicalPlan)

            candidatesWithPlaceholders.flatMap { candidateWithPlaceholders =>
              childPlans.map { childPlan =>
                // Replace the placeholder by the child plan
                candidateWithPlaceholders.transformUp {
                  case p if p.eq(placeholder) => childPlan
                }
              }
            }
        }
        ridStack.remove(0)
        retPlans
      }
    }

    val pruned = prunePlans(plans)
    assert(pruned.hasNext, s"No plan for $plan")

    RecordLogger.logStrategyPruned(invokeCnt, curRid, childRidSeq.head)

    pruned
  }

  /**
   * Collects placeholders marked using [[GenericStrategy#planLater planLater]]
   * by [[strategies]].
   */
  protected def collectPlaceholders(plan: PhysicalPlan): Seq[(PhysicalPlan, LogicalPlan)]

  /** Prunes bad plans to prevent combinatorial explosion. */
  protected def prunePlans(plans: Iterator[PhysicalPlan]): Iterator[PhysicalPlan]
}
