/*
 * Copyright 2019 Square Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.squareup.workflow

import com.squareup.workflow.WorkflowAction.Companion.emitOutput
import com.squareup.workflow.WorkflowAction.Companion.enterState
import com.squareup.workflow.testing.testFromStart
import kotlinx.coroutines.channels.Channel
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertTrue

class WorkflowCompositionIntegrationTest {

  @Test fun `composes parent with single child`() {
    val root = TreeWorkflow("root", TreeWorkflow("leaf"))

    // Setup initial state and change the state the workflow in the tree.
    root.testFromStart("initial input") {
      awaitNextRendering()
          .let {
            assertEquals("root:initial input", it.data)
            assertEquals("leaf:initial input[0]", it["leaf"].data)
          }
    }
  }

  @Test fun `composes parent with multiple children`() {
    val root = TreeWorkflow(
        "root",
        TreeWorkflow("leaf1"),
        TreeWorkflow("leaf2")
    )

    // Setup initial state and change the state the workflow in the tree.
    root.testFromStart("initial input") {
      awaitNextRendering()
          .let {
            assertEquals("root:initial input", it.data)
            assertEquals("leaf1:initial input[0]", it["leaf1"].data)
            assertEquals("leaf2:initial input[1]", it["leaf2"].data)
          }
    }
  }

  @Test fun `composes complex tree`() {
    val root = TreeWorkflow(
        "root",
        TreeWorkflow(
            "middle1",
            TreeWorkflow("leaf1"),
            TreeWorkflow("leaf2")
        ),
        TreeWorkflow(
            "middle2",
            TreeWorkflow("leaf3")
        )
    )

    root.testFromStart("initial input") {
      awaitNextRendering()
          .let {
            assertEquals("root:initial input", it.data)
            assertEquals("middle1:initial input[0]", it["middle1"].data)
            assertEquals("middle2:initial input[1]", it["middle2"].data)
            assertEquals("leaf1:initial input[0][0]", it["middle1", "leaf1"].data)
            assertEquals("leaf2:initial input[0][1]", it["middle1", "leaf2"].data)
            assertEquals("leaf3:initial input[1][0]", it["middle2", "leaf3"].data)
          }
    }
  }

  @Test fun `render fails when duplicate child key`() {
    val root = TreeWorkflow(
        "root",
        TreeWorkflow("leaf"),
        TreeWorkflow("leaf")
    )

    // Setup initial state and change the state the workflow in the tree.
    assertFails {
      root.testFromStart("initial input") {
        awaitNextRendering()
      }
    }.let { error ->
      val causeChain = generateSequence(error) { it.cause }
      assertTrue(causeChain.any { it is IllegalArgumentException })
    }
  }

  // See https://github.com/square/workflow/issues/261.
  @Test fun `renderChild closes over latest state`() {
    val triggerChildOutput = Channel<Unit>()
    val child = Workflow.stateless<Unit, Unit, Unit> {
      onWorkerOutput(triggerChildOutput.asWorker()) { emitOutput(Unit) }
    }
    val workflow = object : StatefulWorkflow<Unit, Int, Int, (Unit) -> Unit>() {
      override fun initialState(
        input: Unit,
        snapshot: Snapshot?
      ) = 0

      override fun render(
        input: Unit,
        state: Int,
        context: RenderContext<Int, Int>
      ): (Unit) -> Unit {
        context.renderChild(child) { emitOutput(state) }
        return context.onEvent { enterState(state + 1) }
      }

      override fun snapshotState(state: Int) = Snapshot.EMPTY
    }

    workflow.testFromStart {
      triggerChildOutput.offer(Unit)
      assertEquals(0, awaitNextOutput())

      awaitNextRendering()
          .invoke(Unit)
      triggerChildOutput.offer(Unit)

      assertEquals(1, awaitNextOutput())

      awaitNextRendering()
          .invoke(Unit)
      triggerChildOutput.offer(Unit)

      assertEquals(2, awaitNextOutput())
    }
  }
}
