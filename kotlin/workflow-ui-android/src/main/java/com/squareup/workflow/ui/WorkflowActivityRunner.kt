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
package com.squareup.workflow.ui

import android.app.Activity
import android.arch.lifecycle.ViewModelProviders
import android.os.Bundle
import android.support.annotation.CheckResult
import android.support.v4.app.FragmentActivity
import com.squareup.workflow.Workflow
import io.reactivex.BackpressureStrategy.LATEST
import io.reactivex.Flowable
import io.reactivex.Observable

/**
 * Packages a [Workflow] and a [ViewRegistry] to drive an [Activity][FragmentActivity].
 *
 * You'll never instantiate one of these yourself. Instead, use
 * [FragmentActivity.setContentWorkflow]. See that method for more details.
 */
@ExperimentalWorkflowUi
class WorkflowActivityRunner<out OutputT : Any>
internal constructor(
  private val model: WorkflowRunnerViewModel<OutputT>
) : WorkflowRunner<OutputT> by model {

  /**
   * To save and restore the progress of your workflow via the activity's persistence [Bundle],
   * call this from [FragmentActivity.onSaveInstanceState].
   *
   * @see [com.squareup.workflow.StatefulWorkflow.snapshotState]
   */
  fun onSaveInstanceState(outState: Bundle) {
    model.onSaveInstanceState(outState)
  }

  /**
   * If your workflow needs to manage the back button, override [FragmentActivity.onBackPressed]
   * and call this method, and have your views or coordinators use [HandlesBack].
   *
   * e.g.:
   *
   *    override fun onBackPressed() {
   *      if (!workflowViewModel.onBackPressed(this)) super.onBackPressed()
   *    }
   */
  fun onBackPressed(activity: Activity): Boolean {
    return HandlesBack.Helper.onBackPressed(activity.findViewById(R.id.workflow_layout))
  }
}

/**
 * Call this method from [FragmentActivity.onCreate], instead of [FragmentActivity.setContentView].
 * It creates a [WorkflowActivityRunner] for this activity, if one doesn't already exist, and
 * sets a view driven by that model as the content view.
 *
 * Hold onto the [WorkflowActivityRunner] returned and:
 *
 *  - Call [WorkflowActivityRunner.onBackPressed] from [FragmentActivity.onBackPressed] to allow
 *    workflows to handle back button events. (See [HandlesBack] for more details.)
 *
 *  - Call [WorkflowActivityRunner.onSaveInstanceState] from [FragmentActivity.onSaveInstanceState].
 *
 *  e.g.:
 *
 *     class MainActivity : AppCompatActivity() {
 *       private lateinit var runner: WorkflowRunner<*, *>
 *
 *       override fun onCreate(savedInstanceState: Bundle?) {
 *         super.onCreate(savedInstanceState)
 *         runner = setContentWorkflow(MyViewRegistry, MyRootWorkflow(), savedInstanceState)
 *       }
 *
 *       override fun onBackPressed() {
 *         if (!runner.onBackPressed(this)) super.onBackPressed()
 *       }
 *
 *       override fun onSaveInstanceState(outState: Bundle) {
 *         super.onSaveInstanceState(outState)
 *         runner.onSaveInstanceState(outState)
 *       }
 *     }
 */
@ExperimentalWorkflowUi
@CheckResult
fun <InputT, OutputT : Any> FragmentActivity.setContentWorkflow(
  viewRegistry: ViewRegistry,
  workflow: Workflow<InputT, OutputT, Any>,
  inputs: Flowable<InputT>,
  savedInstanceState: Bundle?
): WorkflowActivityRunner<OutputT> {
  val factory = WorkflowRunnerViewModel.Factory(workflow, viewRegistry, inputs, savedInstanceState)

  // We use an Android lifecycle ViewModel to shield ourselves from configuration changes.
  // ViewModelProviders.of() uses the factory to instantiate a new instance only
  // on the first call for this activity, and it stores that instance for repeated use
  // until this activity is finished.

  @Suppress("UNCHECKED_CAST")
  val viewModel = ViewModelProviders.of(this, factory)[WorkflowRunnerViewModel::class.java]
      as WorkflowRunnerViewModel<OutputT>
  val runner = WorkflowActivityRunner(viewModel)

  val layout = WorkflowLayout(this@setContentWorkflow)
      .apply {
        id = R.id.workflow_layout
        setWorkflowRunner(runner)
      }

  this.setContentView(layout)

  return runner
}

/**
 * Convenience overload of [setContentWorkflow] for workflows unconcerned with back-pressure
 * of their inputs.
 */
@ExperimentalWorkflowUi
@CheckResult
fun <InputT, OutputT : Any, RenderingT : Any> FragmentActivity.setContentWorkflow(
  viewRegistry: ViewRegistry,
  workflow: Workflow<InputT, OutputT, RenderingT>,
  inputs: Observable<InputT>,
  savedInstanceState: Bundle?
): WorkflowActivityRunner<OutputT> {
  return setContentWorkflow(viewRegistry, workflow, inputs.toFlowable(LATEST), savedInstanceState)
}

/**
 * Convenience overload of [setContentWorkflow] for workflows that take one input value
 * rather than a stream.
 */
@ExperimentalWorkflowUi
@CheckResult
fun <InputT, OutputT : Any, RenderingT : Any> FragmentActivity.setContentWorkflow(
  viewRegistry: ViewRegistry,
  workflow: Workflow<InputT, OutputT, RenderingT>,
  input: InputT,
  savedInstanceState: Bundle?
): WorkflowActivityRunner<OutputT> {
  return setContentWorkflow(viewRegistry, workflow, Observable.just(input), savedInstanceState)
}

/**
 * Convenience overload of [setContentWorkflow] for workflows that take no input.
 */
@ExperimentalWorkflowUi
@CheckResult
fun <OutputT : Any, RenderingT : Any> FragmentActivity.setContentWorkflow(
  viewRegistry: ViewRegistry,
  workflow: Workflow<Unit, OutputT, RenderingT>,
  savedInstanceState: Bundle?
): WorkflowActivityRunner<OutputT> {
  return setContentWorkflow(viewRegistry, workflow, Unit, savedInstanceState)
}
