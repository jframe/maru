/*
 * Copyright Consensys Software Inc.
 *
 * This file is dual-licensed under either the MIT license or Apache License 2.0.
 * See the LICENSE-MIT and LICENSE-APACHE files in the repository root for details.
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */
package maru.sync.pipeline

import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem
import org.hyperledger.besu.services.pipeline.Pipeline
import org.hyperledger.besu.services.pipeline.PipelineBuilder

class BeaconChainDownloadPipelineFactory {
  /**
   * Creates a pipeline for downloading blocks from the beacon chain.
   *
   * @return A [Pipeline] that processes [SyncTargetRange] objects.
   */
  fun createPipeline(): Pipeline<SyncTargetRange?> {
    val downloaderParallelism = 1
    val metricsSystem = NoOpMetricsSystem()
    val validatorSyncSource = ValidatorSyncSource(startBlock = 0uL, targetBlock = 0uL, requestSize = 64u)
    val downloadBlocksStep = DownloadBlocksStep()
    val importBlocksStep = ImportBlocksStep()

    return PipelineBuilder
      .createPipelineFrom(
        "blockNumbers",
        validatorSyncSource,
        downloaderParallelism,
        metricsSystem.createLabelledCounter(
          org.hyperledger.besu.metrics.BesuMetricCategory.SYNCHRONIZER,
          "chain_download_pipeline_processed_total",
          "Number of entries process by each chain download pipeline stage",
          "step",
          "action",
        ),
        true,
        "importBlocks",
      ).thenProcessAsync("downloadBlocks", downloadBlocksStep, downloaderParallelism)
      .andFinishWith("importBlocks", importBlocksStep)
  }
}
