/*
 * Copyright Consensys Software Inc.
 *
 * This file is dual-licensed under either the MIT license or Apache License 2.0.
 * See the LICENSE-MIT and LICENSE-APACHE files in the repository root for details.
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */
package maru.sync.pipeline

import maru.core.Validator
import maru.database.BeaconChain
import maru.p2p.PeerLookup
import org.hyperledger.besu.metrics.BesuMetricCategory
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem
import org.hyperledger.besu.services.pipeline.Pipeline
import org.hyperledger.besu.services.pipeline.PipelineBuilder

class BeaconChainDownloadPipelineFactory(
  private val beaconChain: BeaconChain,
  private val validators: Set<Validator>,
) {
  fun createPipeline(peerLookup: PeerLookup): Pipeline<SyncTargetRange?> {
    val downloaderParallelism = 1
    val metricsSystem = NoOpMetricsSystem()
    val startBlock = 0uL
    val targetBlock = beaconChain.getLatestBeaconState().latestBeaconBlockHeader.number
    val requestSize = 64u

    val syncTargetRangeSequence =
      sequence {
        var currentStart = startBlock
        while (currentStart <= targetBlock) {
          val currentEnd = minOf(currentStart + requestSize.toULong(), targetBlock)
          yield(SyncTargetRange(currentStart, currentEnd))
          currentStart = currentEnd + 1uL
        }
      }

    val downloadBlocksStep = DownloadBlocksStep(peerLookup)
    val importBlocksStep = ImportBlocksStep(beaconChain, validators.toList())

    return PipelineBuilder
      .createPipelineFrom(
        "blockNumbers",
        syncTargetRangeSequence.iterator(),
        downloaderParallelism,
        metricsSystem.createLabelledCounter(
          BesuMetricCategory.SYNCHRONIZER,
          "chain_download_pipeline_processed_total",
          "Number of entries process by each chain download pipeline stage",
          "step",
          "action",
        ),
        true,
        "importBlocks",
      ).thenProcessAsyncOrdered("downloadBlocks", downloadBlocksStep, downloaderParallelism)
      .andFinishWith("importBlocks", importBlocksStep)
  }
}
