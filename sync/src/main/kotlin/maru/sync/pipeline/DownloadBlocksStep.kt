/*
 * Copyright Consensys Software Inc.
 *
 * This file is dual-licensed under either the MIT license or Apache License 2.0.
 * See the LICENSE-MIT and LICENSE-APACHE files in the repository root for details.
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */
package maru.sync.pipeline

import java.util.concurrent.CompletableFuture
import java.util.function.Function
import maru.core.SealedBeaconBlock

class DownloadBlocksStep : Function<SyncTargetRange?, CompletableFuture<List<SealedBeaconBlock>>> {
  override fun apply(targetRange: SyncTargetRange?): CompletableFuture<List<SealedBeaconBlock>> {
    TODO("Not yet implemented")
  }
}
