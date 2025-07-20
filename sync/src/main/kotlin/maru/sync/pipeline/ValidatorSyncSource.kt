/*
 * Copyright Consensys Software Inc.
 *
 * This file is dual-licensed under either the MIT license or Apache License 2.0.
 * See the LICENSE-MIT and LICENSE-APACHE files in the repository root for details.
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */
package maru.sync.pipeline

import kotlin.math.min

class ValidatorSyncSource(
  private val startBlock: ULong,
  private val targetBlock: ULong,
  private val requestSize: UInt,
) : Iterator<SyncTargetRange> {
  private var lastRange: SyncTargetRange? = null

  override fun hasNext(): Boolean = !hasReachedTarget

  override fun next(): SyncTargetRange =
    when {
      hasReachedTarget -> throw NoSuchElementException("No more ranges to sync. Current last range: $lastRange")
      lastRange == null -> createFirstRange().also { lastRange = it }
      else -> createNextRange(lastRange!!).also { lastRange = it }
    }

  private fun createFirstRange() = SyncTargetRange(startBlock, endBlock(startBlock))

  private fun createNextRange(lastRange: SyncTargetRange) =
    SyncTargetRange(
      lastRange.endBlock + 1uL,
      endBlock(
        lastRange.endBlock + 1uL,
      ),
    )

  private val hasReachedTarget: Boolean
    get() =
      when {
        startBlock == targetBlock -> true
        lastRange?.endBlock == null -> false
        else -> lastRange!!.endBlock >= targetBlock
      }

  private fun endBlock(startBlock: ULong) = min(startBlock + requestSize, targetBlock)
}
