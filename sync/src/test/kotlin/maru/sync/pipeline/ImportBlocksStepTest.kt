/*
 * Copyright Consensys Software Inc.
 *
 * This file is dual-licensed under either the MIT license or Apache License 2.0.
 * See the LICENSE-MIT and LICENSE-APACHE files in the repository root for details.
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */
package maru.sync.pipeline

import maru.core.ext.DataGenerators
import maru.database.BeaconChain
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

class ImportBlocksStepTest {
  @Test
  fun `imports valid blocks successfully`() {
    val beaconChain = mock<BeaconChain>()
    val validators = listOf(DataGenerators.randomValidator())
    val block = DataGenerators.randomSealedBeaconBlock(1u)
    whenever(beaconChain.getSealedBeaconBlock(block.beaconBlock.beaconBlockHeader.number - 1uL)).thenReturn(block)

    val step = ImportBlocksStep(beaconChain, validators)
    val blocks = listOf(block)
    step.accept(blocks)
    // No exception means success
    assertThat(true).isTrue()
  }

  @Test
  fun `throws when parent block is missing`() {
    val beaconChain = mock<BeaconChain>()
    val validators = listOf(DataGenerators.randomValidator())
    val block = DataGenerators.randomSealedBeaconBlock(1u)
    whenever(beaconChain.getSealedBeaconBlock(block.beaconBlock.beaconBlockHeader.number - 1uL)).thenReturn(null)

    val step = ImportBlocksStep(beaconChain, validators)
    val blocks = listOf(block)
    try {
      step.accept(blocks)
      assert(false) { "Expected exception" }
    } catch (e: IllegalArgumentException) {
      assertThat(e.message).contains("isn't found")
    }
  }
}
