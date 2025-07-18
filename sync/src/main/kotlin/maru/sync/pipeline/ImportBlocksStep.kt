/*
 * Copyright Consensys Software Inc.
 *
 * This file is dual-licensed under either the MIT license or Apache License 2.0.
 * See the LICENSE-MIT and LICENSE-APACHE files in the repository root for details.
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */
package maru.sync.pipeline

import java.util.function.Consumer
import maru.core.SealedBeaconBlock

class ImportBlocksStep : Consumer<List<SealedBeaconBlock>> {
  override fun accept(blocks: List<SealedBeaconBlock>) {
    // Implementation for importing blocks goes here
    // This could involve saving the blocks to a database or processing them in some way
  }
}
