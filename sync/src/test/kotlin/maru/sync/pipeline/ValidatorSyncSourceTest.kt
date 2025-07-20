/*
 * Copyright Consensys Software Inc.
 *
 * This file is dual-licensed under either the MIT license or Apache License 2.0.
 * See the LICENSE-MIT and LICENSE-APACHE files in the repository root for details.
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */
package maru.sync.pipeline

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ValidatorSyncSourceTest {
  @Test
  fun `produces correct ranges for typical case`() {
    val source = ValidatorSyncSource(startBlock = 1u, targetBlock = 1000u, requestSize = 100u)
    val ranges = mutableListOf<SyncTargetRange>()
    while (source.hasNext()) {
      val range = source.next()
      if (range != null) ranges.add(range)
    }
    assertThat(ranges).containsExactly(
      SyncTargetRange(1u, 101u),
      SyncTargetRange(102u, 202u),
      SyncTargetRange(203u, 303u),
      SyncTargetRange(304u, 404u),
      SyncTargetRange(405u, 505u),
      SyncTargetRange(506u, 606u),
      SyncTargetRange(607u, 707u),
      SyncTargetRange(708u, 808u),
      SyncTargetRange(809u, 909u),
      SyncTargetRange(910u, 1000u),
    )
  }

  @Test
  fun `returns no ranges if startBlock equals targetBlock`() {
    val source = ValidatorSyncSource(startBlock = 100u, targetBlock = 100u, requestSize = 10u)
    assertThat(source.hasNext()).isFalse()
  }

  @Test
  fun `handles requestSize larger than range`() {
    val source = ValidatorSyncSource(startBlock = 1u, targetBlock = 5u, requestSize = 10u)
    val range = source.next()
    assertThat(range).isEqualTo(SyncTargetRange(1u, 5u))
    assertThat(source.hasNext()).isFalse()
  }
}
