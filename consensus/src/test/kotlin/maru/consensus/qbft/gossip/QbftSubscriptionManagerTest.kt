/*
 * Copyright Consensys Software Inc.
 *
 * This file is dual-licensed under either the MIT license or Apache License 2.0.
 * See the LICENSE-MIT and LICENSE-APACHE files in the repository root for details.
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */
package maru.consensus.qbft.gossip

import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import maru.p2p.ValidationResult
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.any
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import tech.pegasys.teku.infrastructure.async.SafeFuture

class QbftSubscriptionManagerTest {
  private lateinit var subscriptionManager: QbftSubscriptionManager
  private lateinit var mockMessageData: MessageData

  @BeforeEach
  fun setUp() {
    subscriptionManager = QbftSubscriptionManager()
    mockMessageData = mock<MessageData>()
  }

  @Test
  fun `hasSubscriptions returns false when no subscriptions`() {
    assertFalse(subscriptionManager.hasSubscriptions())
  }

  @Test
  fun `hasSubscriptions returns true when subscriptions exist`() {
    val handler: (MessageData) -> SafeFuture<ValidationResult> =
      { _: MessageData -> SafeFuture.completedFuture(ValidationResult.Companion.Valid) }
    subscriptionManager.subscribe(handler)

    assertTrue(subscriptionManager.hasSubscriptions())
  }

  @Test
  fun `subscribe returns unique subscription IDs`() {
    val handler1: (MessageData) -> SafeFuture<ValidationResult> =
      { _: MessageData -> SafeFuture.completedFuture(ValidationResult.Companion.Valid) }
    val handler2: (MessageData) -> SafeFuture<ValidationResult> =
      { _: MessageData -> SafeFuture.completedFuture(ValidationResult.Companion.Valid) }

    val id1 = subscriptionManager.subscribe(handler1)
    val id2 = subscriptionManager.subscribe(handler2)

    assertNotEquals(id1, id2)
  }

  @Test
  fun `unsubscribe removes subscription`() {
    val handler: (MessageData) -> SafeFuture<ValidationResult> =
      { _: MessageData -> SafeFuture.completedFuture(ValidationResult.Companion.Valid) }
    val id = subscriptionManager.subscribe(handler)

    assertTrue(subscriptionManager.hasSubscriptions())

    subscriptionManager.unsubscribe(id)

    assertFalse(subscriptionManager.hasSubscriptions())
  }

  @Test
  fun `unsubscribe with invalid ID does not throw`() {
    // Should not throw even with invalid ID
    subscriptionManager.unsubscribe(999)
  }

  @Test
  fun `handleMessage returns Ignore when no subscriptions`() {
    val result = subscriptionManager.handleMessage(mockMessageData).get()

    assertTrue(result is ValidationResult.Companion.Ignore)
    assertEquals("No QBFT message subscriptions", (result as ValidationResult.Companion.Ignore).comment)
  }

  @Test
  fun `handleMessage calls all subscribers`() {
    val handler1 = mock<(MessageData) -> SafeFuture<ValidationResult>>()
    val handler2 = mock<(MessageData) -> SafeFuture<ValidationResult>>()

    whenever(handler1(any())).thenReturn(SafeFuture.completedFuture(ValidationResult.Companion.Valid))
    whenever(handler2(any())).thenReturn(SafeFuture.completedFuture(ValidationResult.Companion.Valid))

    subscriptionManager.subscribe(handler1)
    subscriptionManager.subscribe(handler2)

    subscriptionManager.handleMessage(mockMessageData).get()

    verify(handler1, times(1))(mockMessageData)
    verify(handler2, times(1))(mockMessageData)
  }

  @Test
  fun `handleMessage returns Invalid if any handler returns Invalid`() {
    val handler1: (MessageData) -> SafeFuture<ValidationResult> =
      { _: MessageData -> SafeFuture.completedFuture(ValidationResult.Companion.Valid) }
    val handler2: (MessageData) -> SafeFuture<ValidationResult> =
      { _: MessageData ->
        SafeFuture.completedFuture(ValidationResult.Companion.Invalid("Test error", null))
      }
    val handler3: (MessageData) -> SafeFuture<ValidationResult> =
      { _: MessageData -> SafeFuture.completedFuture(ValidationResult.Companion.Valid) }

    subscriptionManager.subscribe(handler1)
    subscriptionManager.subscribe(handler2)
    subscriptionManager.subscribe(handler3)

    val result = subscriptionManager.handleMessage(mockMessageData).get()

    assertTrue(result is ValidationResult.Companion.Invalid)
    assertEquals("Test error", (result as ValidationResult.Companion.Invalid).error)
  }

  @Test
  fun `handleMessage returns Ignore if any handler returns Ignore and none return Invalid`() {
    val handler1: (MessageData) -> SafeFuture<ValidationResult> =
      { _: MessageData -> SafeFuture.completedFuture(ValidationResult.Companion.Valid) }
    val handler2: (MessageData) -> SafeFuture<ValidationResult> =
      { _: MessageData ->
        SafeFuture.completedFuture(ValidationResult.Companion.Ignore("Test ignore"))
      }
    val handler3: (MessageData) -> SafeFuture<ValidationResult> =
      { _: MessageData -> SafeFuture.completedFuture(ValidationResult.Companion.Valid) }

    subscriptionManager.subscribe(handler1)
    subscriptionManager.subscribe(handler2)
    subscriptionManager.subscribe(handler3)

    val result = subscriptionManager.handleMessage(mockMessageData).get()

    assertTrue(result is ValidationResult.Companion.Ignore)
    assertEquals("Test ignore", (result as ValidationResult.Companion.Ignore).comment)
  }

  @Test
  fun `handleMessage returns Valid if all handlers return Valid`() {
    val handler1: (MessageData) -> SafeFuture<ValidationResult> =
      { _: MessageData -> SafeFuture.completedFuture(ValidationResult.Companion.Valid) }
    val handler2: (MessageData) -> SafeFuture<ValidationResult> =
      { _: MessageData -> SafeFuture.completedFuture(ValidationResult.Companion.Valid) }

    subscriptionManager.subscribe(handler1)
    subscriptionManager.subscribe(handler2)

    val result = subscriptionManager.handleMessage(mockMessageData).get()

    assertTrue(result is ValidationResult.Companion.Valid)
  }

  @Test
  fun `handleMessage handles exception from subscriber`() {
    val handler1: (MessageData) -> SafeFuture<ValidationResult> =
      { _: MessageData -> SafeFuture.completedFuture(ValidationResult.Companion.Valid) }
    val handler2 = mock<(MessageData) -> SafeFuture<ValidationResult>>()

    doThrow(RuntimeException("Test exception")).whenever(handler2)(any())

    subscriptionManager.subscribe(handler1)
    subscriptionManager.subscribe(handler2)

    val result = subscriptionManager.handleMessage(mockMessageData).get()

    assertTrue(result is ValidationResult.Companion.Invalid)
    assertTrue((result as ValidationResult.Companion.Invalid).error.contains("Exception during QBFT message handling"))
  }

  @Test
  fun `handleMessage fails when any handler future fails`() {
    // SafeFuture.collectAll will fail if any of the futures fail
    val handler1: (MessageData) -> SafeFuture<ValidationResult> =
      { _: MessageData -> SafeFuture.completedFuture(ValidationResult.Companion.Valid) }
    val handler2: (MessageData) -> SafeFuture<ValidationResult> =
      { _: MessageData ->
        SafeFuture.failedFuture(RuntimeException("Test exception"))
      }

    subscriptionManager.subscribe(handler1)
    subscriptionManager.subscribe(handler2)

    // The future should fail because one handler returns a failed future
    val futureResult = subscriptionManager.handleMessage(mockMessageData)

    // This should throw ExecutionException containing the RuntimeException
    assertThrows<Exception> {
      futureResult.get()
    }
  }

  @Test
  fun `clear removes all subscriptions`() {
    val handler1: (MessageData) -> SafeFuture<ValidationResult> =
      { _: MessageData -> SafeFuture.completedFuture(ValidationResult.Companion.Valid) }
    val handler2: (MessageData) -> SafeFuture<ValidationResult> =
      { _: MessageData -> SafeFuture.completedFuture(ValidationResult.Companion.Valid) }

    subscriptionManager.subscribe(handler1)
    subscriptionManager.subscribe(handler2)

    assertTrue(subscriptionManager.hasSubscriptions())

    subscriptionManager.clear()

    assertFalse(subscriptionManager.hasSubscriptions())
  }

  @Test
  fun `clear resets subscription ID counter`() {
    val handler: (MessageData) -> SafeFuture<ValidationResult> =
      { _: MessageData -> SafeFuture.completedFuture(ValidationResult.Companion.Valid) }

    subscriptionManager.subscribe(handler)
    subscriptionManager.clear()
    val id2 = subscriptionManager.subscribe(handler)

    // After clear, subscription IDs should start from 0 again
    assertEquals(0, id2)
  }

  @Test
  fun `unsubscribed handler is not called`() {
    val handler1 = mock<(MessageData) -> SafeFuture<ValidationResult>>()
    val handler2 = mock<(MessageData) -> SafeFuture<ValidationResult>>()

    whenever(handler1(any())).thenReturn(SafeFuture.completedFuture(ValidationResult.Companion.Valid))
    whenever(handler2(any())).thenReturn(SafeFuture.completedFuture(ValidationResult.Companion.Valid))

    val id1 = subscriptionManager.subscribe(handler1)
    subscriptionManager.subscribe(handler2)

    subscriptionManager.unsubscribe(id1)

    subscriptionManager.handleMessage(mockMessageData).get()

    verify(handler1, never())(any())
    verify(handler2, times(1))(mockMessageData)
  }

  @Test
  fun `multiple subscriptions and unsubscriptions work correctly`() {
    val handlers = (1..5).map { _ ->
      val handler: (MessageData) -> SafeFuture<ValidationResult> =
        { _: MessageData -> SafeFuture.completedFuture(ValidationResult.Companion.Valid) }
      handler
    }

    val ids = handlers.map { subscriptionManager.subscribe(it) }

    // Unsubscribe some handlers
    subscriptionManager.unsubscribe(ids[1])
    subscriptionManager.unsubscribe(ids[3])

    assertTrue(subscriptionManager.hasSubscriptions())

    // Clear remaining
    subscriptionManager.clear()

    assertFalse(subscriptionManager.hasSubscriptions())
  }
}
