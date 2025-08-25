/*
 * Copyright Consensys Software Inc.
 *
 * This file is dual-licensed under either the MIT license or Apache License 2.0.
 * See the LICENSE-MIT and LICENSE-APACHE files in the repository root for details.
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */
package maru.consensus.qbft

import maru.consensus.qbft.gossip.QbftSubscriptionManager
import maru.p2p.ValidationResult
import org.apache.tuweni.bytes.Bytes
import org.hyperledger.besu.consensus.common.bft.BftEventQueue
import org.hyperledger.besu.consensus.common.bft.events.BftEvent
import org.hyperledger.besu.consensus.common.bft.events.BftReceivedMessageEvent
import org.hyperledger.besu.consensus.qbft.core.messagedata.QbftV1
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import tech.pegasys.teku.infrastructure.async.SafeFuture

class QbftGossipMessageHandlerTest {
  private lateinit var bftEventQueue: BftEventQueue
  private lateinit var qbftSubscriptionManager: QbftSubscriptionManager
  private lateinit var handler: QbftGossipMessageHandler
  
  @BeforeEach
  fun setUp() {
    bftEventQueue = mock()
    qbftSubscriptionManager = mock()
    handler = QbftGossipMessageHandler(bftEventQueue, qbftSubscriptionManager)
  }
  
  @Test
  fun `start subscribes to QbftSubscriptionManager`() {
    // Given
    whenever(qbftSubscriptionManager.subscribe(any())).thenReturn(123)
    
    // When
    handler.start()
    
    // Then
    verify(qbftSubscriptionManager).subscribe(any())
  }
  
  @Test
  fun `start does nothing if already started`() {
    // Given
    whenever(qbftSubscriptionManager.subscribe(any())).thenReturn(123)
    handler.start()
    
    // When
    handler.start() // Start again
    
    // Then
    verify(qbftSubscriptionManager).subscribe(any()) // Only called once
  }
  
  @Test
  fun `stop unsubscribes from QbftSubscriptionManager`() {
    // Given
    whenever(qbftSubscriptionManager.subscribe(any())).thenReturn(123)
    handler.start()
    
    // When
    handler.stop()
    
    // Then
    verify(qbftSubscriptionManager).unsubscribe(123)
  }
  
  @Test
  fun `stop does nothing if not started`() {
    // When
    handler.stop()
    
    // Then
    verify(qbftSubscriptionManager, never()).unsubscribe(any())
  }
  
  @Test
  fun `valid QBFT messages are added to event queue`() {
    // Given
    val messageData = createMockMessageData(QbftV1.PROPOSAL, "0x1234")
    whenever(qbftSubscriptionManager.subscribe(any())).thenAnswer { invocation ->
      val subscriber = invocation.getArgument<(MessageData) -> SafeFuture<ValidationResult>>(0)
      // Simulate receiving a message
      subscriber(messageData)
      123
    }
    
    // When
    handler.start()
    
    // Then
    val eventCaptor = argumentCaptor<BftEvent>()
    verify(bftEventQueue).add(eventCaptor.capture())
    
    val capturedEvent = eventCaptor.firstValue
    assertTrue(capturedEvent is BftReceivedMessageEvent)
    val bftReceivedMessageEvent = capturedEvent as BftReceivedMessageEvent
    assertNotNull(bftReceivedMessageEvent.message)
    assertEquals(messageData, bftReceivedMessageEvent.message.data)
  }
  
  @Test
  fun `all valid QBFT message types are accepted`() {
    val validCodes = listOf(
      QbftV1.PROPOSAL,
      QbftV1.PREPARE,
      QbftV1.COMMIT,
      QbftV1.ROUND_CHANGE
    )
    
    validCodes.forEach { code ->
      // Given
      val messageData = createMockMessageData(code, "0xABCD")
      var result: SafeFuture<ValidationResult>? = null
      
      whenever(qbftSubscriptionManager.subscribe(any())).thenAnswer { invocation ->
        val subscriber = invocation.getArgument<(MessageData) -> SafeFuture<ValidationResult>>(0)
        result = subscriber(messageData)
        123
      }
      
      // When
      handler.start()
      
      // Then
      assertNotNull(result)
      val validationResult = result!!.get()
      assertTrue(validationResult is ValidationResult.Companion.Valid)
      verify(bftEventQueue).add(any())
      
      // Reset for next iteration
      setUp()
    }
  }
  
  @Test
  fun `invalid QBFT message code returns Invalid`() {
    // Given
    val invalidMessageData = createMockMessageData(99, "0x1234") // Invalid code
    var result: SafeFuture<ValidationResult>? = null
    
    whenever(qbftSubscriptionManager.subscribe(any())).thenAnswer { invocation ->
      val subscriber = invocation.getArgument<(MessageData) -> SafeFuture<ValidationResult>>(0)
      result = subscriber(invalidMessageData)
      123
    }
    
    // When
    handler.start()
    
    // Then
    assertNotNull(result)
    val validationResult = result!!.get()
    assertTrue(validationResult is ValidationResult.Companion.Invalid)
    assertEquals("Invalid QBFT message code: 99", (validationResult as ValidationResult.Companion.Invalid).error)
    verify(bftEventQueue, never()).add(any())
  }
  
  @Test
  fun `empty message data returns Invalid`() {
    // Given
    val emptyMessageData = createMockMessageData(QbftV1.PREPARE, "")
    var result: SafeFuture<ValidationResult>? = null
    
    whenever(qbftSubscriptionManager.subscribe(any())).thenAnswer { invocation ->
      val subscriber = invocation.getArgument<(MessageData) -> SafeFuture<ValidationResult>>(0)
      result = subscriber(emptyMessageData)
      123
    }
    
    // When
    handler.start()
    
    // Then
    assertNotNull(result)
    val validationResult = result!!.get()
    assertTrue(validationResult is ValidationResult.Companion.Invalid)
    assertEquals("Empty QBFT message data", (validationResult as ValidationResult.Companion.Invalid).error)
    verify(bftEventQueue, never()).add(any())
  }
  
  @Test
  fun `exception during processing returns Invalid`() {
    // Given
    val messageData = createMockMessageData(QbftV1.COMMIT, "0x5678")
    whenever(bftEventQueue.add(any())).thenThrow(RuntimeException("Test exception"))
    
    var result: SafeFuture<ValidationResult>? = null
    whenever(qbftSubscriptionManager.subscribe(any())).thenAnswer { invocation ->
      val subscriber = invocation.getArgument<(MessageData) -> SafeFuture<ValidationResult>>(0)
      result = subscriber(messageData)
      123
    }
    
    // When
    handler.start()
    
    // Then
    assertNotNull(result)
    val validationResult = result!!.get()
    assertTrue(validationResult is ValidationResult.Companion.Invalid)
    assertTrue((validationResult as ValidationResult.Companion.Invalid).error.contains("Failed to process QBFT message"))
  }
  
  @Test
  fun `message is cast to Message interface before creating event`() {
    // Given
    val messageData = createMockMessageData(QbftV1.ROUND_CHANGE, "0x9ABC")
    whenever(qbftSubscriptionManager.subscribe(any())).thenAnswer { invocation ->
      val subscriber = invocation.getArgument<(MessageData) -> SafeFuture<ValidationResult>>(0)
      subscriber(messageData)
      123
    }
    
    // When
    handler.start()
    
    // Then
    verify(bftEventQueue).add(argThat { event ->
      event is BftReceivedMessageEvent && event.message.data == messageData
    })
  }
  
  private fun createMockMessageData(code: Int, hexData: String): MessageData {
    val mockMessageData = mock<MessageData>()
    whenever(mockMessageData.code).thenReturn(code)
    whenever(mockMessageData.data).thenReturn(
      if (hexData.isEmpty()) Bytes.EMPTY else Bytes.fromHexString(hexData)
    )
    whenever(mockMessageData.size).thenReturn(
      if (hexData.isEmpty()) 0 else Bytes.fromHexString(hexData).size()
    )
    
    return mockMessageData
  }
}