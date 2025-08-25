/*
 * Copyright Consensys Software Inc.
 *
 * This file is dual-licensed under either the MIT license or Apache License 2.0.
 * See the LICENSE-MIT and LICENSE-APACHE files in the repository root for details.
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */
package maru.consensus.qbft

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import maru.consensus.qbft.gossip.QbftSubscriptionManager
import maru.p2p.Message
import maru.p2p.P2PNetwork
import maru.p2p.ValidationResult
import maru.p2p.GossipMessageType
import maru.p2p.Version
import maru.p2p.gossip.MessageDataSerDe
import org.apache.tuweni.bytes.Bytes
import org.hyperledger.besu.consensus.common.bft.BftEventQueue
import org.hyperledger.besu.consensus.common.bft.events.BftEvent
import org.hyperledger.besu.consensus.common.bft.events.BftReceivedMessageEvent
import org.hyperledger.besu.consensus.qbft.core.messagedata.QbftV1
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import tech.pegasys.teku.infrastructure.async.SafeFuture

class QbftGossipIntegrationTest {
  private lateinit var p2pNetwork: P2PNetwork
  private lateinit var bftEventQueue: BftEventQueue
  private lateinit var qbftSubscriptionManager: QbftSubscriptionManager
  private lateinit var qbftGossipMessageHandler: QbftGossipMessageHandler
  private lateinit var messageDataSerDe: MessageDataSerDe
  
  @BeforeEach
  fun setUp() {
    p2pNetwork = mock()
    bftEventQueue = mock()
    qbftSubscriptionManager = QbftSubscriptionManager()
    qbftGossipMessageHandler = QbftGossipMessageHandler(bftEventQueue, qbftSubscriptionManager)
    messageDataSerDe = MessageDataSerDe()
  }
  
  @Test
  fun `end-to-end flow - broadcast QBFT message and receive via gossip`() {
    // Given
    val testMessageData = createTestMessageData(QbftV1.PROPOSAL, "0x1234567890")
    val latch = CountDownLatch(1)
    var receivedEvent: BftEvent? = null
    
    // Mock P2P network to simulate receiving the gossip message
    whenever(p2pNetwork.broadcastMessage(any())).thenAnswer { invocation ->
      val message = invocation.getArgument<Message<*, GossipMessageType>>(0)
      
      // Simulate receiving the message via gossip
      if (message.type == GossipMessageType.QBFT && message.payload is MessageData) {
        val messageData = message.payload as MessageData
        // Simulate the P2P layer receiving this message and passing it to the subscription manager
        qbftSubscriptionManager.handleMessage(messageData)
      }
      
      SafeFuture.completedFuture(Unit)
    }
    
    // Mock event queue to capture the event
    doAnswer { invocation ->
      receivedEvent = invocation.getArgument(0)
      latch.countDown()
      Unit
    }.whenever(bftEventQueue).add(any())
    
    // Start the handler
    qbftGossipMessageHandler.start()
    
    // When - broadcast a QBFT message
    val domainMessage = Message(
      type = GossipMessageType.QBFT,
      version = Version.V1,
      payload = testMessageData
    )
    p2pNetwork.broadcastMessage(domainMessage)
    
    // Then - verify the message was received and processed
    assertTrue(latch.await(5, TimeUnit.SECONDS), "Message was not processed within timeout")
    
    assertNotNull(receivedEvent)
    assertTrue(receivedEvent is BftReceivedMessageEvent)
    val bftReceivedMessageEvent = receivedEvent as BftReceivedMessageEvent
    assertEquals(testMessageData, bftReceivedMessageEvent.message.data)
    
    // Cleanup
    qbftGossipMessageHandler.stop()
  }
  
  @Test
  fun `multiple subscribers receive QBFT messages`() {
    // Given
    val subscriber1Latch = CountDownLatch(1)
    val subscriber2Latch = CountDownLatch(1)
    var subscriber1ReceivedData: MessageData? = null
    var subscriber2ReceivedData: MessageData? = null
    
    // Subscribe multiple handlers
    qbftSubscriptionManager.subscribe { messageData ->
      subscriber1ReceivedData = messageData
      subscriber1Latch.countDown()
      SafeFuture.completedFuture(ValidationResult.Companion.Valid)
    }
    
    qbftSubscriptionManager.subscribe { messageData ->
      subscriber2ReceivedData = messageData
      subscriber2Latch.countDown()
      SafeFuture.completedFuture(ValidationResult.Companion.Valid)
    }
    
    // When - handle a message
    val testMessageData = createTestMessageData(QbftV1.COMMIT, "0xABCDEF")
    qbftSubscriptionManager.handleMessage(testMessageData)
    
    // Then - both subscribers should receive the message
    assertTrue(subscriber1Latch.await(1, TimeUnit.SECONDS))
    assertTrue(subscriber2Latch.await(1, TimeUnit.SECONDS))
    
    assertEquals(testMessageData, subscriber1ReceivedData)
    assertEquals(testMessageData, subscriber2ReceivedData)
  }
  
  @Test
  fun `serialization round-trip preserves message data`() {
    // Given
    val originalMessage = createTestMessageData(QbftV1.ROUND_CHANGE, "0xDEADBEEF1234")
    
    // When - serialize and deserialize
    val serialized = messageDataSerDe.serialize(originalMessage)
    val deserialized = messageDataSerDe.deserialize(serialized)
    
    // Then
    assertEquals(originalMessage.code, deserialized.code)
    assertEquals(originalMessage.data, deserialized.data)
    assertEquals(originalMessage.size, deserialized.size)
  }
  
  @Test
  fun `invalid message codes are rejected`() {
    // Given
    val invalidMessageData = createTestMessageData(99, "0x1234") // Invalid code
    val latch = CountDownLatch(1)
    var validationResult: ValidationResult? = null
    
    // Subscribe to capture validation result
    qbftSubscriptionManager.subscribe { messageData ->
      // This is what QbftGossipMessageHandler does internally
      validationResult = if (messageData.code !in setOf(QbftV1.PROPOSAL, QbftV1.PREPARE, QbftV1.COMMIT, QbftV1.ROUND_CHANGE)) {
        ValidationResult.Companion.Invalid("Invalid QBFT message code: ${messageData.code}", null)
      } else {
        ValidationResult.Companion.Valid
      }
      latch.countDown()
      SafeFuture.completedFuture(validationResult!!)
    }
    
    // When
    qbftSubscriptionManager.handleMessage(invalidMessageData)
    
    // Then
    assertTrue(latch.await(1, TimeUnit.SECONDS))
    assertTrue(validationResult is ValidationResult.Companion.Invalid)
    assertEquals("Invalid QBFT message code: 99", (validationResult as ValidationResult.Companion.Invalid).error)
  }
  
  @Test
  fun `message types are validated correctly`() {
    // Given
    val validCodes = listOf(
      QbftV1.PROPOSAL,
      QbftV1.PREPARE,
      QbftV1.COMMIT,
      QbftV1.ROUND_CHANGE
    )
    
    // Start handler which subscribes to the manager
    qbftGossipMessageHandler.start()
    
    // Verify handler is subscribed
    assertTrue(qbftSubscriptionManager.hasSubscriptions(), "Handler did not subscribe to manager")
    
    // Test valid message codes
    validCodes.forEach { code ->
      val messageData = createTestMessageData(code, "0x12345678")
      val future = qbftSubscriptionManager.handleMessage(messageData)
      val result = future.get(1, TimeUnit.SECONDS)
      
      assertTrue(result is ValidationResult.Companion.Valid, 
        "Message with code $code should be valid but got: $result")
    }
    
    // Test invalid message code
    val invalidMessageData = createTestMessageData(99, "0xBAD")
    val invalidFuture = qbftSubscriptionManager.handleMessage(invalidMessageData)
    val invalidResult = invalidFuture.get(1, TimeUnit.SECONDS)
    
    assertTrue(invalidResult is ValidationResult.Companion.Invalid,
      "Message with invalid code should be rejected")
    
    // Cleanup
    qbftGossipMessageHandler.stop()
    assertFalse(qbftSubscriptionManager.hasSubscriptions(), "Handler did not unsubscribe from manager")
  }
  
  private fun createTestMessageData(code: Int, hexData: String): MessageData {
    return object : MessageData {
      override fun getCode(): Int = code
      override fun getData(): Bytes = Bytes.fromHexString(hexData)
      override fun getSize(): Int = data.size()
    }
  }
}