/*
 * Copyright Consensys Software Inc.
 *
 * This file is dual-licensed under either the MIT license or Apache License 2.0.
 * See the LICENSE-MIT and LICENSE-APACHE files in the repository root for details.
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */
package maru.p2p

import maru.p2p.gossip.MessageDataSerDe
import org.apache.tuweni.bytes.Bytes
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class QbftMessageBroadcastingTest {
  private lateinit var messageDataSerDe: MessageDataSerDe
  
  @BeforeEach
  fun setUp() {
    messageDataSerDe = MessageDataSerDe()
  }
  
  @Test
  fun `QBFT messages can be created with correct codes`() {
    // Create mock QBFT messages for different types
    val proposalMessage = createMockMessageData(0x00, "0x1111")
    val prepareMessage = createMockMessageData(0x01, "0x2222")
    val commitMessage = createMockMessageData(0x02, "0x3333")
    val roundChangeMessage = createMockMessageData(0x03, "0x4444")
    
    // Verify messages can be created with correct codes
    assertEquals(0x00, proposalMessage.code)
    assertEquals(0x01, prepareMessage.code)
    assertEquals(0x02, commitMessage.code)
    assertEquals(0x03, roundChangeMessage.code)
  }
  
  @Test
  fun `message serialization and deserialization preserves data`() {
    val originalMessage = createMockMessageData(0x02, "0xDEADBEEF")
    
    // Serialize
    val serialized = messageDataSerDe.serialize(originalMessage)
    
    // Deserialize
    val deserialized = messageDataSerDe.deserialize(serialized)
    
    // Verify
    assertEquals(originalMessage.code, deserialized.code)
    assertEquals(originalMessage.data, deserialized.data)
    assertEquals(originalMessage.size, deserialized.size)
  }
  
  @Test
  fun `domain message creation works for QBFT`() {
    val messageData = createMockMessageData(0x01, "0xABCD")
    
    // Create domain message
    val domainMessage = Message(
      type = GossipMessageType.QBFT,
      version = Version.V1,
      payload = messageData
    )
    
    // Verify message properties
    assertEquals(GossipMessageType.QBFT, domainMessage.type)
    assertEquals(Version.V1, domainMessage.version)
    assertEquals(messageData, domainMessage.payload)
  }
  
  @Test
  fun `serialization handles all QBFT message types`() {
    val messageTypes = listOf(
      0x00 to "Proposal",
      0x01 to "Prepare",
      0x02 to "Commit",
      0x03 to "RoundChange"
    )
    
    for ((code, name) in messageTypes) {
      val message = createMockMessageData(code, "0xABCDEF")
      val serialized = messageDataSerDe.serialize(message)
      val deserialized = messageDataSerDe.deserialize(serialized)
      
      assertEquals(code, deserialized.code, "Failed for $name message")
      assertEquals(message.data, deserialized.data, "Failed for $name message")
    }
  }
  
  @Test
  fun `deserialization fails with empty data`() {
    val emptyBytes = ByteArray(0)
    
    assertThrows(IllegalArgumentException::class.java) {
      messageDataSerDe.deserialize(emptyBytes)
    }
  }
  
  @Test
  fun `serialization preserves message data integrity`() {
    // Test with various data sizes
    val testCases = listOf(
      "0x00", // Minimal
      "0x0123456789ABCDEF", // Medium
      "0x" + "FF".repeat(100) // Large
    )
    
    for (hexData in testCases) {
      val message = createMockMessageData(0x02, hexData)
      val serialized = messageDataSerDe.serialize(message)
      val deserialized = messageDataSerDe.deserialize(serialized)
      
      assertEquals(message.code, deserialized.code)
      assertEquals(message.data, deserialized.data)
    }
  }
  
  private fun createMockMessageData(code: Int, hexData: String): MessageData {
    return object : MessageData {
      override fun getCode(): Int = code
      override fun getData(): Bytes = Bytes.fromHexString(hexData)
      override fun getSize(): Int = data.size()
    }
  }
}