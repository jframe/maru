/*
 * Copyright Consensys Software Inc.
 *
 * This file is dual-licensed under either the MIT license or Apache License 2.0.
 * See the LICENSE-MIT and LICENSE-APACHE files in the repository root for details.
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */
package maru.p2p.gossip

import org.apache.tuweni.bytes.Bytes
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.hyperledger.besu.consensus.qbft.core.messagedata.QbftV1
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

class MessageDataSerDeTest {
  private val serDe = MessageDataSerDe()

  @Test
  fun `serialize adds type code byte before message data`() {
    // Given
    val messageData = mock<MessageData>()
    val testData = Bytes.fromHexString("0xdeadbeef")
    whenever(messageData.code).thenReturn(QbftV1.PREPARE)
    whenever(messageData.data).thenReturn(testData)

    // When
    val serialized = serDe.serialize(messageData)

    // Then
    val serializedBytes = Bytes.wrap(serialized)
    assertThat(serializedBytes.size()).isEqualTo(5) // 1 byte type + 4 bytes data
    assertThat(serializedBytes.get(0)).isEqualTo(QbftV1.PREPARE.toByte())
    assertThat(serializedBytes.slice(1)).isEqualTo(testData)
  }

  @Test
  fun `deserialize extracts type code and message data`() {
    // Given
    val typeCode = QbftV1.PROPOSAL
    val messageData = Bytes.fromHexString("0xcafebabe")
    val serialized = Bytes.concatenate(Bytes.of(typeCode.toByte()), messageData).toArray()

    // When
    val deserialized = serDe.deserialize(serialized)

    // Then
    assertThat(deserialized.code).isEqualTo(typeCode)
    assertThat(deserialized.data).isEqualTo(messageData)
    assertThat(deserialized.size).isEqualTo(messageData.size())
  }

  @Test
  fun `serialize and deserialize round trip works for all message types`() {
    val testCases = listOf(
      QbftV1.PREPARE to Bytes.fromHexString("0x01"),
      QbftV1.PROPOSAL to Bytes.fromHexString("0x0203"),
      QbftV1.COMMIT to Bytes.fromHexString("0x040506"),
      QbftV1.ROUND_CHANGE to Bytes.fromHexString("0x07080910")
    )

    testCases.forEach { (messageCode, testData) ->
      // Given
      val originalMessage = mock<MessageData>()
      whenever(originalMessage.code).thenReturn(messageCode)
      whenever(originalMessage.data).thenReturn(testData)

      // When
      val serialized = serDe.serialize(originalMessage)
      val deserialized = serDe.deserialize(serialized)

      // Then
      assertThat(deserialized.code).isEqualTo(messageCode)
      assertThat(deserialized.data).isEqualTo(testData)
    }
  }

  @Test
  fun `deserialize throws exception for empty data`() {
    assertThatThrownBy {
      serDe.deserialize(ByteArray(0))
    }.isInstanceOf(IllegalArgumentException::class.java)
      .hasMessage("Message data cannot be empty")
  }

  @Test
  fun `deserialize handles single byte message correctly`() {
    // Given - just a type code with no data
    val typeCode = QbftV1.COMMIT
    val serialized = byteArrayOf(typeCode.toByte())

    // When
    val deserialized = serDe.deserialize(serialized)

    // Then
    assertThat(deserialized.code).isEqualTo(typeCode)
    assertThat(deserialized.data).isEqualTo(Bytes.EMPTY)
    assertThat(deserialized.size).isEqualTo(0)
  }

  @Test
  fun `serialize handles arbitrary message codes`() {
    // Given - a message with a non-standard code
    val messageData = mock<MessageData>()
    val customCode = 99
    val testData = Bytes.fromHexString("0xabcdef")
    whenever(messageData.code).thenReturn(customCode)
    whenever(messageData.data).thenReturn(testData)

    // When
    val serialized = serDe.serialize(messageData)

    // Then
    val serializedBytes = Bytes.wrap(serialized)
    assertThat(serializedBytes.get(0)).isEqualTo(customCode.toByte())
    assertThat(serializedBytes.slice(1)).isEqualTo(testData)
  }
}