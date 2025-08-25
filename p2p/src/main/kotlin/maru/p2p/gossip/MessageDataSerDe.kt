/*
 * Copyright Consensys Software Inc.
 *
 * This file is dual-licensed under either the MIT license or Apache License 2.0.
 * See the LICENSE-MIT and LICENSE-APACHE files in the repository root for details.
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */
package maru.p2p.gossip

import maru.serialization.SerDe
import org.apache.tuweni.bytes.Bytes
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData

/**
 * SerDe implementation for MessageData types.
 * Handles serialization and deserialization of MessageData with type discrimination.
 */
class MessageDataSerDe : SerDe<MessageData> {
  /**
   * Serialize a MessageData with type discrimination.
   * Format: [1 byte message type code][remaining bytes: message data]
   *
   * @param value The MessageData to serialize
   * @return The serialized bytes including type discrimination
   */
  override fun serialize(value: MessageData): ByteArray {
    // Use the message's own code directly
    val typeCode = value.code

    // Concatenate type byte with message data
    return Bytes.concatenate(Bytes.of(typeCode.toByte()), value.data).toArray()
  }

  /**
   * Deserialize bytes into a MessageData.
   * The first byte determines the message type.
   *
   * @param bytes The serialized message bytes
   * @return The deserialized MessageData
   */
  override fun deserialize(bytes: ByteArray): MessageData {
    require(bytes.isNotEmpty()) { "Message data cannot be empty" }

    val wrappedBytes = Bytes.wrap(bytes)
    val typeCode = wrappedBytes.get(0).toInt()
    val messageData = wrappedBytes.slice(1)

    // Create a wrapper MessageData that will be decoded by the appropriate engine
    return object : MessageData {
      override fun getCode(): Int = typeCode
      
      override fun getData(): Bytes = messageData
      
      override fun getSize(): Int = messageData.size()
    }
  }
}