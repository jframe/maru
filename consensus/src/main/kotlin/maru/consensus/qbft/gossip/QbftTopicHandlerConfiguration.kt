/*
 * Copyright Consensys Software Inc.
 *
 * This file is dual-licensed under either the MIT license or Apache License 2.0.
 * See the LICENSE-MIT and LICENSE-APACHE files in the repository root for details.
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */
package maru.consensus.qbft.gossip

/**
 * Configuration guide for setting up QBFT topic handler in P2P module.
 * 
 * Unlike beacon blocks, QBFT messages don't require strict ordering enforcement 
 * at the gossip layer. The QBFT consensus algorithm handles message ordering 
 * internally based on rounds and heights.
 * 
 * To configure QBFT topic handler in P2PNetworkImpl:
 * 
 * 1. Create a topic ID for QBFT:
 * ```
 * private val qbftTopicId = topicIdGenerator.id(
 *   GossipMessageType.QBFT.name,
 *   Version.V1,
 *   Encoding.RLP_SNAPPY
 * )
 * ```
 * 
 * 2. Create a simple topic handler without ordering:
 * ```
 * private val qbftTopicHandler = object : TopicHandler {
 *   override fun prepareMessage(payload: Bytes, arrivalTimestamp: Optional<UInt64>): PreparedGossipMessage =
 *     MaruPreparedGossipMessage(
 *       origMessage = payload,
 *       arrTimestamp = arrivalTimestamp,
 *       domain = LINEA_DOMAIN,
 *       topicId = qbftTopicId
 *     )
 *   
 *   override fun handleMessage(message: PreparedGossipMessage): SafeFuture<Libp2pValidationResult> {
 *     try {
 *       val messageData = messageDataSerDe.deserialize(message.originalMessage.toArray())
 *       return qbftSubscriptionManager.handleMessage(messageData)
 *         .thenApply { validationResult ->
 *           validationResult.code.toLibP2P()
 *         }
 *     } catch (e: Exception) {
 *       return SafeFuture.completedFuture(Libp2pValidationResult.Invalid)
 *     }
 *   }
 *   
 *   override fun getMaxMessageSize(): Int = MAX_MESSAGE_SIZE
 * }
 * ```
 * 
 * 3. Register the topic handler during network initialization:
 * ```
 * gossipTopicHandlers.add(qbftTopicId, qbftTopicHandler)
 * ```
 * 
 * This approach:
 * - Avoids unnecessary ordering overhead for QBFT messages
 * - Lets the QBFT engine handle message validation and ordering
 * - Maintains compatibility with the existing gossip infrastructure
 */
object QbftTopicHandlerConfiguration {
  // This is a documentation object to guide P2P module configuration
}