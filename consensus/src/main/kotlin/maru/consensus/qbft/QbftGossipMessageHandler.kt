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
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.hyperledger.besu.consensus.common.bft.BftEventQueue
import org.hyperledger.besu.consensus.common.bft.events.BftReceivedMessageEvent
import org.hyperledger.besu.consensus.qbft.core.messagedata.QbftV1
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData
import tech.pegasys.teku.infrastructure.async.SafeFuture

/**
 * Simple Message implementation that wraps MessageData for BftReceivedMessageEvent.
 * This allows us to pass MessageData to the QBFT consensus engine.
 * Since these messages come from gossip rather than direct peer connections,
 * we don't have a PeerConnection to return.
 */
private class MessageWrapper(private val messageData: MessageData) : 
    org.hyperledger.besu.ethereum.p2p.rlpx.wire.Message {
  override fun getData(): MessageData = messageData
  
  override fun getConnection(): org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection? {
    // Gossip messages don't have a direct peer connection
    return null
  }
}

/**
 * Handles incoming QBFT gossip messages and routes them to the consensus engine.
 * 
 * This handler subscribes to the QbftSubscriptionManager to receive QBFT messages
 * from the P2P gossip network and converts them into BftReceivedMessageEvent objects
 * for processing by the QBFT consensus engine.
 * 
 * Note: Only validator nodes should instantiate and start this handler, as only
 * validators need to subscribe to and process QBFT consensus messages.
 * 
 * The handler performs basic validation on incoming messages and then creates
 * BftReceivedMessageEvent objects which are added to the bftEventQueue for
 * processing by the QBFT consensus state machine.
 */
class QbftGossipMessageHandler(
  private val bftEventQueue: BftEventQueue,
  private val qbftSubscriptionManager: QbftSubscriptionManager,
) {
  companion object {
    private val log: Logger = LogManager.getLogger(QbftGossipMessageHandler::class.java)
    
    // Valid QBFT message codes
    private val VALID_MESSAGE_CODES = setOf(
      QbftV1.PROPOSAL,
      QbftV1.PREPARE,
      QbftV1.COMMIT,
      QbftV1.ROUND_CHANGE
    )
  }
  
  private var subscriptionId: Int? = null
  
  /**
   * Start handling QBFT gossip messages.
   */
  fun start() {
    if (subscriptionId != null) {
      log.warn("QbftGossipMessageHandler already started")
      return
    }
    
    subscriptionId = qbftSubscriptionManager.subscribe { messageData ->
      handleQbftMessage(messageData)
    }
    
    log.info("Started QBFT gossip message handler with subscription ID: {}", subscriptionId)
  }
  
  /**
   * Stop handling QBFT gossip messages.
   */
  fun stop() {
    subscriptionId?.let { id ->
      qbftSubscriptionManager.unsubscribe(id)
      subscriptionId = null
      log.info("Stopped QBFT gossip message handler")
    }
  }
  
  /**
   * Handle an incoming QBFT message from the gossip network.
   * 
   * This method performs basic validation on the message. The actual
   * processing is handled by the QBFT consensus engine when messages
   * are received through the P2P topic handler.
   * 
   * @param messageData The raw QBFT message data
   * @return Validation result indicating whether the message was accepted
   */
  private fun handleQbftMessage(messageData: MessageData): SafeFuture<ValidationResult> {
    return try {
      log.trace("Received QBFT message via gossip: code={}, size={}", messageData.code, messageData.size)
      
      // Validate message code
      if (messageData.code !in VALID_MESSAGE_CODES) {
        log.debug("Invalid QBFT message code: {}", messageData.code)
        return SafeFuture.completedFuture(
          ValidationResult.Companion.Invalid(
            "Invalid QBFT message code: ${messageData.code}",
            null
          )
        )
      }
      
      // Basic size validation
      if (messageData.size == 0) {
        log.debug("Empty QBFT message data")
        return SafeFuture.completedFuture(
          ValidationResult.Companion.Invalid(
            "Empty QBFT message data",
            null
          )
        )
      }
      
      // Create a Message wrapper around MessageData and then create BftReceivedMessageEvent
      val message = MessageWrapper(messageData)
      val event = BftReceivedMessageEvent(message)
      bftEventQueue.add(event)
      
      log.debug("Added QBFT message to event queue: code={}", messageData.code)
      
      SafeFuture.completedFuture(ValidationResult.Companion.Valid)
    } catch (e: Exception) {
      log.error("Failed to handle QBFT gossip message", e)
      SafeFuture.completedFuture(
        ValidationResult.Companion.Invalid(
          "Failed to process QBFT message: ${e.message}",
          e
        )
      )
    }
  }
}