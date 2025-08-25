/*
 * Copyright Consensys Software Inc.
 *
 * This file is dual-licensed under either the MIT license or Apache License 2.0.
 * See the LICENSE-MIT and LICENSE-APACHE files in the repository root for details.
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */
package maru.consensus.qbft.gossip

import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Supplier
import maru.p2p.ValidationResult
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData
import tech.pegasys.teku.infrastructure.async.SafeFuture

/**
 * Manages subscriptions for QBFT messages received via gossip.
 * 
 * This subscription manager is responsible for handling incoming QBFT messages
 * from the P2P layer and distributing them to registered handlers within the
 * consensus module.
 */
class QbftSubscriptionManager {
  companion object {
    private val log: Logger = LogManager.getLogger(QbftSubscriptionManager::class.java)
  }

  private val nextSubscriptionId = AtomicInteger()
  private val subscriptions: MutableMap<Int, (MessageData) -> SafeFuture<ValidationResult>> = mutableMapOf()

  /**
   * Check if there are any active subscriptions.
   */
  fun hasSubscriptions(): Boolean = subscriptions.isNotEmpty()

  /**
   * Subscribe to QBFT messages.
   * 
   * @param subscriber Handler for incoming QBFT messages
   * @return Subscription ID that can be used to unsubscribe
   */
  @Synchronized
  fun subscribe(subscriber: (MessageData) -> SafeFuture<ValidationResult>): Int {
    val subscriptionId = nextSubscriptionId.getAndIncrement()
    subscriptions[subscriptionId] = subscriber
    log.debug("Added QBFT message subscription with ID: {}", subscriptionId)
    return subscriptionId
  }

  /**
   * Remove a subscription.
   * 
   * @param subscriptionId The ID returned from subscribe()
   */
  @Synchronized
  fun unsubscribe(subscriptionId: Int) {
    if (subscriptions.remove(subscriptionId) != null) {
      log.debug("Removed QBFT message subscription with ID: {}", subscriptionId)
    }
  }

  /**
   * Handle an incoming QBFT message by distributing it to all subscribers.
   * 
   * @param messageData The QBFT message data
   * @return Combined validation result from all subscribers
   */
  fun handleMessage(messageData: MessageData): SafeFuture<ValidationResult> {
    if (subscriptions.isEmpty()) {
      log.trace("No subscriptions for QBFT message, ignoring")
      return SafeFuture.completedFuture(
        ValidationResult.Companion.Ignore("No QBFT message subscriptions")
      )
    }

    val handlerFutures = subscriptions.map { (subscriptionId, handler) ->
      try {
        log.trace("Handling QBFT message in subscription: {}", subscriptionId)
        handler(messageData)
      } catch (th: Throwable) {
        log.error(
          Supplier<String> { "Error from subscription=$subscriptionId while handling QBFT message" },
          th
        )
        SafeFuture.completedFuture<ValidationResult>(
          ValidationResult.Companion.Invalid(
            "Exception during QBFT message handling",
            th
          )
        )
      }
    }

    return SafeFuture.collectAll(handlerFutures.stream()).thenApply { results ->
      results.reduce { acc: ValidationResult, next: ValidationResult ->
        when {
          // If any handler returns Invalid, the overall result is Invalid
          acc is ValidationResult.Companion.Invalid -> acc
          next is ValidationResult.Companion.Invalid -> next
          // If any handler returns Ignore, prefer that over Valid
          acc is ValidationResult.Companion.Ignore -> acc
          next is ValidationResult.Companion.Ignore -> next
          // Otherwise both are Valid
          else -> acc
        }
      }
    }
  }

  /**
   * Clear all subscriptions.
   */
  @Synchronized
  fun clear() {
    subscriptions.clear()
    nextSubscriptionId.set(0)
    log.debug("Cleared all QBFT message subscriptions")
  }
}