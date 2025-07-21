/*
 * Copyright Consensys Software Inc.
 *
 * This file is dual-licensed under either the MIT license or Apache License 2.0.
 * See the LICENSE-MIT and LICENSE-APACHE files in the repository root for details.
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */
package maru.sync.pipeline

import java.util.function.Consumer
import maru.consensus.StaticValidatorProvider
import maru.consensus.blockimport.TransactionalSealedBeaconBlockImporter
import maru.consensus.blockimport.ValidatingSealedBeaconBlockImporter
import maru.consensus.qbft.ProposerSelectorImpl
import maru.consensus.state.StateTransitionImpl
import maru.consensus.validation.BeaconBlockValidatorFactory
import maru.consensus.validation.BlockNumberValidator
import maru.consensus.validation.BodyRootValidator
import maru.consensus.validation.CompositeBlockValidator
import maru.consensus.validation.EmptyBlockValidator
import maru.consensus.validation.ParentRootValidator
import maru.consensus.validation.ProposerValidator
import maru.consensus.validation.QuorumOfSealsVerifier
import maru.consensus.validation.SCEP256SealVerifier
import maru.consensus.validation.StateRootValidator
import maru.consensus.validation.TimestampValidator
import maru.core.BeaconBlockHeader
import maru.core.SealedBeaconBlock
import maru.core.Validator
import maru.database.BeaconChain
import maru.p2p.ValidationResultCode
import maru.p2p.ValidationResultCode.ACCEPT
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import tech.pegasys.teku.infrastructure.async.SafeFuture

class ImportBlocksStep(
  private val beaconChain: BeaconChain,
  private val validators: List<Validator>,
  private val allowEmptyBlocks: Boolean = true,
) : Consumer<List<SealedBeaconBlock>> {
  private val log: Logger = LogManager.getLogger(this::javaClass)

  override fun accept(blocks: List<SealedBeaconBlock>) {
    if (blocks.isEmpty()) return

    // Create necessary components
    val validatorProvider = StaticValidatorProvider(validators = validators.toSet())
    val stateTransition = StateTransitionImpl(validatorProvider)
    val sealsVerifier = QuorumOfSealsVerifier(validatorProvider, SCEP256SealVerifier())

    // Create custom BeaconBlockValidatorFactory without ExecutionPayloadValidator
    val beaconBlockValidatorFactory = createBeaconBlockValidatorFactory()

    // Create TransactionalSealedBeaconBlockImporter without BeaconBlockImporter
    // We'll handle state transitions and database commits directly
    val transactionalSealedBeaconBlockImporter =
      TransactionalSealedBeaconBlockImporter(
        beaconChain = beaconChain,
        stateTransition = stateTransition,
        beaconBlockImporter = { _, _ ->
          // TODO store in database
          SafeFuture.completedFuture(Unit)
        },
      )

    // Create ValidatingSealedBeaconBlockImporter
    val validatingSealedBeaconBlockImporter =
      ValidatingSealedBeaconBlockImporter(
        sealsVerifier = sealsVerifier,
        beaconBlockImporter = transactionalSealedBeaconBlockImporter,
        beaconBlockValidatorFactory = beaconBlockValidatorFactory,
      )

    // Process blocks sequentially
    blocks.forEach { sealedBeaconBlock ->
      try {
        val result = validatingSealedBeaconBlockImporter.importBlock(sealedBeaconBlock).join()
        when (result.code) {
          ACCEPT -> {
            log.info(
              "Successfully imported block number={} hash={}",
              sealedBeaconBlock.beaconBlock.beaconBlockHeader.number,
              sealedBeaconBlock.beaconBlock.beaconBlockHeader.hash,
            )
          }
          ValidationResultCode.REJECT -> {
            log.error(
              "Block validation failed for block {}",
              sealedBeaconBlock.beaconBlock.beaconBlockHeader.hash,
            )
            return
          }
          ValidationResultCode.IGNORE -> {
            log.warn("Block validation ignored for block {}", sealedBeaconBlock.beaconBlock.beaconBlockHeader.hash)
            return
          }
        }
      } catch (e: Exception) {
        log.error("Exception importing block", e)
        throw e
      }
    }
  }

  private fun createBeaconBlockValidatorFactory(): BeaconBlockValidatorFactory {
    // Create a custom BeaconBlockValidatorFactory that excludes ExecutionPayloadValidator
    return BeaconBlockValidatorFactory { beaconBlockHeader: BeaconBlockHeader ->
      val parentBeaconBlockNumber = beaconBlockHeader.number - 1UL
      val parentBlock =
        beaconChain.getSealedBeaconBlock(parentBeaconBlockNumber)
          ?: throw IllegalArgumentException("Expected block for header number=$parentBeaconBlockNumber isn't found!")

      val parentHeader = parentBlock.beaconBlock.beaconBlockHeader

      CompositeBlockValidator(
        blockValidators =
          listOfNotNull(
            StateRootValidator(StateTransitionImpl(StaticValidatorProvider(validators.toSet()))),
            BlockNumberValidator(parentHeader),
            TimestampValidator(parentHeader),
            ProposerValidator(ProposerSelectorImpl, beaconChain),
            ParentRootValidator(parentHeader),
            BodyRootValidator(),
            // Excluded ExecutionPayloadValidator
            if (!allowEmptyBlocks) EmptyBlockValidator else null,
          ),
      )
    }
  }
}
