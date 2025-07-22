/*
 * Copyright Consensys Software Inc.
 *
 * This file is dual-licensed under either the MIT license or Apache License 2.0.
 * See the LICENSE-MIT and LICENSE-APACHE files in the repository root for details.
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */
package maru.p2p

import io.libp2p.core.PeerId
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import kotlin.random.Random
import kotlin.random.nextULong
import maru.config.P2P
import maru.p2p.discovery.MaruDiscoveryService
import maru.p2p.messages.Status
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import tech.pegasys.teku.infrastructure.async.SafeFuture
import tech.pegasys.teku.networking.p2p.libp2p.LibP2PNodeId
import tech.pegasys.teku.networking.p2p.network.P2PNetwork
import tech.pegasys.teku.networking.p2p.peer.Peer

class MaruPeerManagerTest {
  private val scheduler = mock<ScheduledExecutorService>()
  private val maruPeerFactory = mock<MaruPeerFactory>()
  private val p2pConfig = mock<P2P>()
  private val discoveryService = mock<MaruDiscoveryService>()
  private val p2pNetwork = mock<P2PNetwork<Peer>>()
  private val scheduledFuture = mock<ScheduledFuture<*>>()

  private lateinit var maruPeerManager: MaruPeerManager

  @BeforeEach
  fun setUp() {
    whenever(p2pConfig.maxPeers).thenReturn(25)
    whenever(scheduler.scheduleAtFixedRate(any(), any(), any(), any())).thenReturn(scheduledFuture)
    whenever(p2pNetwork.peerCount).thenReturn(0)
    whenever(discoveryService.searchForPeers()).thenReturn(SafeFuture.completedFuture(emptyList()))

    maruPeerManager = MaruPeerManager(scheduler, maruPeerFactory, p2pConfig)
    maruPeerManager.start(discoveryService, p2pNetwork)
  }

  @Test
  fun `periodicallyUpdateStatus sends status to all connected peers`() {
    val peer1 = mock<MaruPeer>()
    val peer2 = mock<MaruPeer>()
    val peer3 = mock<MaruPeer>()

    val mockPeer1 = mock<Peer>()
    val mockPeer2 = mock<Peer>()
    val mockPeer3 = mock<Peer>()

    whenever(mockPeer1.id).thenReturn(LibP2PNodeId(PeerId.fromBase58("QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N")))
    whenever(mockPeer2.id).thenReturn(LibP2PNodeId(PeerId.fromBase58("QmRjZZSqV1jMBmDfW2ub8hGYB1NqQBBpx16wH2Nq3x4D4a")))
    whenever(mockPeer3.id).thenReturn(LibP2PNodeId(PeerId.fromBase58("QmPFdSzvgd1HmMkUy8ZcLpyG2FcLGNkRBhj3e8x5R4AJZR")))

    whenever(maruPeerFactory.createMaruPeer(mockPeer1)).thenReturn(peer1)
    whenever(maruPeerFactory.createMaruPeer(mockPeer2)).thenReturn(peer2)
    whenever(maruPeerFactory.createMaruPeer(mockPeer3)).thenReturn(peer3)

    val status1 = Status(Random.nextBytes(32), Random.nextBytes(32), Random.nextULong())
    val status2 = Status(Random.nextBytes(32), Random.nextBytes(32), Random.nextULong())
    val status3 = Status(Random.nextBytes(32), Random.nextBytes(32), Random.nextULong())

    whenever(peer1.sendStatus()).thenReturn(SafeFuture.completedFuture(status1))
    whenever(peer2.sendStatus()).thenReturn(SafeFuture.completedFuture(status2))
    whenever(peer3.sendStatus()).thenReturn(SafeFuture.completedFuture(status3))

    // Simulate peers connecting
    maruPeerManager.onConnect(mockPeer1)
    maruPeerManager.onConnect(mockPeer2)
    maruPeerManager.onConnect(mockPeer3)

    // Execute the periodicallyUpdateStatus method directly
    maruPeerManager.periodicallyUpdateStatus()

    // Verify sendStatus was called on each peer
    verify(peer1).sendStatus()
    verify(peer2).sendStatus()
    verify(peer3).sendStatus()
  }

  @Test
  fun `periodicallyUpdateStatus handles peers with sendStatus failures gracefully`() {
    val peer1 = mock<MaruPeer>()
    val peer2 = mock<MaruPeer>()

    val nodeId1 = LibP2PNodeId(PeerId.fromBase58("QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N"))
    val nodeId2 = LibP2PNodeId(PeerId.fromBase58("QmRjZZSqV1jMBmDfW2ub8hGYB1NqQBBpx16wH2Nq3x4D4a"))

    val mockPeer1 = mock<Peer>()
    val mockPeer2 = mock<Peer>()

    whenever(mockPeer1.id).thenReturn(nodeId1)
    whenever(mockPeer2.id).thenReturn(nodeId2)

    whenever(maruPeerFactory.createMaruPeer(mockPeer1)).thenReturn(peer1)
    whenever(maruPeerFactory.createMaruPeer(mockPeer2)).thenReturn(peer2)

    whenever(peer1.connectionInitiatedLocally()).thenReturn(true)
    whenever(peer2.connectionInitiatedLocally()).thenReturn(true)

    // First peer fails to send status
    val failedFuture = SafeFuture<Status>()
    failedFuture.completeExceptionally(RuntimeException("Failed to send status"))
    whenever(peer1.sendStatus()).thenReturn(failedFuture)

    // Second peer succeeds
    val status2 = Status(Random.nextBytes(32), Random.nextBytes(32), Random.nextULong())
    whenever(peer2.sendStatus()).thenReturn(SafeFuture.completedFuture(status2))

    // Simulate peers connecting
    maruPeerManager.onConnect(mockPeer1)
    maruPeerManager.onConnect(mockPeer2)

    // Execute the periodicallyUpdateStatus method directly
    maruPeerManager.periodicallyUpdateStatus()

    // Verify sendStatus was called on both peers despite the first one failing (once on connect, once on periodic update)
    verify(peer1, times(2)).sendStatus()
    verify(peer2, times(2)).sendStatus()
  }
}
