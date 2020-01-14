/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.linking

import com.google.common.base.Stopwatch
import com.google.common.collect.Sets
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.openlattice.data.EntityDataKey
import com.openlattice.data.EntityKeyIdService
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.HazelcastQueue
import com.openlattice.rhizome.core.service.ContinuousRepeatingTaskService
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.sql.Connection
import java.util.*
import java.util.concurrent.TimeUnit

internal const val REFRESH_PROPERTY_TYPES_INTERVAL_MILLIS = 30_000L
internal const val LINKING_BATCH_TIMEOUT_MILLIS = 120_000L
internal const val MINIMUM_SCORE = 0.75

/**
 * Performs realtime linking of individuals as they are integrated ino the system.
 */
@Component
class BackgroundLinkingService
(
        private val executor: ListeningExecutorService,
        hazelcastInstance: HazelcastInstance,
        private val blocker: Blocker,
        private val matcher: Matcher,
        private val ids: EntityKeyIdService,
        private val loader: DataLoader,
        private val lqs: LinkingQueryService,
        private val linkingFeedbackService: PostgresLinkingFeedbackService,
        private val linkableTypes: Set<UUID>,
        private val linkingLogService: LinkingLogService,
        private val configuration: LinkingConfiguration
): ContinuousRepeatingTaskService<EntityDataKey, EntityDataKey>(
        executor,
        LoggerFactory.getLogger(BackgroundLinkingService::class.java),
        HazelcastQueue.LINKING_CANDIDATES.getQueue(hazelcastInstance),
        HazelcastMap.LINKING_LOCKS.getMap(hazelcastInstance),
        configuration.parallelism,
        { entityDataKey -> entityDataKey },
        configuration.loadSize,
        LINKING_BATCH_TIMEOUT_MILLIS
) {

    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundLinkingService::class.java)
    }

    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)

    override fun enqueuerEnabledCheck(): Boolean {
        return configuration.backgroundLinkingEnabled
    }

    override fun workerEnabledCheck(): Boolean {
        return configuration.backgroundLinkingEnabled
    }

    override fun sourceSequence(): Sequence<EntityDataKey> {
        return entitySets.values.asSequence()
                .filter { linkableTypes.contains(it.entityTypeId) && !it.flags.contains(EntitySetFlag.LINKING) }
                .map { it.id }
                .flatMap { esid ->
                    lqs.getEntitiesNeedingLinking(esid, 2 * configuration.loadSize).asSequence()
                }
    }

    override fun operate(candidate: EntityDataKey) {
        link(candidate)
    }

    private fun link(candidate: EntityDataKey) {
        clearNeighborhoods(candidate)
        // if we have positive feedbacks on entity, we use its linking id and match them together
        if (linkingFeedbackService.hasFeedbacks(FeedbackType.Positive, candidate)) {
            try {
                // only linking id of entity should remain, since we cleared neighborhood, except the ones
                // with positive feedback
                val clusters = lqs.getClustersForIds(setOf(candidate))
                val cluster = clusters.entries.first()
                val clusterId = cluster.key

                lqs.lockClustersForUpdates(setOf(clusterId)).use { conn ->
                    val scoredCluster = cluster(candidate, cluster, ::completeLinkCluster)
                    if (scoredCluster.score <= MINIMUM_SCORE) {
                        logger.error(
                                "Recalculated score {} of linking id {} with positives feedbacks did not pass minimum score {}",
                                scoredCluster.score,
                                cluster.key,
                                MINIMUM_SCORE
                        )
                    }

                    insertMatches(conn, scoredCluster.clusterId, candidate, scoredCluster.cluster, false)
                }
            } catch (ex: Exception) {
                logger.error("An error occurred while performing linking.", ex)
                throw IllegalStateException("Error occured while performing linking.", ex)
            }
        } else {
            // Run standard blocking + clustering
            val sw = Stopwatch.createStarted()
            val initialBlock = blocker.block(candidate.entitySetId, candidate.entityKeyId)

            logger.info(
                    "Blocking ({}, {}) took {} ms.",
                    candidate.entitySetId,
                    candidate.entityKeyId,
                    sw.elapsed(TimeUnit.MILLISECONDS)
            )

            //block contains element being blocked
            val elem = initialBlock.second.getValue(candidate)

            // initialize
            sw.reset().start()
            logger.info("Initializing matching for block {}", candidate)
            val initializedBlock = matcher.initialize(initialBlock)
            logger.info("Initialization took {} ms", sw.elapsed(TimeUnit.MILLISECONDS))
            val dataKeys = collectKeys(initializedBlock.second)

            //Decision that needs to be made is whether to start new cluster or merge into existing cluster.
            //No locks are required since any items that block to this element will be skipped.
            try {
                val clusters = lqs.getClustersForIds(dataKeys)
                lqs.lockClustersForUpdates(clusters.keys).use { conn ->
                    val maybeBestCluster = clusters
                            .asSequence()
                            .map { cluster -> cluster(candidate, cluster, ::completeLinkCluster) }
                            .filter { scoredCluster -> scoredCluster.score > MINIMUM_SCORE }
                            .maxBy { scoredCluster -> scoredCluster.score }

                    if (maybeBestCluster != null) {
                        return@use insertMatches(
                                conn, maybeBestCluster.clusterId, candidate, maybeBestCluster.cluster, false
                        )
                    }
                    val clusterId = ids.reserveLinkingIds(1).first()
                    val block = candidate to mapOf(candidate to elem)
                    //TODO: When creating new cluster do we really need to re-match or can we assume score of 1.0?
                    return@use insertMatches(conn, clusterId, candidate, matcher.match(block).second, true)
                }
            } catch (ex: Exception) {
                logger.error("An error occurred while performing linking.", ex)
                throw IllegalStateException("Error occured while performing linking.", ex)
            }
        }
    }

    private fun cluster(
            blockKey: EntityDataKey,
            identifiedCluster: Map.Entry<UUID, Map<EntityDataKey, Map<EntityDataKey, Double>>>,
            clusteringStrategy: (Map<EntityDataKey, Map<EntityDataKey, Double>>) -> Double
    ): ScoredCluster {
        val block = blockKey to loader.getEntities(collectKeys(identifiedCluster.value) + blockKey)
        //At some point, we may want to skip recomputing matches for existing cluster elements as an optimization.
        //Since we're freshly loading entities it's not too bad to recompute everything.
        val matchedBlock = matcher.match(block)
        val matchedCluster = matchedBlock.second
        val score = clusteringStrategy(matchedCluster)
        return ScoredCluster(identifiedCluster.key, matchedCluster, score)
    }

    private fun insertMatches(
            conn: Connection,
            linkingId: UUID,
            newMember: EntityDataKey,
            scores: Map<EntityDataKey, Map<EntityDataKey, Double>>,
            newCluster: Boolean
    ) {
        lqs.insertMatchScores(conn, linkingId, scores)
        lqs.updateIdsTable(linkingId, newMember)

        var toRemove = setOf<EntityDataKey>()
        var toAdd = setOf<EntityDataKey>()
        val oldCluster = if (newCluster) {
            mapOf()
        } else {
            linkingLogService.readLatestLinkLog(linkingId)
        }

        val scoresAsEsidToEkids = (collectKeys(scores) + newMember)
                .groupBy { edk -> edk.entitySetId }
                .mapValues { (esid, edks) ->
                    val newEdks = edks.toSet()
                    val oldEdks = (oldCluster[esid] ?: setOf()).mapTo(mutableSetOf(), { EntityDataKey(esid, it) })

                    toAdd = Sets.union(toAdd, Sets.difference(newEdks, oldEdks))
                    toRemove = Sets.union(toRemove, Sets.difference(oldEdks, newEdks))

                    Sets.newLinkedHashSet(edks.map { it.entityKeyId })
                }

        /* TODO: we do an upsert into data table for every member in the cluster regardless of score */
        if (newCluster) {
            lqs.createOrUpdateLink(linkingId, scoresAsEsidToEkids)
        } else {
            logger.debug("Writing ${toAdd.size} new links")
            logger.debug("Removing ${toRemove.size} old links")

            if (toAdd.isNotEmpty()) {
                lqs.createLinks(linkingId, toAdd)
            }
            if (toRemove.isNotEmpty()) {
                lqs.tombstoneLinks(linkingId, toRemove)
            }
        }
        linkingLogService.createOrUpdateCluster(linkingId, scoresAsEsidToEkids, newCluster)
    }

    private fun clearNeighborhoods(candidate: EntityDataKey) {
        logger.debug("Starting neighborhood cleanup of {}", candidate)
        val positiveFeedbacks = linkingFeedbackService.getLinkingFeedbackEntityKeyPairs(
                FeedbackType.Positive, candidate
        )

        val clearedCount = lqs.deleteNeighborhood(candidate, positiveFeedbacks)
        logger.debug("Cleared {} neighbors from neighborhood of {}", clearedCount, candidate)
    }

    private fun <T> collectKeys(m: Map<EntityDataKey, Map<EntityDataKey, T>>): Set<EntityDataKey> {
        return m.keys + m.values.flatMap { it.keys }
    }

    private fun completeLinkCluster(matchedCluster: Map<EntityDataKey, Map<EntityDataKey, Double>>): Double {
        return matchedCluster.values.flatMap { it.values }.min() ?: 0.0
    }

    data class ScoredCluster(
            val clusterId: UUID,
            val cluster: Map<EntityDataKey, Map<EntityDataKey, Double>>,
            val score: Double
    ) : Comparable<Double> {
        override fun compareTo(other: Double): Int {
            return score.compareTo(other)
        }
    }
}
