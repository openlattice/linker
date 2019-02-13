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

package com.openlattice.linking.matching

import com.codahale.metrics.annotation.Timed
import com.google.common.base.Stopwatch
import com.openlattice.data.EntityDataKey
import com.openlattice.linking.EntityKeyPair
import com.openlattice.linking.Matcher
import com.openlattice.linking.PostgresLinkingFeedbackService
import com.openlattice.linking.util.PersonMetric
import com.openlattice.rhizome.hazelcast.DelegatedStringSet
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.nd4j.linalg.factory.Nd4j
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.*
import java.util.concurrent.TimeUnit

const val THRESHOLD = 0.9
private val logger = LoggerFactory.getLogger(SocratesMatcher::class.java)

/**
 * Performs matching using the model generated by Socrates.
 */
@Component
class SocratesMatcher(
        model: MultiLayerNetwork,
        private val fqnToIdMap: Map<FullQualifiedName, UUID>,
        private val linkingFeedbackService: PostgresLinkingFeedbackService) : Matcher {

    private var localModel = ThreadLocal.withInitial { model }
    //            Thread.currentThread().contextClassLoader.getResourceAsStream("model.bin") }

    override fun updateMatchingModel(model: MultiLayerNetwork) {
        localModel = ThreadLocal.withInitial { model }
    }

    /**
     * Gets an initial block of entities that closely match the entity.
     * @param block A block of potential matches based on search
     * @return block The resulting block around the entity data key in block.first
     */
    @Timed
    override fun initialize(
            block: Pair<EntityDataKey, Map<EntityDataKey, Map<UUID, Set<Any>>>>
    ): Pair<EntityDataKey, MutableMap<EntityDataKey, MutableMap<EntityDataKey, Double>>> {
        val model = localModel.get()

        val entityDataKey = block.first

        // feedback on entity to itself should always be 1
        val positiveFeedbacks = mutableSetOf(entityDataKey)
        // filter out positive matches from feedback to avoid computation of scores
        // negative feedbacks are already filtered out when blocking
        val entities = block.second
                .filter {
                    if(it.key == entityDataKey) {// is itself
                        return@filter false
                    }

                    val feedback = linkingFeedbackService.getLinkingFeedback(EntityKeyPair(entityDataKey, it.key))
                    if (feedback != null) {
                        if (feedback.linked) {
                            positiveFeedbacks.add(it.key)
                        }
                        !feedback.linked
                    }
                    true
                }

        // extract properties and features for all entities in block
        val firstProperties = extractProperties(entities[entityDataKey]!!)
        val extractedFeatures = entities.mapValues {
            val extractedProperties = extractProperties(it.value)
            extractFeatures(firstProperties, extractedProperties)
        }

        // transform features to matrix and compute scores
        val featureKeys = extractedFeatures.map { it.key }.toTypedArray()
        val featureMatrix = extractedFeatures.map { it.value }.toTypedArray()
        val scores = computeScore(model, featureMatrix).toTypedArray()
        val matchedEntities = featureKeys.zip(scores).toMap().plus(positiveFeedbacks.map { it to 1.0 }).toMutableMap()
        val initializedBlock = entityDataKey to mutableMapOf(entityDataKey to matchedEntities)

        // trim low scores
        trimAndMerge(initializedBlock)
        return initializedBlock
    }

    /**
     * Computes the pairwise matching values for a block.
     * @param block The resulting block around for the entity data key in block.first and property values for each
     * entity around as block.second
     * @return All pairs of entities in the block scored by the current model.
     */
    @Timed
    override fun match(
            block: Pair<EntityDataKey, Map<EntityDataKey, Map<UUID, Set<Any>>>>
    ): Pair<EntityDataKey, MutableMap<EntityDataKey, MutableMap<EntityDataKey, Double>>> {
        val sw = Stopwatch.createStarted()

        val model = localModel.get()
        val entityDataKey = block.first

        // feedback on entity to itself should always be 1
        val positiveFeedbacks = mutableSetOf(EntityKeyPair(entityDataKey, entityDataKey))
        // filter out positive matches from feedback to avoid computation of scores
        // negative feedbacks are already filter out when blocking
        val entities = block.second.mapValues { entity ->
            block.second.keys.filter {
                if (it == entityDataKey && entity.key == entityDataKey) { // is itself
                    return@filter false
                }

                val entityPair = EntityKeyPair(entity.key, it)
                val feedback = linkingFeedbackService.getLinkingFeedback(entityPair)
                if (feedback != null) {
                    if (feedback.linked) {
                        positiveFeedbacks.add(entityPair)
                    }
                    !feedback.linked
                }
                true
            }
        }.filter { !it.value.isEmpty() }


        // extract properties
        val extractedProperties = block.second.map { it.key to extractProperties(it.value) }.toMap()

        // extract features for all entities in block
        val extractedFeatures = entities.mapValues {
            val selfProperties = extractedProperties[it.key]!!
            it.value.map {
                val otherProperties = extractedProperties[it]!!
                it to extractFeatures(selfProperties, otherProperties)
            }.toMap()
        }

        // transform features to matrix and compute scores
        val featureMatrix = extractedFeatures
                .flatMap { (_, features) -> features.map { it.value } }
                .toTypedArray()

        // extract list of keys (instead of map)
        val featureKeys = extractedFeatures.flatMap { (entityDataKey1, features) ->
            features.map {
                entityDataKey1 to it.key
            }
        }

        val featureExtractionSW = sw.elapsed(TimeUnit.MILLISECONDS)

        // get scores from matrix
        val scores = computeScore(model, featureMatrix)

        // collect and combine keys and scores
        val results = scores.zip(featureKeys).map {
            ResultSet(it.second.first, it.second.second, it.first)
        }.plus(positiveFeedbacks.map { ResultSet(it.first, it.second, 1.0) })

        // from list of results to expected output
        val matchedEntities =
                results.groupBy { it.lhs }
                        .mapValues { x ->
                            x.value.groupBy { it.rhs }
                                    .mapValues { x -> x.value[0].score }
                                    .toMutableMap()
                        }.toMutableMap()

        logger.info(
                "Matching block {} with {} elements: feature extraction: {} ms - matching: {} ms - total: {} ms",
                block.first, block.second.values.map { it.size }.sum(),
                featureExtractionSW,
                sw.elapsed(TimeUnit.MILLISECONDS) - featureExtractionSW,
                sw.elapsed(TimeUnit.MILLISECONDS)
        )

        return entityDataKey to matchedEntities

    }

    private fun computeScore(
            model: MultiLayerNetwork, features: Array<DoubleArray>
    ): DoubleArray {
        val sw = Stopwatch.createStarted()
        val scores = model.getModelScore(features)
        logger.info("The model computed scores in {} ms", sw.elapsed(TimeUnit.MILLISECONDS))
        return scores
    }

    override fun extractFeatures(
            lhs: Map<UUID, DelegatedStringSet>, rhs: Map<UUID, DelegatedStringSet>
    ): DoubleArray {
        return PersonMetric.pDistance(lhs, rhs, fqnToIdMap).map { it * 100.0 }.toDoubleArray()
    }

    override fun extractProperties(entity: Map<UUID, Set<Any>>): Map<UUID, DelegatedStringSet> {
        return entity.map { it.key to DelegatedStringSet.wrap(it.value.map(Any::toString).toSet()) }.toMap()
    }

    @Timed
    override fun trimAndMerge(
            matchedBlock: Pair<EntityDataKey, MutableMap<EntityDataKey, MutableMap<EntityDataKey, Double>>>
    ) {
        //Trim non-center matching thigns.
        matchedBlock.second[matchedBlock.first] = matchedBlock.second[matchedBlock.first]?.filter {
            it.value > THRESHOLD
        }?.toMutableMap() ?: mutableMapOf()
    }
}

fun MultiLayerNetwork.getModelScore(features: Array<DoubleArray>): DoubleArray {
    return try {
        output(Nd4j.create(features)).toDoubleVector()
    } catch (ex: Exception) {
        logger.error("Failed to compute model score trying again! Features = {}", features.toList(), ex)
        try {
            output(Nd4j.create(features)).toDoubleVector()
        } catch (ex2: Exception) {
            logger.error("Failed to compute model score a second time! Return 0! Features = {}", features.toList(), ex)
            Nd4j.ones(features.size).toDoubleVector()
        }
    }
}

data class ResultSet(val lhs: EntityDataKey, val rhs: EntityDataKey, val score: Double)
