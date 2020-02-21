package com.openlattice.linking.controllers

import com.hazelcast.core.HazelcastInstance
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.datastore.services.EdmManager
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.ids.IdCipherManager
import com.openlattice.linking.*
import com.openlattice.linking.RealtimeLinkingApi.*
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RestController
import java.util.*
import javax.inject.Inject

@RestController
@RequestMapping(CONTROLLER)
class RealtimeLinkingController(
        hazelcastInstance: HazelcastInstance,
        lc: LinkingConfiguration,
        edm: EdmManager,
        private val idCipherManager: IdCipherManager
) : RealtimeLinkingApi, AuthorizingComponent {
    @Inject
    private lateinit var lqs: LinkingQueryService

    @Inject
    private lateinit var authz: AuthorizationManager

    private val entitySetBlacklist = lc.blacklist
    private val whitelist = lc.whitelist
    private val linkableTypes = edm.getEntityTypeUuids(lc.entityTypes)
    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)

    override fun getAuthorizationManager(): AuthorizationManager {
        return authz
    }

    @SuppressFBWarnings(
            value = ["RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE"],
            justification = "lateinit prevents NPE here"
    )
    @RequestMapping(
            path = [FINISHED + SET],
            method = [RequestMethod.GET],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getLinkingFinishedEntitySets(): Set<UUID> {
        ensureAdminAccess()

        // TODO do we still use black and white lists?
        val linkableEntitySets = lqs
                .getLinkableEntitySets(linkableTypes, entitySetBlacklist, whitelist.orElse(setOf()))
                .toSet()
        val entitySetsNeedLinking = lqs.getEntitiesNotLinked(linkableEntitySets).map { it.entitySetId }.toSet()
        return linkableEntitySets.minus(entitySetsNeedLinking)
    }

    @RequestMapping(
            path = [MATCHED + LINKING_ENTITY_SET_ID_PATH + LINKING_ID_PATH],
            method = [RequestMethod.GET],
            produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getMatchedEntitiesForLinkingId(
            @PathVariable(LINKING_ENTITY_SET_ID) linkingEntitySetId: UUID,
            @PathVariable(LINKING_ID) linkingId: UUID
    ): Set<MatchedEntityPair> {
        ensureAdminAccess()

        val linkingEntitySet = entitySets[linkingEntitySetId]
        requireNotNull(linkingEntitySet) { "Entity set with id $linkingEntitySetId does not exist." }
        require(linkingEntitySet.isLinking) { "Entity set with id $linkingEntitySetId is not a linking entity set." }

        // get all matching entities
        val decryptedLinkingId = idCipherManager.decryptId(linkingEntitySetId, linkingId)
        val matches = lqs.getClusterFromLinkingId(decryptedLinkingId)

        // only return entities that are within the linking entity set
        val linkedEntitySetIds = linkingEntitySet.linkedEntitySets
        return matches
                .filter { linkedEntitySetIds.contains(it.key.entitySetId) }
                .flatMap { (srcEntity, dstEntities) ->
                    dstEntities
                            .filter { linkedEntitySetIds.contains(it.key.entitySetId) }
                            .map { (dstEntity, score) ->
                                MatchedEntityPair(EntityKeyPair(srcEntity, dstEntity), score)
                            }
                }.toSet()
    }
}