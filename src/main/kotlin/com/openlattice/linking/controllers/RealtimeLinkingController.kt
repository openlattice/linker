package com.openlattice.linking.controllers

import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.datastore.services.EdmManager
import com.openlattice.linking.*
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import retrofit2.http.Body
import java.util.*
import javax.inject.Inject
import kotlin.collections.HashMap
import kotlin.collections.HashSet

@RestController
@RequestMapping(RealtimeLinkingApi.CONTROLLER)
class RealtimeLinkingController(
        lc: LinkingConfiguration,
        edm: EdmManager
) : RealtimeLinkingApi, AuthorizingComponent {
    companion object {
        private val logger = LoggerFactory.getLogger(RealtimeLinkingController::class.java)
    }
    @Inject
    private lateinit var lqs: LinkingQueryService

    @Inject
    private lateinit var authz: AuthorizationManager

    private val entitySetBlacklist = lc.blacklist
    private val whitelist = lc.whitelist
    private val linkableTypes = edm.getEntityTypeUuids(lc.entityTypes)

    override fun getAuthorizationManager(): AuthorizationManager {
        return authz
    }

    @SuppressFBWarnings(
            value = ["RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE"],
            justification = "lateinit prevents NPE here"
    )
    @RequestMapping(
            path = [RealtimeLinkingApi.FINISHED + RealtimeLinkingApi.SET],
            method = [RequestMethod.GET],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getLinkingFinishedEntitySets(): Set<UUID> {
        ensureAdminAccess()
        val linkableEntitySets = lqs
                .getLinkableEntitySets(linkableTypes, entitySetBlacklist, whitelist.orElse(setOf()))
                .toSet()
        val entitySetsNeedLinking = lqs.getEntitiesNotLinked(linkableEntitySets).map { it.first }
        return linkableEntitySets.minus(entitySetsNeedLinking)
    }

    @RequestMapping(
            path = [RealtimeLinkingApi.MISSING],
            method = [RequestMethod.POST],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )

    override fun getEntitySetNotYetLinkedCount(@RequestBody entitySetIds: Set<UUID>): Map<UUID, Long> {
        ensureAdminAccess()
        val linkableEntitySets = lqs
                .getLinkableEntitySets(linkableTypes, entitySetBlacklist, whitelist.orElse(setOf()))
                .toSet()
        val linkableRequestedEntitySets = linkableEntitySets
                .intersect(entitySetIds)

        return lqs
                .getEntitySetLinkingCount(linkableRequestedEntitySets)
                .toMap()
    }

    @RequestMapping(
            path = [RealtimeLinkingApi.MATCHED + RealtimeLinkingApi.LINKING_ID_PATH],
            method = [RequestMethod.GET],
            produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getMatchedEntitiesForLinkingId(
            @PathVariable(RealtimeLinkingApi.LINKING_ID) linkingId: UUID
    ): Set<MatchedEntityPair> {
        ensureAdminAccess()

        val matches = lqs.getClusterFromLinkingId( linkingId )
        val matchedEntityPairs = HashSet<MatchedEntityPair>()

        matches.forEach {
            val first = it
            first.value.forEach {
                matchedEntityPairs.add(MatchedEntityPair(EntityKeyPair(first.key, it.key), it.value))
            }
        }

        return matchedEntityPairs
    }
}