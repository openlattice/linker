package com.openlattice.linking.controllers

import com.hazelcast.core.HazelcastInstance
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.data.EntityDataKey
import com.openlattice.datastore.services.EdmManager
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.linking.*
import com.openlattice.linking.util.PersonProperties
import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.*
import javax.inject.Inject

@RestController
@RequestMapping(LinkingFeedbackApi.CONTROLLER)
class LinkingFeedbackController
@Inject
constructor(
        hazelcastInstance: HazelcastInstance,
        private val authorizationManager: AuthorizationManager,
        private val feedbackQueryService: PostgresLinkingFeedbackQueryService,
        private val matcher: Matcher,
        private val dataLoader: DataLoader,
        private val edm: EdmManager
) : LinkingFeedbackApi, AuthorizingComponent {

    private val linkingFeedbacks = hazelcastInstance
            .getMap<EntityKeyPair, EntityLinkingFeedback>(HazelcastMap.LINKING_FEEDBACKS.name)
    private val personPropertyIds = PersonProperties.FQNS.map { edm.getPropertyTypeId(it) }

    companion object {
        private val logger = LoggerFactory.getLogger(LinkingFeedbackController::class.java)
    }

    @PutMapping(path = [LinkingFeedbackApi.FEEDBACK])
    override fun addLinkingFeedback(@RequestBody feedback: LinkingFeedback): Int {
        // ensure read access on linking entity set
        ensureReadAccess(AclKey(feedback.linkingEntityDataKey.entitySetId))

        // ensure read access on all entity sets involved and the properties used for linking
        linkingFeedbackAccessCheck(feedback.linkingEntities)
        linkingFeedbackAccessCheck(feedback.nonLinkingEntities)


        // add feedbacks
        val linkingEntitiesList = feedback.linkingEntities.toList()
        val nonLinkingEntitiesList = feedback.nonLinkingEntities.toList()

        var positiveFeedbackCount = 0
        var negativeFeedbackCount = 0


        feedback.linkingEntities.forEachIndexed { index, linkingEntity ->
            // generate pairs between linking entities themselves
            positiveFeedbackCount += createLinkingFeedbackCombinations(
                    linkingEntity, linkingEntitiesList, index + 1, true)

            // generate pairs between linking and non-linking entities
            negativeFeedbackCount += createLinkingFeedbackCombinations(
                    linkingEntity, nonLinkingEntitiesList, 0, false)
        }


        logger.info("Submitted $positiveFeedbackCount positive and $negativeFeedbackCount negative feedbacks for " +
                "linking id ${feedback.linkingEntityDataKey.entityKeyId}")

        return positiveFeedbackCount + negativeFeedbackCount
    }

    private fun linkingFeedbackAccessCheck(entityDataKeys: Set<EntityDataKey>) {
        entityDataKeys.forEach { entityDataKey ->
            ensureReadAccess(AclKey(entityDataKey.entitySetId))
            personPropertyIds.forEach { ensureReadAccess(AclKey(entityDataKey.entitySetId, it)) }
        }
    }

    private fun createLinkingFeedbackCombinations(
            entityDataKey: EntityDataKey, entityList: List<EntityDataKey>, offset: Int, linked: Boolean): Int {
        return (offset until entityList.size).map{
            addLinkingFeedback(EntityLinkingFeedback(EntityKeyPair(entityDataKey, entityList[it]), linked))
        }.sum()
    }

    private fun addLinkingFeedback(entityLinkingFeedback: EntityLinkingFeedback): Int {
        logger.info("Linking feedback submitted for entities: ${entityLinkingFeedback.entityPair.getFirst()} - " +
                "${entityLinkingFeedback.entityPair.getSecond()}, linked = ${entityLinkingFeedback.linked}")
        linkingFeedbacks.set(entityLinkingFeedback.entityPair, entityLinkingFeedback)

        return feedbackQueryService.addLinkingFeedback(entityLinkingFeedback)
    }

    @GetMapping(path = [LinkingFeedbackApi.FEEDBACK + LinkingFeedbackApi.ALL])
    override fun getAllLinkingFeedbacks(): Iterable<EntityLinkingFeedback> {
        return feedbackQueryService.getLinkingFeedbacks()
    }

    @GetMapping(path = [LinkingFeedbackApi.FEEDBACK + LinkingFeedbackApi.ALL + LinkingFeedbackApi.FEATURES])
    override fun getAllLinkingFeedbacksWithFeatures(): Iterable<EntityLinkingFeatures> {
        return feedbackQueryService.getLinkingFeedbacks().map {
            val entities = dataLoader.getEntities(setOf(it.entityPair.getFirst(), it.entityPair.getSecond()))
            EntityLinkingFeatures(
                    it,
                    matcher.extractFeatures(
                            matcher.extractProperties(entities.getValue(it.entityPair.getFirst())),
                            matcher.extractProperties(entities.getValue(it.entityPair.getSecond()))))
        }
    }

    @PostMapping(path = [LinkingFeedbackApi.FEEDBACK + LinkingFeedbackApi.FEATURES])
    override fun getLinkingFeedbackWithFeatures(
            @RequestBody entityPair: EntityKeyPair): EntityLinkingFeatures? {
        val feedback = checkNotNull(feedbackQueryService.getLinkingFeedback(entityPair))
        { "Linking feedback for entities ${entityPair.getFirst()} - ${entityPair.getSecond()} does not exist" }

        val entities = dataLoader.getEntities(setOf(feedback.entityPair.getFirst(), feedback.entityPair.getSecond()))
        return EntityLinkingFeatures(
                feedback,
                matcher.extractFeatures(
                        matcher.extractProperties(entities.getValue(feedback.entityPair.getFirst())),
                        matcher.extractProperties(entities.getValue(feedback.entityPair.getSecond()))))
    }


    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }
}