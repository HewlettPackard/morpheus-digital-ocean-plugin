package com.morpheusdata.digitalocean.cloud.sync

import com.morpheusdata.digitalocean.DigitalOceanApiService
import com.morpheusdata.digitalocean.DigitalOceanPlugin
import com.morpheusdata.digitalocean.cloud.DigitalOceanCloudProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.BulkRemoveResult
import com.morpheusdata.core.MorpheusAsyncServices
import com.morpheusdata.core.MorpheusServicePlanPriceSetService
import com.morpheusdata.core.MorpheusServicePlanService
import com.morpheusdata.core.MorpheusServices
import com.morpheusdata.core.synchronous.MorpheusSynchronousServicePlanPriceSetService
import com.morpheusdata.core.synchronous.MorpheusSynchronousServicePlanService
import com.morpheusdata.model.ServicePlanPriceSet
import com.morpheusdata.core.MorpheusVirtualImageService
import com.morpheusdata.core.Plugin
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ServicePlan
import com.morpheusdata.model.projection.ServicePlanIdentityProjection
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.ObservableEmitter
import io.reactivex.rxjava3.core.ObservableOnSubscribe
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.annotations.NonNull
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Subject

class SizesSyncSpec extends Specification {

	@Subject
	SizesSync service
	@Shared
	DigitalOceanApiService apiService
	@Shared
	MorpheusServicePlanService servicePlanContext

	def setup() {
		Plugin plugin = new DigitalOceanPlugin()
		MorpheusContext context = Mock(MorpheusContext)
		servicePlanContext = Mock(MorpheusServicePlanService)
		context.getServicePlan() >> servicePlanContext
		apiService = Mock(DigitalOceanApiService)
		Cloud cloud = new Cloud(configMap: [doApiKey: 'abc123', doUsername: 'user'])
		service = new SizesSync(plugin, cloud, apiService)
	}

	//TODO: this test doesn't work, needs some love
	@Ignore
	void "cacheSizes"() {
		given:
		ServicePlan updatePlan = new ServicePlan(id: 1, externalId: 'abc123')
		ServicePlan createPlan = new ServicePlan(externalId: 'def567')
		ServicePlan removePlan = new ServicePlan(id: 2, externalId: 'ghi890')
		Observable listFullObjectsObservable = Observable.create(new ObservableOnSubscribe<ServicePlan>() {
			@Override
			void subscribe(@NonNull ObservableEmitter<ServicePlan> emitter) throws Exception {
				try {
					List<ServicePlan> plans = [updatePlan]
					for (plan in plans) {
						emitter.onNext(plan)
					}
					emitter.onComplete()
				} catch (Exception e) {
					emitter.onError(e)
				}
			}
		})

		Observable listSyncProjections = Observable.create(new ObservableOnSubscribe<ServicePlanIdentityProjection>() {
			@Override
			void subscribe(@NonNull ObservableEmitter<ServicePlanIdentityProjection> emitter) throws Exception {
				try {
					List<ServicePlanIdentityProjection> projections = [new ServicePlanIdentityProjection(id: updatePlan.id, externalId: updatePlan.externalId), new ServicePlanIdentityProjection(id: removePlan.id, externalId: removePlan.externalId)]
					for (projection in projections) {
						emitter.onNext(projection)
					}
					emitter.onComplete()
				} catch (Exception e) {
					emitter.onError(e)
				}
			}
		})

		when:
		service.execute()

		then:
		1 * apiService.makeApiCall(_, _) >> [
			json: [
				sizes: [
					[slug: createPlan.externalId, memory: 1024, disk: 25],
					[slug: updatePlan.externalId, memory: 1024, disk: 25]
				]
			]
		]
		1 * servicePlanContext.listById(_) >> listFullObjectsObservable
		1 * servicePlanContext.listSyncProjections(_) >> listSyncProjections
		1 * servicePlanContext.create({ list -> list.size() == 1 && list.first().externalId == createPlan.externalId }) >> Single.just(true)
		1 * servicePlanContext.save([updatePlan]) >> Single.just([updatePlan])
		1 * servicePlanContext.remove({ list -> list.size() == 1 && list.first().externalId == removePlan.externalId })
	}

	void "cleanDuplicates keeps the lowest id per code and deactivates copies that cannot be removed"() {
		given:
		MorpheusContext context = Mock(MorpheusContext)
		MorpheusServices services = Mock(MorpheusServices)
		MorpheusAsyncServices async = Mock(MorpheusAsyncServices)
		MorpheusSynchronousServicePlanService syncPlans = Mock(MorpheusSynchronousServicePlanService)
		MorpheusSynchronousServicePlanPriceSetService syncPlanPriceSets = Mock(MorpheusSynchronousServicePlanPriceSetService)
		MorpheusServicePlanPriceSetService asyncPlanPriceSets = Mock(MorpheusServicePlanPriceSetService)
		context.getServices() >> services
		context.getAsync() >> async
		services.getServicePlan() >> syncPlans
		services.getServicePlanPriceSet() >> syncPlanPriceSets
		async.getServicePlan() >> servicePlanContext
		async.getServicePlanPriceSet() >> asyncPlanPriceSets
		service.@morpheusContext = context

		def keepA = new ServicePlanIdentityProjection(id: 10, code: 'digitalocean.size.a')
		def dupA = new ServicePlanIdentityProjection(id: 20, code: 'digitalocean.size.a')
		def usedA = new ServicePlanIdentityProjection(id: 30, code: 'digitalocean.size.a')
		def keepB = new ServicePlanIdentityProjection(id: 5, code: 'digitalocean.size.b')
		ServicePlanPriceSet link = new ServicePlanPriceSet(id: 1)
		ServicePlan usedPlan = new ServicePlan(id: 30, code: 'digitalocean.size.a', active: true)

		when:
		def keepers = service.cleanDuplicates([usedA, keepA, keepB, dupA])

		then:
		keepers*.id.sort() == [5L, 10L]
		1 * asyncPlanPriceSets.listByServicePlanIds({ it.sort() == [20L, 30L] }) >> Observable.just(link)
		1 * syncPlanPriceSets.bulkRemove([link])
		1 * syncPlans.bulkRemove({ it*.id.sort() == [20L, 30L] }) >> new BulkRemoveResult<ServicePlanIdentityProjection>(null, null, [usedA])
		1 * servicePlanContext.listById([30L]) >> Observable.just(usedPlan)
		1 * syncPlans.bulkSave({ it == [usedPlan] && it.first().active == false })
	}

	void "cleanDuplicates does nothing when there are no duplicates"() {
		given:
		MorpheusContext context = Mock(MorpheusContext)
		service.@morpheusContext = context
		def a = new ServicePlanIdentityProjection(id: 1, code: 'digitalocean.size.a')
		def b = new ServicePlanIdentityProjection(id: 2, code: 'digitalocean.size.b')

		when:
		def keepers = service.cleanDuplicates([a, b])

		then:
		keepers == [a, b]
		0 * context._
	}

	void "cleanDuplicates keeps a live copy over an older soft-deleted copy"() {
		given:
		MorpheusContext context = Mock(MorpheusContext)
		MorpheusServices services = Mock(MorpheusServices)
		MorpheusAsyncServices async = Mock(MorpheusAsyncServices)
		MorpheusSynchronousServicePlanService syncPlans = Mock(MorpheusSynchronousServicePlanService)
		MorpheusServicePlanPriceSetService asyncPlanPriceSets = Mock(MorpheusServicePlanPriceSetService)
		context.getServices() >> services
		context.getAsync() >> async
		services.getServicePlan() >> syncPlans
		async.getServicePlanPriceSet() >> asyncPlanPriceSets
		service.@morpheusContext = context
		def deletedOld = new ServicePlanIdentityProjection(id: 43023, code: 'digitalocean.size.s-2vcpu-8gb-amd', deleted: true)
		def live = new ServicePlanIdentityProjection(id: 43026, code: 'digitalocean.size.s-2vcpu-8gb-amd', deleted: false)

		when:
		def keepers = service.cleanDuplicates([deletedOld, live])

		then:
		keepers*.id == [43026L]
		1 * asyncPlanPriceSets.listByServicePlanIds([43023L]) >> Observable.empty()
		1 * syncPlans.bulkRemove({ it*.id == [43023L] }) >> new BulkRemoveResult<ServicePlanIdentityProjection>(null, null, [])
	}

	void "filterServicePlans only keeps DigitalOcean plans offered in the cloud datacenter"() {
		given:
		DigitalOceanCloudProvider provider = new DigitalOceanCloudProvider(null, null)
		Cloud cloud = new Cloud(configMap: [datacenter: 'nyc1'])
		ServicePlan inRegion = new ServicePlan(code: 'digitalocean.size.amd', subRegionCodes: ['nyc1', 'sfo3'] as Set)
		ServicePlan outOfRegion = new ServicePlan(code: 'digitalocean.size.basic', subRegionCodes: ['sfo3'] as Set)
		ServicePlan unavailable = new ServicePlan(code: 'digitalocean.size.gone', subRegionCodes: [] as Set)
		ServicePlan notSynced = new ServicePlan(code: 'digitalocean.size.new', subRegionCodes: null)
		ServicePlan custom = new ServicePlan(code: 'custom.plan', subRegionCodes: ['ams3'] as Set)

		expect:
		provider.filterServicePlans(cloud, [inRegion, outOfRegion, unavailable, notSynced, custom], null) == [inRegion, notSynced, custom]
		provider.filterServicePlans(new Cloud(configMap: [:]), [outOfRegion], null) == [outOfRegion]
	}

	void "getNameForSize"() {
		expect:
		expected == service.getNameForSize(sizeData)

		where:
		sizeData                                                              | expected
		[description: 'Plugin Droplet', vcpus: 1, memory: 25, disk: 25]      | 'Plugin Droplet 1 CPU, 25 MB Memory, 25 GB Storage'
		[description: 'Plugin Droplet', vcpus: 3, memory: 25 * 1024, disk: 500] | 'Plugin Droplet 3 CPU, 25 GB Memory, 500 GB Storage'
	}
}
