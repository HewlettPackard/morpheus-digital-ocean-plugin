package com.morpheusdata.digitalocean.provisioning

import com.morpheusdata.digitalocean.DigitalOceanPlugin
import com.morpheusdata.core.cloud.MorpheusCloudService
import com.morpheusdata.core.MorpheusAsyncServices
import com.morpheusdata.core.MorpheusComputeServerService
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.MorpheusVirtualImageService
import com.morpheusdata.core.provisioning.MorpheusProvisionService
import com.morpheusdata.digitalocean.DigitalOceanApiService
import com.morpheusdata.core.Plugin
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.CloudPool
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.ComputeServerType
import com.morpheusdata.model.Instance
import com.morpheusdata.model.KeyPair
import com.morpheusdata.model.ServicePlan
import com.morpheusdata.model.provisioning.UsersConfiguration
import com.morpheusdata.model.Workload
import com.morpheusdata.model.provisioning.WorkloadRequest
import com.morpheusdata.request.ResizeRequest
import com.morpheusdata.response.ServiceResponse
import groovy.json.JsonSlurper
import io.reactivex.rxjava3.core.Maybe
import io.reactivex.rxjava3.core.Single
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Subject

class DigitalOceanProvisionProviderSpec extends Specification {

	@Subject
	DigitalOceanProvisionProvider provider
	@Shared
	DigitalOceanApiService apiService
	@Shared
	MorpheusContext context
	@Shared
	MorpheusCloudService cloudContext
	@Shared
	MorpheusVirtualImageService virtualImageContext
	MorpheusProvisionService provisionService
	MorpheusComputeServerService computeServerContext

	def setup() {
		Plugin plugin = new DigitalOceanPlugin()
		context = Mock(MorpheusContext)
		cloudContext = Mock(MorpheusCloudService)
		virtualImageContext = Mock(MorpheusVirtualImageService)
		provisionService = Mock(MorpheusProvisionService)
		computeServerContext = Mock(MorpheusComputeServerService)
		context.getComputeServer() >> computeServerContext
		context.getCloud() >> cloudContext
		context.getVirtualImage() >> virtualImageContext
		context.getProvision() >> provisionService
		MorpheusAsyncServices asyncServices = Mock(MorpheusAsyncServices)
		context.getAsync() >> asyncServices
		asyncServices.getCloud() >> cloudContext
		provider = new DigitalOceanProvisionProvider(plugin, context)
		apiService = Mock(DigitalOceanApiService)
	}

	void "startWorkload"() {
		given:
		Cloud cloud = new Cloud(name: 'Digital Ocean', configMap: [apiKey: 'abc123'])
		Workload workload = new Workload()
		workload.server = new ComputeServer(name: 'serv1', externalId: 'drop1111', cloud: cloud)

		when:
		def resp = provider.startWorkload(workload, apiService)

		then:
		1 * apiService.performDropletAction('abc123', 'drop1111', 'power_on') >> new ServiceResponse(success: true, data: actionSuccessJson('power_on').action)
		resp.success == true
		resp.data.id == 1092647540
	}

	void "startWorkload - no droplet id"() {
		given:
		Cloud cloud = new Cloud(name: 'Digital Ocean', configMap: [apiKey: 'abc123'])
		Workload workload = new Workload()
		workload.server = new ComputeServer(name: 'serv1', externalId: null, cloud: cloud)

		when:
		def resp = provider.startWorkload(workload, apiService)

		then:
		0 * apiService.performDropletAction(_, _, _)
		resp.success == false
	}

	void "startWorkload - fail"() {
		given:
		String responseContent = """
{"action":{"id":1092647540,"status":"in-progress","type":"power_on","started_at":"2020-12-16T18:06:52Z","completed_at":null,"resource_id":221818430,"resource_type":"droplet","region":{"name":"New York 1","slug":"nyc1","features":["backups","ipv6","metadata","install_agent","storage","image_transfer"],"available":true,"sizes":["s-1vcpu-1gb","512mb","s-1vcpu-2gb","1gb","s-3vcpu-1gb","s-2vcpu-2gb","s-1vcpu-3gb","s-2vcpu-4gb","2gb","s-4vcpu-8gb","m-1vcpu-8gb","c-2","4gb","c2-2vcpu-4gb","g-2vcpu-8gb","gd-2vcpu-8gb","m-16gb","s-8vcpu-16gb","m-2vcpu-16gb","s-6vcpu-16gb","c-4","8gb","c2-4vpcu-8gb","m3-2vcpu-16gb","g-4vcpu-16gb","so-2vcpu-16gb","m6-2vcpu-16gb","gd-4vcpu-16gb","m-32gb","so1_5-2vcpu-16gb","m-4vcpu-32gb","s-8vcpu-32gb","c-8","c2-8vpcu-16gb","m3-4vcpu-32gb","g-8vcpu-32gb","s-12vcpu-48gb","so-4vcpu-32gb","m6-4vcpu-32gb","gd-8vcpu-32gb","m-64gb","so1_5-4vcpu-32gb","m-8vcpu-64gb","s-16vcpu-64gb","c-16","32gb","c2-16vcpu-32gb","m3-8vcpu-64gb","g-16vcpu-64gb","s-20vcpu-96gb","48gb","so-8vcpu-64gb","m6-8vcpu-64gb","gd-16vcpu-64gb","m-128gb","so1_5-8vcpu-64gb","m-16vcpu-128gb","s-24vcpu-128gb","c-32","64gb","c2-32vpcu-64gb","m3-16vcpu-128gb","m-24vcpu-192gb","g-32vcpu-128gb","s-32vcpu-192gb","so-16vcpu-128gb","m6-16vcpu-128gb","gd-32vcpu-128gb","m3-24vcpu-192gb","g-40vcpu-160gb","so1_5-16vcpu-128gb","gd-40vcpu-160gb","so-24vcpu-192gb","m6-24vcpu-192gb","m3-32vcpu-256gb","so1_5-24vcpu-192gb","so-32vcpu-256gb","m6-32vcpu-256gb","so1_5-32vcpu-256gb"]},"region_slug":"nyc1"}}
"""
		JsonSlurper slurper = new JsonSlurper()
		def json = slurper.parseText(responseContent)
		Cloud cloud = new Cloud(name: 'Digital Ocean', configMap: [apiKey: 'abc123'])
		Workload workload = new Workload()
		workload.server = new ComputeServer(name: 'serv1', externalId: 'drop1111', cloud: cloud)

		when:
		def resp = provider.startWorkload(workload, apiService)

		then:
		1 * apiService.performDropletAction(_, _, _) >> new ServiceResponse(success: false)
		resp.success == false
	}

	void "stopWorkload"() {
		given:
		Cloud cloud = new Cloud(name: 'Digital Ocean', configMap: [apiKey: 'abc123'])
		Workload workload = new Workload()
		workload.server = new ComputeServer(name: 'serv1', externalId: 'drop1111', cloud: cloud)

		when:
		def resp = provider.stopWorkload(workload, apiService)

		then:
		1 * apiService.performDropletAction('abc123', 'drop1111', 'shutdown') >> new ServiceResponse(success: true, data: [id: 1092647540])
		1 * apiService.checkActionComplete('abc123', '1092647540') >> new ServiceResponse(success: true, data: actionSuccessJson('shutdown').action)
		resp.success == true
		resp.data.id == 1092647540
	}

	void "stopWorkload - no droplet id"() {
		given:
		Cloud cloud = new Cloud(name: 'Digital Ocean', configMap: [apiKey: 'abc123'])
		Workload workload = new Workload()
		workload.server = new ComputeServer(name: 'serv1', externalId: null, cloud: cloud)

		when:
		def resp = provider.stopWorkload(workload, apiService)

		then:
		0 * apiService.makeApiCall(_, _)
		resp.success == false
		resp.msg == 'No Droplet ID provided'
	}

	void "stopWorkload and power off"() {
		given:
		Cloud cloud = new Cloud(name: 'Digital Ocean', configMap: [apiKey: 'abc123'])
		Workload workload = new Workload()
		workload.server = new ComputeServer(name: 'serv1', externalId: 'drop1111', cloud: cloud)

		when:
		def resp = provider.stopWorkload(workload, apiService)

		then:
		1 * apiService.performDropletAction('abc123', 'drop1111', 'shutdown') >> new ServiceResponse(success: false)
		1 * apiService.performDropletAction('abc123', 'drop1111', 'power_off') >> new ServiceResponse(success: true, data: actionSuccessJson('power_off').action)
		resp.success == true
		resp.data.id == 1092647540
	}

	void "stopWorkload and power off - fail"() {
		given:
		Cloud cloud = new Cloud(name: 'Digital Ocean', configMap: [apiKey: 'abc123'])
		Workload workload = new Workload()
		workload.server = new ComputeServer(name: 'serv1', externalId: 'drop1111', cloud: cloud)

		when:
		def resp = provider.stopWorkload(workload, apiService)

		then:
		1 * apiService.performDropletAction('abc123', 'drop1111', 'shutdown') >> new ServiceResponse(success: false)
		1 * apiService.performDropletAction('abc123', 'drop1111', 'power_off') >> new ServiceResponse(success: false)
		resp.success == false
	}

	void "runWorkload"() {
		given:
		Cloud cloud = new Cloud(name: 'Digital Ocean', configMap: [apiKey: 'abc123'])
		ServicePlan plan = new ServicePlan(externalId: 'plan1')
		Workload workload = new Workload(plan: plan)
		WorkloadRequest workloadRequest = new WorkloadRequest()
		workload.server = new ComputeServer(name: 'serv1', externalId: 'drop1111', cloud: cloud, resourcePool: new CloudPool(externalId: 'vpc1'))
		Map serverOpts = [
				'name'             : 'droplet1',
				'datacenterName'   : 'nyc1',
				'sizeRef'          : 's-1vcpu-1gb',
				'imageRef'         : 'ubuntu-16-04-x64',
				'sshKeys'          : [12345],
				'doBackups'        : false,
				'ipv6'             : "true",
				'userData'         : null,
				'privateNetworking': "true"
		]
		String createServerResponse = """{
  "droplet": {
    "id": 3164494,
    "name": "example.com",
    "memory": 1024,
    "vcpus": 1,
    "disk": 25,
    "locked": true,
    "status": "new"
    }
}"""
		JsonSlurper slurper = new JsonSlurper()
		def createServerJson = slurper.parseText(createServerResponse)

		when:
		def resp = provider.runWorkload(workload, workloadRequest, serverOpts, apiService)

		then:
		2 * computeServerContext.save(*_) >> Single.just(true)
		1 * apiService.createDroplet('abc123', _) >> new ServiceResponse(success: true, data: createServerJson.droplet)
		1 * cloudContext.findOrGenerateKeyPair(_) >> Single.just(new KeyPair(id: 789, externalId: 'key1'))
		1 * provisionService.getUserConfig(_,_,_) >> Single.just(new UsersConfiguration())
		1 * cloudContext.getComputeServerTypes(_) >> Single.just([new ComputeServerType(code: 'digitalOceanVm')])
		1 * computeServerContext.get(_) >> Maybe.just(workload.server)

		resp.success == true
		resp.data.externalId == "3164494"
	}

	void "runWorkload - fail"() {
		given:
		Cloud cloud = new Cloud(name: 'Digital Ocean', configMap: [apiKey: 'abc123'])
		ServicePlan plan = new ServicePlan(externalId: 'plan1')
		Workload workload = new Workload(plan: plan)
		WorkloadRequest workloadRequest = new WorkloadRequest()
		workload.server = new ComputeServer(name: 'serv1', externalId: 'drop1111', cloud: cloud, resourcePool: new CloudPool(externalId: 'vpc1'))
		Map serverOpts = [:]

		when:
		def resp = provider.runWorkload(workload, workloadRequest, serverOpts, apiService)

		then:
		1 * computeServerContext.save(*_) >> Single.just(true)
		1 * provisionService.getUserConfig(_,_,_) >> Single.just(new UsersConfiguration())
		1 * apiService.createDroplet('abc123', _) >> new ServiceResponse(success: false, errorCode: '400', results: [:])
		1 * cloudContext.findOrGenerateKeyPair(_) >> Single.just(new KeyPair(id: 789, externalId: 'key1'))
		resp.success == false
		resp.msg == 'API error: 400'
	}

	void "runWorkload - missing apiKey"() {
		given:
		Cloud cloud = new Cloud(name: 'Digital Ocean', configMap: [:])
		Workload workload = new Workload()
		WorkloadRequest workloadRequest = new WorkloadRequest()
		workload.server = new ComputeServer(name: 'serv1', externalId: 'drop1111', cloud: cloud)
		Map serverOpts = [:]

		when:
		def resp = provider.runWorkload(workload, workloadRequest, serverOpts, apiService)

		then:
		0 * apiService.makeApiCall(_, _)
		0 * cloudContext.findOrGenerateKeyPair(_)
		resp.success == false
		resp.msg == 'No API Key provided'
	}

	void "resizeServer"() {
		given:
		Cloud cloud = new Cloud(name: 'Digital Ocean', configMap: [apiKey: 'abc123'])
		Workload workload = new Workload()
		ComputeServer server = new ComputeServer(name: 'serv1', externalId: 'drop1111', cloud: cloud)
		workload.server = server
		ServicePlan plan = new ServicePlan(externalId: 'plan_123')
		Instance instance = new Instance(id: 42)
		ResizeRequest resizeRequest = new ResizeRequest()
		resizeRequest.plan = plan
		Map opts = [:]

		when:
		def resp = provider.resizeWorkload(instance, workload, resizeRequest, opts, apiService)

		then:
		resp.success
		1 * apiService.performDropletAction('abc123', 'drop1111', 'resize', [disk: true, size: 'plan_123']) >> new ServiceResponse(success: true, data: actionSuccessJson('resize').action)
	}

	def actionSuccessJson(String type) {
		String responseContent = """
{"action":{"id":1092647540,"status":"completed","type":"$type","started_at":"2020-12-16T18:06:52Z","completed_at":"2020-12-16T18:07:52Z","resource_id":221818430,"resource_type":"droplet","region":{"name":"New York 1","slug":"nyc1","features":["backups","ipv6","metadata","install_agent","storage","image_transfer"],"available":true,"sizes":["s-1vcpu-1gb","512mb","s-1vcpu-2gb","1gb","s-3vcpu-1gb","s-2vcpu-2gb","s-1vcpu-3gb","s-2vcpu-4gb","2gb","s-4vcpu-8gb","m-1vcpu-8gb","c-2","4gb","c2-2vcpu-4gb","g-2vcpu-8gb","gd-2vcpu-8gb","m-16gb","s-8vcpu-16gb","m-2vcpu-16gb","s-6vcpu-16gb","c-4","8gb","c2-4vpcu-8gb","m3-2vcpu-16gb","g-4vcpu-16gb","so-2vcpu-16gb","m6-2vcpu-16gb","gd-4vcpu-16gb","m-32gb","so1_5-2vcpu-16gb","m-4vcpu-32gb","s-8vcpu-32gb","c-8","c2-8vpcu-16gb","m3-4vcpu-32gb","g-8vcpu-32gb","s-12vcpu-48gb","so-4vcpu-32gb","m6-4vcpu-32gb","gd-8vcpu-32gb","m-64gb","so1_5-4vcpu-32gb","m-8vcpu-64gb","s-16vcpu-64gb","c-16","32gb","c2-16vcpu-32gb","m3-8vcpu-64gb","g-16vcpu-64gb","s-20vcpu-96gb","48gb","so-8vcpu-64gb","m6-8vcpu-64gb","gd-16vcpu-64gb","m-128gb","so1_5-8vcpu-64gb","m-16vcpu-128gb","s-24vcpu-128gb","c-32","64gb","c2-32vpcu-64gb","m3-16vcpu-128gb","m-24vcpu-192gb","g-32vcpu-128gb","s-32vcpu-192gb","so-16vcpu-128gb","m6-16vcpu-128gb","gd-32vcpu-128gb","m3-24vcpu-192gb","g-40vcpu-160gb","so1_5-16vcpu-128gb","gd-40vcpu-160gb","so-24vcpu-192gb","m6-24vcpu-192gb","m3-32vcpu-256gb","so1_5-24vcpu-192gb","so-32vcpu-256gb","m6-32vcpu-256gb","so1_5-32vcpu-256gb"]},"region_slug":"nyc1"}}
"""
		JsonSlurper slurper = new JsonSlurper()
		slurper.parseText(responseContent)
	}

	def actionInProgressJson(String type) {
		String responseContent = """
{"action":{"id":1092647540,"status":"in-progress","type":"$type","started_at":"2020-12-16T18:06:52Z","completed_at":null,"resource_id":221818430,"resource_type":"droplet","region":{"name":"New York 1","slug":"nyc1","features":["backups","ipv6","metadata","install_agent","storage","image_transfer"],"available":true,"sizes":["s-1vcpu-1gb","512mb","s-1vcpu-2gb","1gb","s-3vcpu-1gb","s-2vcpu-2gb","s-1vcpu-3gb","s-2vcpu-4gb","2gb","s-4vcpu-8gb","m-1vcpu-8gb","c-2","4gb","c2-2vcpu-4gb","g-2vcpu-8gb","gd-2vcpu-8gb","m-16gb","s-8vcpu-16gb","m-2vcpu-16gb","s-6vcpu-16gb","c-4","8gb","c2-4vpcu-8gb","m3-2vcpu-16gb","g-4vcpu-16gb","so-2vcpu-16gb","m6-2vcpu-16gb","gd-4vcpu-16gb","m-32gb","so1_5-2vcpu-16gb","m-4vcpu-32gb","s-8vcpu-32gb","c-8","c2-8vpcu-16gb","m3-4vcpu-32gb","g-8vcpu-32gb","s-12vcpu-48gb","so-4vcpu-32gb","m6-4vcpu-32gb","gd-8vcpu-32gb","m-64gb","so1_5-4vcpu-32gb","m-8vcpu-64gb","s-16vcpu-64gb","c-16","32gb","c2-16vcpu-32gb","m3-8vcpu-64gb","g-16vcpu-64gb","s-20vcpu-96gb","48gb","so-8vcpu-64gb","m6-8vcpu-64gb","gd-16vcpu-64gb","m-128gb","so1_5-8vcpu-64gb","m-16vcpu-128gb","s-24vcpu-128gb","c-32","64gb","c2-32vpcu-64gb","m3-16vcpu-128gb","m-24vcpu-192gb","g-32vcpu-128gb","s-32vcpu-192gb","so-16vcpu-128gb","m6-16vcpu-128gb","gd-32vcpu-128gb","m3-24vcpu-192gb","g-40vcpu-160gb","so1_5-16vcpu-128gb","gd-40vcpu-160gb","so-24vcpu-192gb","m6-24vcpu-192gb","m3-32vcpu-256gb","so1_5-24vcpu-192gb","so-32vcpu-256gb","m6-32vcpu-256gb","so1_5-32vcpu-256gb"]},"region_slug":"nyc1"}}
"""
		JsonSlurper slurper = new JsonSlurper()
		slurper.parseText(responseContent)
	}

	void "startServer"() {
		given:
		Cloud cloud = new Cloud(name: 'Digital Ocean', configMap: [apiKey: 'abc123'])
		ComputeServer computeServer = new ComputeServer(name: 'serv1', externalId: 'drop1111', cloud: cloud)

		when:
		def resp = provider.startServer(computeServer, apiService)

		then:
		1 * apiService.performDropletAction('abc123', 'drop1111', 'power_on') >> new ServiceResponse(success: true, data: actionSuccessJson('power_on').action)
		resp.success == true
	}

	void "startServer - no droplet id"() {
		given:
		Cloud cloud = new Cloud(name: 'Digital Ocean', configMap: [apiKey: 'abc123'])
		ComputeServer computeServer = new ComputeServer(name: 'serv1', externalId: null, cloud: cloud)

		when:
		def resp = provider.startServer(computeServer, apiService)

		then:
		0 * apiService.performDropletAction(_, _, _)
		resp.success == false
	}

	void "stopServer"() {
		given:
		Cloud cloud = new Cloud(name: 'Digital Ocean', configMap: [apiKey: 'abc123'])
		ComputeServer computeServer = new ComputeServer(name: 'serv1', externalId: 'drop1111', cloud: cloud)

		when:
		def resp = provider.stopServer(computeServer, apiService)

		then:
		1 * apiService.performDropletAction('abc123', 'drop1111', 'shutdown') >> new ServiceResponse(success: true, data: actionSuccessJson('shutdown').action)
		resp.success == true
	}

	void "stopServer - no droplet id"() {
		given:
		Cloud cloud = new Cloud(name: 'Digital Ocean', configMap: [apiKey: 'abc123'])
		ComputeServer computeServer = new ComputeServer(name: 'serv1', externalId: null, cloud: cloud)

		when:
		def resp = provider.stopServer(computeServer, apiService)

		then:
		0 * apiService.performDropletAction(_, _, _)
		resp.success == false
	}

	void "getServerDetails - timeout reports a reason"() {
		given:
		Cloud cloud = new Cloud(name: 'Digital Ocean', configMap: [apiKey: 'abc123'])
		ComputeServer computeServer = new ComputeServer(id: 1, name: 'serv1', externalId: 'drop1111', cloud: cloud)
		provider.serverPollIntervalMs = 0l
		provider.serverPollMaxAttempts = 3

		when:
		def resp = provider.getServerDetails(computeServer, apiService)

		then:
		3 * apiService.getDroplet('abc123', 'drop1111') >> new ServiceResponse(success: true, data: [id: 'drop1111', status: 'new'])
		resp.success == false
		resp.msg.contains('Timed out')
		resp.msg.contains('drop1111')
	}

	void "getServerDetails - errored droplet reports status"() {
		given:
		Cloud cloud = new Cloud(name: 'Digital Ocean', configMap: [apiKey: 'abc123'])
		ComputeServer computeServer = new ComputeServer(id: 1, name: 'serv1', externalId: 'drop1111', cloud: cloud)
		provider.serverPollIntervalMs = 0l

		when:
		def resp = provider.getServerDetails(computeServer, apiService)

		then:
		1 * apiService.getDroplet('abc123', 'drop1111') >> new ServiceResponse(success: true, data: [id: 'drop1111', status: 'errored'])
		resp.success == false
		resp.msg.contains('errored')
	}

	void "finalizeWorkload - failure includes a message"() {
		given:
		Cloud cloud = new Cloud(name: 'Digital Ocean', configMap: [apiKey: 'abc123'])
		ComputeServer computeServer = new ComputeServer(id: 1, name: 'serv1', externalId: 'drop1111', cloud: cloud)
		Workload workload = new Workload(id: 2, server: computeServer)
		provider.serverPollIntervalMs = 0l
		provider.serverPollMaxAttempts = 1

		when:
		def resp = provider.finalizeWorkload(workload)

		then:
		1 * apiService.getDroplet('abc123', 'drop1111') >> new ServiceResponse(success: false, msg: 'rate limited')
		resp.success == false
		resp.msg.contains('rate limited')
	}

	void "optionTypes"() {
		when:
		def options = provider.optionTypes

		then:
		options.size() == 1
	}

	void "dropletToProvisionResponse"() {
		given:
		def droplet = [
				id      : '1111',
				networks: [
						v4: [
								[ip_address: '10.10.10.10', type: 'public'],
								[ip_address: '192.168.0.10', type: 'private'],
						]
				]
		]

		when:
		def resp = provider.dropletToProvisionResponse(droplet)

		then:
		resp.externalId == '1111'
		resp.publicIp == '10.10.10.10'
		resp.privateIp == '192.168.0.10'
	}

	void "getServerDetails waits for the public ip when the active droplet only has a private ip"() {
		given:
		provider = new DigitalOceanProvisionProvider(new DigitalOceanPlugin(), context) {
			@Override
			protected Long getPollIntervalMs() { 0l }
		}
		ComputeServer server = new ComputeServer(id: 1l, externalId: 'drop1111', cloud: new Cloud(configMap: [apiKey: 'abc123']))
		def privateOnly = [id: 'drop1111', status: 'active', networks: [v4: [[ip_address: '10.116.32.6', type: 'private']]]]
		def noNetworks = [id: 'drop1111', status: 'active', networks: [v4: []]]
		def complete = [id: 'drop1111', status: 'active', networks: [v4: [
			[ip_address: '10.116.32.6', type: 'private'], [ip_address: '142.93.243.111', type: 'public']]]]

		when:
		def resp = provider.getServerDetails(server, apiService)

		then:
		3 * apiService.getDroplet('abc123', 'drop1111') >>> [
			new ServiceResponse(success: true, data: noNetworks),
			new ServiceResponse(success: true, data: privateOnly),
			new ServiceResponse(success: true, data: complete)
		]
		resp.success == true
		resp.data.publicIp == '142.93.243.111'
		resp.data.privateIp == '10.116.32.6'
		resp.msg == null
	}

	void "getServerDetails fails when the active droplet never gets a public ip"() {
		given:
		provider = new DigitalOceanProvisionProvider(new DigitalOceanPlugin(), context) {
			@Override
			protected Long getPollIntervalMs() { 0l }
		}
		ComputeServer server = new ComputeServer(id: 1l, externalId: 'drop1111', cloud: new Cloud(configMap: [apiKey: 'abc123']))
		def privateOnly = [id: 'drop1111', status: 'active', networks: [v4: [[ip_address: '10.116.32.6', type: 'private']]]]

		when:
		def resp = provider.getServerDetails(server, apiService)

		then:
		_ * apiService.getDroplet('abc123', 'drop1111') >> new ServiceResponse(success: true, data: privateOnly)
		resp.success == false
	}

	void "finalizeWorkload saves the public and private ips"() {
		given:
		provider = new DigitalOceanProvisionProvider(new DigitalOceanPlugin(), context) {
			@Override
			ServiceResponse getServerDetails(ComputeServer server, DigitalOceanApiService api) {
				new ServiceResponse(success: true, data: new com.morpheusdata.response.ProvisionResponse(publicIp: '142.93.243.111', privateIp: '10.116.32.6'))
			}
		}
		ComputeServer server = new ComputeServer(id: 1l, cloud: new Cloud())
		Workload workload = new Workload(server: server)

		when:
		def resp = provider.finalizeWorkload(workload)

		then:
		1 * computeServerContext.save([server]) >> Single.just(true)
		resp.success == true
		server.externalIp == '142.93.243.111'
		server.internalIp == '10.116.32.6'
	}

}
