package com.morpheusdata.digitalocean.cloud.sync

import com.morpheusdata.core.MorpheusAsyncServices
import com.morpheusdata.core.MorpheusComputeServerService
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.digitalocean.DigitalOceanApiService
import com.morpheusdata.digitalocean.DigitalOceanPlugin
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.CloudPool
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.ComputeServerType
import com.morpheusdata.model.ServicePlan
import com.morpheusdata.model.projection.CloudPoolIdentity
import io.reactivex.rxjava3.core.Single
import spock.lang.Specification

class VirtualMachineSyncSpec extends Specification {

	void "updateMatchedVirtualMachines moves sshHost to the public ip when the server was first synced with only a private ip"() {
		given:
		MorpheusContext context = Mock(MorpheusContext)
		MorpheusAsyncServices asyncServices = Mock(MorpheusAsyncServices)
		MorpheusComputeServerService computeServerService = Mock(MorpheusComputeServerService)
		context.getAsync() >> asyncServices
		asyncServices.getComputeServer() >> computeServerService
		DigitalOceanPlugin plugin = Mock(DigitalOceanPlugin)
		plugin.getMorpheusContext() >> context
		Cloud cloud = new Cloud(id: 1l)
		def sync = new VirtualMachineSync(plugin, cloud, Mock(DigitalOceanApiService), null)
		sync.@zonePools = ['vpc1': new CloudPool(externalId: 'vpc1')]
		sync.@servicePlans = ['other': new ServicePlan()]
		ComputeServer server = new ComputeServer(id: 5l, name: 'srv', cloud: cloud, status: 'running', computeServerType: new ComputeServerType(code: 'x'))
		def droplet = { List networks -> [name: 'srv', status: 'active', vpc_uuid: 'vpc1', memory: 2048, disk: 60, size_slug: 's-2vcpu-2gb', networks: [v4: networks]] }
		def sync1 = { Map item -> sync.updateMatchedVirtualMachines([new SyncTask.UpdateItem<ComputeServer, Map>(existingItem: server, masterItem: item)]) }

		when:
		sync1(droplet([[ip_address: '10.116.32.6', type: 'private']]))
		sync1(droplet([[ip_address: '10.116.32.6', type: 'private'], [ip_address: '142.93.243.111', type: 'public']]))

		then:
		2 * computeServerService.bulkSave(_) >> Single.just(true)
		server.externalIp == '142.93.243.111'
		server.internalIp == '10.116.32.6'
		server.sshHost == '142.93.243.111'
	}
}
