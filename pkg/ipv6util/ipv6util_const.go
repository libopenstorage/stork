package ipv6util

const (
	sampleIpv6PxctlStatusOutput = `
Status: PX is operational
Telemetry: Disabled or Unhealthy
License: Trial
Node ID: f703597a-9772-4bdb-b630-6395b3c98658
	IP: 0000:111:2222:3333:444:5555:6666:666
 	Local Storage Pool: 1 pool
	POOL	IO_PRIORITY	RAID_LEVEL	USABLE	USED	STATUS	ZONE	REGION
	0	HIGH		raid0		100 GiB	6.2 GiB	Online	default	default
	Local Storage Devices: 1 device
	Device	Path		Media Type		Size		Last-Scan
	0:1	/dev/sdb	STORAGE_MEDIUM_SSD	100 GiB		some time
	* Internal kvdb on this node is sharing this storage device /dev/sdb  to store its data.
	total		-	100 GiB
	Cache Devices:
	 * No cache devices
Cluster Summary
	Cluster ID: px-cluster-2c8df3fc-a2b9-4c31-8b9d-5fddcb4646e1
	Cluster UUID: f2c71ae5-c076-4e33-be1c-001c0d558274
	Scheduler: kubernetes
	Nodes: 6 node(s) with storage (6 online)
	IP					ID					SchedulerNodeName	Auth		StorageNode	Used	Capacity	Status	StorageStatus	Version		Kernel			OS
	0000:111:2222:3333:444:5555:6666:666	f703597a-9772-4bdb-b630-6395b3c98658	node05			Disabled	Yes		6.2 GiB	100 GiB		Online	Up (This node)	2.11.0-5fbb8c2	3.10.0-1160.53.1.el7.x86_64	CentOS Linux 7 (Core)
	0000:111:2222:3333:444:5555:6666:555	cedc897f-a489-4c28-9c20-12b8b4c3d1d8	node01			Disabled	Yes		6.7 GiB	100 GiB		Online	Up		2.11.0-5fbb8c2	3.10.0-1160.53.1.el7.x86_64	CentOS Linux 7 (Core)
	0000:111:2222:3333:444:5555:6666:444	956aafc1-a52d-41f3-afb1-6427e2a3b0ef	node04			Disabled	Yes		6.3 GiB	100 GiB		Online	Up		2.11.0-5fbb8c2	3.10.0-1160.53.1.el7.x86_64	CentOS Linux 7 (Core)
	0000:111:2222:3333:444:5555:6666:333	6d801e0f-a7e7-4063-8f2f-50b43c1d9608	node03			Disabled	Yes		6.6 GiB	100 GiB		Online	Up		2.11.0-5fbb8c2	3.10.0-1160.53.1.el7.x86_64	CentOS Linux 7 (Core)
	0000:111:2222:3333:444:5555:6666:222	28dee5d4-7724-41eb-a86d-929a3f88456e	node06			Disabled	Yes		6.3 GiB	100 GiB		Online	Up		2.11.0-5fbb8c2	3.10.0-1160.53.1.el7.x86_64	CentOS Linux 7 (Core)
	0000:111:2222:3333:444:5555:6666:111	0e88d11f-6fb1-4898-b76a-e38c200fa7ae	node02			Disabled	Yes		6.1 GiB	100 GiB		Online	Up		2.11.0-5fbb8c2	3.10.0-1160.53.1.el7.x86_64	CentOS Linux 7 (Core)
	Warnings:
		 WARNING: Internal Kvdb is not using dedicated drive on nodes [0000:111:2222:3333:444:5555:6666:666 0000:111:2222:3333:444:5555:6666:444 0000:111:2222:3333:444:5555:6666:333]. This configuration is not recommended for production clusters.
Global Storage Pool
	Total Used    	:  38 GiB
	Total Capacity	:  600 GiB
`
	sampleIpv4PxctlStatusOutput = `
Status: PX is operational
Telemetry: Disabled or Unhealthy
License: Trial
Node ID: f703597a-9772-4bdb-b630-6395b3c98658
	IP: 192.168.121.111
 	Local Storage Pool: 1 pool
	POOL	IO_PRIORITY	RAID_LEVEL	USABLE	USED	STATUS	ZONE	REGION
	0	HIGH		raid0		100 GiB	6.2 GiB	Online	default	default
	Local Storage Devices: 1 device
	Device	Path		Media Type		Size		Last-Scan
	0:1	/dev/sdb	STORAGE_MEDIUM_SSD	100 GiB		some time
	* Internal kvdb on this node is sharing this storage device /dev/sdb  to store its data.
	total		-	100 GiB
	Cache Devices:
	 * No cache devices
Cluster Summary
	Cluster ID: px-cluster-2c8df3fc-a2b9-4c31-8b9d-5fddcb4646e1
	Cluster UUID: f2c71ae5-c076-4e33-be1c-001c0d558274
	Scheduler: kubernetes
	Nodes: 6 node(s) with storage (6 online)
	IP					ID					SchedulerNodeName	Auth		StorageNode	Used	Capacity	Status	StorageStatus	Version		Kernel			OS
	192.168.121.111	f703597a-9772-4bdb-b630-6395b3c98658	node05			Disabled	Yes		6.2 GiB	100 GiB		Online	Up (This node)	2.11.0-5fbb8c2	3.10.0-1160.53.1.el7.x86_64	CentOS Linux 7 (Core)
	192.168.121.222	cedc897f-a489-4c28-9c20-12b8b4c3d1d8	node01			Disabled	Yes		6.7 GiB	100 GiB		Online	Up		2.11.0-5fbb8c2	3.10.0-1160.53.1.el7.x86_64	CentOS Linux 7 (Core)
	192.168.121.333	956aafc1-a52d-41f3-afb1-6427e2a3b0ef	node04			Disabled	Yes		6.3 GiB	100 GiB		Online	Up		2.11.0-5fbb8c2	3.10.0-1160.53.1.el7.x86_64	CentOS Linux 7 (Core)
	192.168.121.444	6d801e0f-a7e7-4063-8f2f-50b43c1d9608	node03			Disabled	Yes		6.6 GiB	100 GiB		Online	Up		2.11.0-5fbb8c2	3.10.0-1160.53.1.el7.x86_64	CentOS Linux 7 (Core)
	192.168.121.555	28dee5d4-7724-41eb-a86d-929a3f88456e	node06			Disabled	Yes		6.3 GiB	100 GiB		Online	Up		2.11.0-5fbb8c2	3.10.0-1160.53.1.el7.x86_64	CentOS Linux 7 (Core)
	192.168.121.666	0e88d11f-6fb1-4898-b76a-e38c200fa7ae	node02			Disabled	Yes		6.1 GiB	100 GiB		Online	Up		2.11.0-5fbb8c2	3.10.0-1160.53.1.el7.x86_64	CentOS Linux 7 (Core)
Global Storage Pool
	Total Used    	:  38 GiB
	Total Capacity	:  600 GiB
`

	sampleIpv6PxctlClusterListOutput = `
Cluster ID: px-cluster-2c8df3fc-a2b9-4c31-8b9d-5fddcb4646e1
Cluster UUID: bd0e2e27-5072-4f70-8a3d-5974c1e2119f
Status: OK

Nodes in the cluster:
ID					SCHEDULER_NODE_NAME	DATA IP					CPU		MEM TOTAL	MEM FREE	CONTAINERS	VERSION		Kernel				OS		STATUS
2ca8932b-b17e-425c-bcbe-d33b0f64b623	node03			0000:111:2222:3333:444:5555:6666:111	6.19469		17 GB		14 GB		N/A		2.11.0-5fbb8c2	3.10.0-1160.53.1.el7.x86_64	CentOS Linux 7 (Core)	Online
6b9d12e0-fb28-459e-acf1-cea4d57004e2	node04			0000:111:2222:3333:444:5555:6666:222	12.025316	17 GB		14 GB		N/A		2.11.0-5fbb8c2	3.10.0-1160.53.1.el7.x86_64	CentOS Linux 7 (Core)	Online
c4b514ef-b925-4dff-8ae1-279a84104d7b	node06			0000:111:2222:3333:444:5555:6666:333	46.700508	17 GB		14 GB		N/A		2.11.0-5fbb8c2	3.10.0-1160.53.1.el7.x86_64	CentOS Linux 7 (Core)	Online
1c251a9f-605d-47fc-925d-7c8cb38d8b48	node01			0000:111:2222:3333:444:5555:6666:444	18.781726	17 GB		14 GB		N/A		2.11.0-5fbb8c2	3.10.0-1160.53.1.el7.x86_64	CentOS Linux 7 (Core)	Online
657bbf29-feb8-4119-be9d-d1c14762b5ba	node02			0000:111:2222:3333:444:5555:6666:555	11.223203	17 GB		14 GB		N/A		2.11.0-5fbb8c2	3.10.0-1160.53.1.el7.x86_64	CentOS Linux 7 (Core)	Online
e832ca35-1763-445e-b9fd-0881c1e76356	node05			0000:111:2222:3333:444:5555:6666:666	8.998733	17 GB		14 GB		N/A		2.11.0-5fbb8c2	3.10.0-1160.53.1.el7.x86_64	CentOS Linux 7 (Core)	Online
`

	sampleIpv6PxctlClusterInspectOutput = `
ID            		:  2ca8932b-b17e-425c-bcbe-d33b0f64b623
Scheduler Node Name	:  node03
Mgmt IP       		:  0000:111:2222:3333:444:5555:6666:111
Data IP       		:  0000:111:2222:3333:444:5555:6666:111
CPU           		:  28.498727735368956
Mem Total     		:  16825671680
Mem Used      		:  2389970944
Volumes 		:

Status  		:  Online
`

	sampleIpv6PxctlVolumeListOutput = `
ID			NAME	SIZE	HA	SHARED	ENCRYPTED	PROXY-VOLUME	IO_PRIORITY	STATUS							SNAP-ENABLED
197020883293002044	ipv6-volume	1 GiB	1	no	no		no		LOW		up - attached on 0000:111:2222:3333:444:5555:6666:111	no
`

	sampleIpv6PxctlVolumeInspectOutput = `
	Volume          	 :  197020883293002044
	Name            	 :  test
	Size            	 :  1.0 GiB
	Format          	 :  ext4
	HA              	 :  1
	IO Priority     	 :  LOW
	Creation time   	 :  Apr 20 01:48:59 UTC 2022
	Shared          	 :  no
	Status          	 :  up
	State           	 :  Attached: 1c251a9f-605d-47fc-925d-7c8cb38d8b48 (0000:111:2222:3333:444:5555:6666:111)
	Last Attached   	 :  Apr 20 01:49:26 UTC 2022
	Device Path     	 :  /dev/pxd/pxd197020883293002044
	Mount Options          	 :  discard
	Reads           	 :  43
	Reads MS        	 :  85
	Bytes Read      	 :  1060864
	Writes          	 :  0
	Writes MS       	 :  0
	Bytes Written   	 :  0
	IOs in progress 	 :  0
	Bytes used      	 :  596 KiB
	Replica sets on nodes:
		Set 0
		  Node 		 : 0000:111:2222:3333:444:5555:6666:111 (Pool f54c56c1-eb9e-408b-ac92-010426e59500 )
	Replication Status	 :  Up
`
	sampleNodeCount = 6
)
