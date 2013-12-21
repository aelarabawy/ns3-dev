/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
/*
 * Copyright (c) 2013 Ahmed ElArabawy <aelarabawy.git@lasilka.com>
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation;
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */

 /*
  * This class constitutes the fat tree Network object
  * Author: Ahmed ElArabawy <aelarabawy.git@lasilka.com>
  * 
 */

/*
 * A Fat tree Network (K) is composed of K pods 
 * Each POD is two layers,
 * 1. Aggregation layer (top)
 *    K/2 Switches each have K ports
 *    K/2 Ports connected to edge switches in the same pod
 *    and K/2 ports connected to core switches on top of it
 *    This means total of k * k/2 = k * k /2 aggregate switches
 * 2. Edge layer (bottom)
 *    K/2 Switches each having K ports
 *    K/2 Ports connected to aggregation switches in the same pod
 *    K/2 Ports connected to hosts below it
 *    This means total of k * k/2 = k * k /2 edge switches
 * The PODs are interconnected through a layer of Core switches
 *     i. Each Core swithch have K Ports, each connected to one of the pods (one of the ports of an  
 *        Aggregate switch within the pod
 *     ii. This means that the number of core switches = k pods * k/2 agg switches * k/2 ports/K ports
 *         This is k*k/4 switches
 * The edge switches are connected to the hosts.
 *     i. This means number of hosts per pod = k/2 * k/2 = k*k/4
 *     ii. This means the total number of hosts in the network is = k*k*k/4
 * 
 * The conclusion is that a fat tree network contains, (K^3)/4 Hosts and 5*k^2/4 Switches(of K ports each)
 */

#include <iostream>
#include <string>

#include "fat-tree.h"
#include "ns3/core-module.h"
#include "ns3/point-to-point-module.h"
#include "ns3/internet-module.h"

using namespace std;

NS_LOG_COMPONENT_DEFINE ("FatTreeNetwork");

namespace ns3 {

NS_OBJECT_ENSURE_REGISTERED (FatTreeNetwork);

TypeId FatTreeNetwork::GetTypeId (void) {
    static TypeId tid = TypeId ("ns3::FatTreeNetwork")
        .SetParent<Object> ()
        .AddConstructor<FatTreeNetwork>()

        .AddAttribute ("Size",
                       "The Size of the Fat-Tree Network, representing number of Pods, number of ports per switch",
                       UintegerValue(4),
                       MakeUintegerAccessor(&FatTreeNetwork::m_K),
                       MakeUintegerChecker<unsigned int> ())

       .AddAttribute ("Name",
                      "The Name of the fat Tree Network, this name needs to be unique",
                      StringValue("UnNamed"),
                      MakeStringAccessor(&FatTreeNetwork::m_networkName),
                      MakeStringChecker ())
       
        .AddAttribute ("Core2AggrDelay",
                       "The delay on links between the core switches and the Aggregate switches",
                       TimeValue(Seconds(0)),
                       MakeTimeAccessor(&FatTreeNetwork::m_a2cDelay),
                       MakeTimeChecker(Time(0)))

        .AddAttribute ("Aggr2EdgeDelay",
                       "The delay on links between the Aggregate switches and the Edge switches",
                       TimeValue(Seconds(0)),
                       MakeTimeAccessor(&FatTreeNetwork::m_e2aDelay),
                       MakeTimeChecker(Time(0)))

        .AddAttribute ("Edge2HostDelay",
                       "The delay on links between the Edge switches and the Host Nodes",
                       TimeValue(Seconds(0)),
                       MakeTimeAccessor(&FatTreeNetwork::m_h2eDelay),
                       MakeTimeChecker(Time(0)))

        .AddAttribute ("Core2AggrDataRate",
                       "The data rate of links between the core switches and the Aggregate switches",
                       DataRateValue(DataRate("100Mbps")),
                       MakeDataRateAccessor(&FatTreeNetwork::m_a2cRate),
                       MakeDataRateChecker())

        .AddAttribute ("Aggr2EdgeDataRate",
                       "The data rate of links between the Aggregate switches and the Edge switches",
                       DataRateValue(DataRate("100Mbps")),
                       MakeDataRateAccessor(&FatTreeNetwork::m_e2aRate),
                       MakeDataRateChecker())

        .AddAttribute ("Edge2HostDataRate",
                       "The data rate of links between the Edge switches and the Host Nodes",
                       DataRateValue(DataRate("100Mbps")),
                       MakeDataRateAccessor(&FatTreeNetwork::m_h2eRate),
                       MakeDataRateChecker())
        ;
    return tid;
}

/*Default Constructor*/
FatTreeNetwork::FatTreeNetwork():
    //Size of network
    m_K(4),

    //Name and prefix
    m_networkName ("unNamed"),
    m_prefix ("Names/fatTreeNetwork/unNamed"),

    //Data Rates of links between the layers 
    m_h2eRate(DataRate("100Mbps")),
    m_e2aRate(DataRate("100Mbps")),
    m_a2cRate(DataRate("100Mbps")),

    //Delay of links between the layers
    m_h2eDelay(Time(0)),
    m_e2aDelay(Time(0)),
    m_a2cDelay(Time(0))  {
    
    NS_LOG_FUNCTION(this);
}// Default Constructor

FatTreeNetwork::~FatTreeNetwork() {
    NS_LOG_FUNCTION(this);
    //Do Nothing
}// Desturctor

//Create a Fat-tree Netowrk
void FatTreeNetwork::Build(void) {
    NS_LOG_FUNCTION(this);
    
    unsigned int podCount = m_K;
    unsigned int perDirectionPortCount = m_K/2;
    unsigned int perPodEdgeCount = m_K/2;
    unsigned int perPodAggrCount = m_K/2;
    unsigned int totalEdgeCount  = podCount * perPodEdgeCount;
    unsigned int totalAggrCount  = podCount * perPodAggrCount;
    unsigned int totalCoreCount  = m_K * m_K /4;
    unsigned int perPodHostCount = m_K * m_K /4;
    unsigned int totalHostCount  = m_K * perPodHostCount;
    unsigned int totalNodeCount = totalCoreCount + 
                                  totalAggrCount +
                                  totalEdgeCount +
                                  totalHostCount;

    NS_LOG_LOGIC("Building Network : " << m_networkName);
    
    m_prefix = "Names/FatTreeNetwork/";
    m_prefix += m_networkName;

    NS_LOG_LOGIC("Names will have the Prefix: " << m_prefix);

    NS_LOG_LOGIC("Network contains:" << endl 
                  << "/t" << podCount << "pods" << endl
                  << "/t" << totalCoreCount << " Core Switches" << endl
                  << "/t" << totalEdgeCount << " Edge Switches" << endl
                  << "/t" << totalAggrCount  << " Aggregate Switches" << endl
                  << "/t" << totalHostCount << " Hosts" << endl);

    //Create the different nodes
    NS_LOG_LOGIC ("Creating " << totalNodeCount << " Nodes for the Fat -Tree Network");

    m_allNodes.Create(totalNodeCount);

    //Assign the nodes to their respective containers
    unsigned int nodeIndex = 0;
    string nodeName;
    for (unsigned int podNum = 0; podNum < podCount; ++podNum) {
        //Aggregate Switches
        for (unsigned int aggrNum = 0; aggrNum < perPodAggrCount; ++aggrNum) {

            NS_LOG_LOGIC ("pod:aggr (" << podNum << ":" << aggrNum << ") " 
                          << "Assigning Name: " << GetAggrNodeName(podNum, aggrNum, nodeName) 
                          << " For node with index " << nodeIndex << endl);
            Names::Add(m_prefix, GetAggrNodeName(podNum, aggrNum, nodeName) , m_allNodes.Get(nodeIndex));
            m_aggrSwitches.Add(m_allNodes.Get(nodeIndex++));
        }
        
        //Edge Switches
        for (unsigned int edgeNum = 0; edgeNum < perPodEdgeCount; ++edgeNum) {

            //Name exampe edge_7_4
            NS_LOG_LOGIC ("pod:edge (" << podNum << ":" << edgeNum << ") " 
                          << "Assigning Name: " << GetEdgeNodeName(podNum, edgeNum, nodeName) 
                          << " For node with index " << nodeIndex << endl);

            Names::Add(m_prefix, GetEdgeNodeName(podNum, edgeNum, nodeName) , m_allNodes.Get(nodeIndex));
            m_edgeSwitches.Add(m_allNodes.Get(nodeIndex++));
        }
        
        //Host Nodes
        for (unsigned int hostNum = 0; hostNum < perPodHostCount; ++hostNum) {

            //Name exampe host_7_12
            NS_LOG_LOGIC ("pod:host (" << podNum << ":" << hostNum << ") " 
                          << "Assigning Name: " << GetHostNodeName(podNum, hostNum, nodeName) 
                          << " For node with index " << nodeIndex << endl);

            Names::Add(m_prefix, GetHostNodeName(podNum, hostNum, nodeName) , m_allNodes.Get(nodeIndex));
            m_hosts.Add(m_allNodes.Get(nodeIndex++));
        }
    }
    
    //Core Switches
    for (unsigned int coreNum = 0; coreNum < totalCoreCount; ++coreNum)
    {

        //Name exampe core_7_4
        NS_LOG_LOGIC ("core (" << coreNum << ") " 
                      << "Assigning Name: " << GetCoreNodeName(coreNum, nodeName) 
                      << " For node with index " << nodeIndex << endl);
        
        Names::Add(m_prefix, GetCoreNodeName(coreNum, nodeName) , m_allNodes.Get(nodeIndex));
        m_coreSwitches.Add(m_allNodes.Get(nodeIndex++));
    }
      
    //Make sure we covered all nodes
    if (nodeIndex != totalNodeCount)
    {
        NS_LOG_ERROR("Error... Total NodeCount = " << totalNodeCount << endl
                      << "While #Nodes added = " << "nodeIndex" << endl);
    }
    
       
    /*
     * Note:
     * I will start with basic point2point devices, 
     * later I will switch to more suitable type of devices, maybe csma, or 802.1qbb devices
     */

    //Create Net Devices and assign IP addresses   
    NS_LOG_LOGIC ("Creating connections and devices....");

    //Preparing Helpers that will be used for different loops
    PointToPointHelper point2Point;
      
    string devName;
    
    //Connecting Hosts to Edge Switches
    point2Point.SetDeviceAttribute ("DataRate", DataRateValue(m_h2eRate));
    point2Point.SetChannelAttribute("Delay", TimeValue(m_h2eDelay));

    for (unsigned int podNum = 0; podNum < podCount; ++podNum) {
        for (unsigned int edgeNum =0; edgeNum < perPodEdgeCount; ++edgeNum) {
            for (unsigned int portNum = 0; portNum < perDirectionPortCount; ++portNum) {
                
                NetDeviceContainer devices;
                NodeContainer nodePair;

                NS_LOG_LOGIC("Adding the Edge Switch " << GetEdgeNodeName(podNum, edgeNum, nodeName) << " to the nodePair" << endl);
                nodePair.Add(GetEdgeNodeName(podNum, edgeNum, nodeName));

                NS_LOG_LOGIC("Adding the Host Node " << GetHostNodeName(podNum, portNum, nodeName) << " to the nodePair" << endl);                
                nodePair.Add(GetHostNodeName(podNum, portNum + perDirectionPortCount * edgeNum, nodeName));

                devices = point2Point.Install (nodePair);
                m_allDevices.Add(devices);
                
                //Name the associated devices
                //Device Name = dev_<from_Node>_<to_Node>
                //So, if the device is on host_3_1 connected to edge_3_2
                //the device name will be dev_host_3_1_edge_3_2
                //Note that the pod number is repeated in each node

                SetDeviceNames (devices, nodePair);
            }
        }
    }
    
    //Connecting Edge Switches to Aggregate Switches
    point2Point.SetDeviceAttribute ("DataRate", DataRateValue(m_e2aRate));
    point2Point.SetChannelAttribute("Delay", TimeValue(m_e2aDelay));

    for (unsigned int podNum = 0; podNum < podCount; ++podNum) {
        for (unsigned int aggrNum = 0; aggrNum < perPodAggrCount; ++aggrNum) {
            for (unsigned int edgeNum = 0; edgeNum < perPodEdgeCount; ++edgeNum) {
                
                NetDeviceContainer devices;
                NodeContainer nodePair;
                
                NS_LOG_LOGIC("Adding the Aggr Switch " << GetAggrNodeName(podNum, aggrNum, nodeName) << " to the nodePair" << endl);
                nodePair.Add(GetAggrNodeName(podNum, aggrNum, nodeName));

                NS_LOG_LOGIC("Adding the Edge Switch " << GetEdgeNodeName(podNum, edgeNum, nodeName) << " to the nodePair" << endl);
                nodePair.Add(GetEdgeNodeName(podNum, edgeNum, nodeName));

                devices = point2Point.Install (nodePair);
                m_allDevices.Add(devices);
                
                //Name the associated devices
                SetDeviceNames (devices, nodePair);
            }
        }
    }
    
    //Connecting Core Switches to Aggregate Switches
    point2Point.SetDeviceAttribute ("DataRate", DataRateValue(m_a2cRate));
    point2Point.SetChannelAttribute("Delay", TimeValue(m_a2cDelay));
    
    for (unsigned int coreNum = 0; coreNum < totalCoreCount; ++coreNum) {
        for (unsigned int podNum = 0; podNum < podCount; ++podNum) {

            unsigned int aggrNum = coreNum / perDirectionPortCount;
            
            NetDeviceContainer devices;
            NodeContainer nodePair;
                
            NS_LOG_LOGIC("Adding the Core Switch " << GetEdgeNodeName(podNum, coreNum, nodeName) << " to the nodePair" << endl);
            nodePair.Add(GetCoreNodeName(coreNum, nodeName));
                
            NS_LOG_LOGIC("Adding the Aggr Switch " << GetAggrNodeName(podNum, aggrNum, nodeName) << " to the nodePair" << endl);
            nodePair.Add(GetAggrNodeName(podNum, aggrNum, nodeName));
            
            devices = point2Point.Install (nodePair);
            m_allDevices.Add(devices);
            
            //Name the associated devices
            SetDeviceNames (devices, nodePair);
        }
    }

    //Install Stack on all Nodes   
    NS_LOG_LOGIC ("Installing TCP/IP stack in all nodes....");

    InternetStackHelper stack;
    stack .Install (m_allNodes);
    
    //Assign IP Addresses on all Devices
    NS_LOG_LOGIC("Assigning IP Addresses for all Devices");
    Ipv4AddressHelper address;
    address.SetBase ("10.0.0.0", "255.0.0.0");
    Ipv4InterfaceContainer interfaces = address.Assign (m_allDevices);                               
    

    //Build Routing tables in all nodes
    NS_LOG_LOGIC("Build Routing Table in all Nodes");
    Ipv4GlobalRoutingHelper globalRoutingHelper;
    Ipv4GlobalRoutingHelper::PopulateRoutingTables ();
    cout << "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++" <<endl;
    
    Ptr<OutputStreamWrapper> routingTable = Create<OutputStreamWrapper> ("routingTable", std::ios::out);
    globalRoutingHelper.PrintRoutingTableAllAt(Seconds(0), routingTable);
    cout << "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++" << endl;
    
#if 1
    //Dumping Device Info for debugging purpose
    //TODO I need to get rid of the cout and use only NS_LOG
    
    Ptr<OutputStreamWrapper> devInfo = Create<OutputStreamWrapper> ("devInfo.txt", std::ios::out);
    for (unsigned int i = 0; i < m_allDevices.GetN(); ++i) {
        *devInfo->GetStream() << "Device #" << i << ": Name: " << Names::FindName(m_allDevices.Get(i)) << ": IP-Address: " ;
        GetIpAddressForDevice(m_allDevices.Get(i)).Print(*devInfo->GetStream());
        *devInfo->GetStream() << endl;
    }
    
    Ptr<OutputStreamWrapper> nodeInfo = Create<OutputStreamWrapper> ("nodeInfo.txt", std::ios::out);
    for (unsigned int i = 0; i < m_allNodes.GetN(); ++i) {
            *nodeInfo->GetStream() << "Node #" << i << ": Name: " << Names::FindName(m_allNodes.Get(i)) << endl ;
    }
#endif 
    
}// Build()

//TODO This function will be moved later outside this file. I made sure it is not a class member
template<typename T>
string numberToString (T number) {
    ostringstream ss;
    ss << number;
    return ss.str();
}

//Private functions that construct the names for the nodes
string& FatTreeNetwork::GetEdgeNodeName(unsigned int podNum, unsigned int nodeNum, string &nodeName) {
    nodeName = "edge_";
    nodeName += numberToString(podNum) + "_" + numberToString(nodeNum);
    
    return nodeName;
}

string& FatTreeNetwork::GetAggrNodeName(unsigned int podNum, unsigned int nodeNum, string &nodeName) {
    nodeName = "aggr_";
    nodeName += numberToString(podNum) + "_" + numberToString(nodeNum);
    
    return nodeName;
}

string& FatTreeNetwork::GetCoreNodeName(unsigned int nodeNum, string& nodeName) {
    nodeName = "core_";
    nodeName += numberToString(nodeNum);
    
    return nodeName;
}

string& FatTreeNetwork::GetHostNodeName(unsigned int podNum, unsigned int nodeNum, string &nodeName) {
    nodeName = "host_";
    nodeName += numberToString(podNum) + "_" + numberToString(nodeNum);
    
    return nodeName;
}

void FatTreeNetwork::SetDeviceNames (NetDeviceContainer& devices,
                                    NodeContainer& nodePair) {
    string devName;
                   
    NS_LOG_LOGIC("Adding a name to the Dev: " << GetDevName(nodePair.Get(0), nodePair.Get(1), devName) << endl);       
    Names::Add(m_prefix,
               GetDevName(nodePair.Get(0),
                          nodePair.Get(1),
                          devName),
                          devices.Get(0));

    NS_LOG_LOGIC("Adding a name to the Dev: " << GetDevName(nodePair.Get(1), nodePair.Get(0), devName) << endl);
    Names::Add(m_prefix,
               GetDevName(nodePair.Get(1),
                          nodePair.Get(0),
                          devName),
                          devices.Get(1));    
}

string& FatTreeNetwork::GetDevName (Ptr<Node> nodeFrom,
                                   Ptr<Node> nodeTo,
                                   string& devName) {
        devName = "dev_";
        devName += Names::FindName(nodeFrom);
        devName += "_";
        devName += Names::FindName(nodeTo);

        return devName;
}

string& FatTreeNetwork::GetHostDevName (unsigned int podNum,
                                       unsigned int edgeNum,
                                       unsigned int hostNum,
                                       string& devName)      {
    devName  = "dev_host_";
    devName += numberToString(podNum);
    devName += "_";
    devName += numberToString(hostNum);
    devName += "_edge_";
    devName += numberToString(podNum);
    devName += "_";
    devName += numberToString(edgeNum);
    
    return devName;
}


Ipv4Address FatTreeNetwork::GetHostIpAddress (Ptr<Node> node) {
    NS_LOG_FUNCTION(this);
    
    Ptr<Ipv4> ipv4 = node->GetObject<Ipv4> ();
    int32_t interface = ipv4->GetInterfaceForDevice(node->GetDevice(0));    
    
    return ipv4->GetAddress(interface, 0).GetLocal();   
}

Ipv4Address FatTreeNetwork::GetHostIpAddress (unsigned int podNum, unsigned int hostNum) {
    
    NS_LOG_FUNCTION(this);
    
    //Get Associated Edge Switch
    unsigned int perDirectionPortCount = m_K/2;
    unsigned int edgeNum = hostNum/perDirectionPortCount;

    string devName;

    //Find the device using its name
    Ptr<NetDevice> dev = Names::Find<NetDevice>(m_prefix, GetHostDevName(podNum,edgeNum,hostNum,devName));
    
    //return the IP address for that device 
    return GetIpAddressForDevice(dev);
}

Ipv4Address FatTreeNetwork::GetIpAddressForDevice (Ptr<NetDevice> dev) {
    NS_LOG_FUNCTION(dev);

    Ptr<Node> node = dev->GetNode();
    Ptr<Ipv4> ipv4 = node->GetObject<Ipv4> ();
    int32_t interface = ipv4->GetInterfaceForDevice(dev);   
    
    return ipv4->GetAddress(interface, 0).GetLocal();
}

void FatTreeNetwork::AssignIpAddr(unsigned int baseAddr) {
    /*
	 * 
	 * The formula is as follows. there are six categories:
	 * (1) on host towards edge
	 * (2) edge towards host
	 * (3) edge towards aggr
	 * (4) aggr towards edge
	 * (5) aggr towards core
	 * (6) on core towards aggr
	 * 
	 * Each Category will have K^3/4 devices (IP Addresses)
	 * Total IP Addresses of 3 K^3/2 
	 * We will use a network of subnet mask 255.0.0.0, which means,
	 * we will need to get only the first byte and the rest is used
	 * within the network
	 * 
	 * Hierarchy of Addresses as follows,
	 *
	 * Address         Scheme
	 *               | 7 bit   | 1 bit |  6 bit   | 2 bit | 8 bit    |
	 * Host (to edge)| Pod Num |   0   | Edge Num |  00   | Host Num |
	 * Edge (to host)| Pod Num |   0   | Edge Num |  10   | Host Num |
	 * Edge (to aggr)| Pod Num |   0   | Edge Num |  11   | Aggr Num |
	 * Agg. (to edge)| Pod Num |   0   | Edge Num |  01   | Aggr Num |
	 *
	 * Address         Scheme
	 *               | 7 bit   | 1 bit | 2 bit |  6 bit   | 8 bit    |
	 * Agg. (to core)| Pod Num |   1   |  00   | Aggr Num | Core Num |
	 * Core (to aggr)| Pod Num |   1   |  01   | Core Num | Aggr Num |
	 *
	 * All Num's starts with zeroes from the left, and local to the pod
	*/
	for (unsigned int i = 0; i < m_allDevices.GetN(); ++i) {
		string str1("");
		string str2("");
		string tokens [10];
		unsigned int index = 0;
		unsigned int offset = 0;
		unsigned int podNum, hostNum, edgeNum, aggrNum, coreNum;
		unsigned char ipAddr[4];
		
		ipAddr[0] = baseAddr;
		
		string devName = Names::FindName(m_allDevices.Get(i));
		string myString(devName);
		
		//Now we tokenize the string to its components
		offset = myString.find("_");
		while (offset != string::npos) {
			tokens[index++] = myString.substr(0,offset);
			myString = myString.substr(offset+1);
			
			offset = myString.find("_");	
		}
		
		//Now we start to understand the device Name
		if ((index < 6) || (tokens[0] != "dev")) {
			NS_LOG_ERROR("Invalid Device Name: " << devName);
			continue;	
		}
		
        if (tokens[1] == "host") {
        	//Category 1 Host to Edge
        	podNum  = atoi(tokens[2].c_str());
        	hostNum = atoi(tokens[3].c_str());
        	edgeNum = atoi(tokens[6].c_str());
        	
        	ipAddr[1] = (podNum  << 1);
        	ipAddr[2] = (edgeNum << 2);
        	ipAddr[3] = hostNum;
        } 
        else if (tokens[1] == "edge") {
        	if (tokens[4] == "host") {
          	    //Category 2 Edge to Host
        	    podNum  = atoi(tokens[2].c_str());
        	    edgeNum = atoi(tokens[3].c_str());
        	    hostNum = atoi(tokens[6].c_str());
        	
        	    ipAddr[1] = (podNum << 1);
        	    ipAddr[2] = 2 + (edgeNum << 2);
        	    ipAddr[3] = hostNum;
        	}
        	else {
        		//Category 3 Edge to Aggr
        	    podNum  = atoi(tokens[2].c_str());
        	    edgeNum = atoi(tokens[3].c_str());
        	    aggrNum = atoi(tokens[6].c_str());
        	
        	    ipAddr[1] = (podNum << 1);
        	    ipAddr[2] = 3 + (edgeNum << 2);
        	    ipAddr[3] = aggrNum;
        	}
        }
        else if (tokens[1] == "aggr") {
        	if (tokens[4] == "edge") {
          	    //Category 4 Aggr to Edge
        	    podNum  = atoi(tokens[2].c_str());
        	    aggrNum = atoi(tokens[3].c_str());
        	    edgeNum = atoi(tokens[6].c_str());
        	
        	    ipAddr[1] = (podNum << 1);
        	    ipAddr[2] = 1 + (edgeNum << 2);
        	    ipAddr[3] = aggrNum;
        	}
        	else {
        		//Category 5 Aggr to Core
        	    podNum  = atoi(tokens[2].c_str());
        	    aggrNum = atoi(tokens[3].c_str());
        	    coreNum = atoi(tokens[5].c_str());
        	
        	    ipAddr[1] = 1 + (podNum << 1);
        	    ipAddr[2] = aggrNum;
        	    ipAddr[3] = coreNum;
        	}
        }
        else {
        	//Category 6 Core to Aggr
    	    coreNum  = atoi(tokens[2].c_str());
    	    podNum   = atoi(tokens[4].c_str());
    	    aggrNum  = atoi(tokens[5].c_str());
    	
    	    ipAddr[1] = 1 + (podNum << 1);
    	    ipAddr[2] = coreNum + 64;
    	    ipAddr[3] = aggrNum;
        }
        
        //Now the full IP address
        unsigned int fullIpAddress = ipAddr[4] + (ipAddr[3] << 8) + (ipAddr[2] << 16) + (ipAddr[1] << 24);
        
        AssignIpAddr(m_allDevices.Get(i), fullIpAddress);
	}		     	
}


void FatTreeNetwork::AssignIpAddr(Ptr<NetDevice> dev, unsigned int  address) {
        Ipv4AddressHelper addressHelper;
        unsigned int baseIpAddress = address & 0xFF000000;
        addressHelper.SetBase (Ipv4Address(baseIpAddress), "255.0.0.0",Ipv4Address(address));
	NetDeviceContainer devContainer;
        devContainer.Add(dev);

        addressHelper.Assign (dev);
    }

}; //namespace ns3

