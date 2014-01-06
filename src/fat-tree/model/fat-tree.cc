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
    
    AssignIpAddrAll(10);

    //Build Routing tables in all nodes
    NS_LOG_LOGIC("Build Routing Table in all Nodes");

#if 1
    InstallStaticRoutingTableAll ();
#else
    Ipv4GlobalRoutingHelper globalRoutingHelper;
    Ipv4GlobalRoutingHelper::PopulateRoutingTables ();
#endif

#if 1 
    Ptr<OutputStreamWrapper> routingTable = Create<OutputStreamWrapper> ("routingTable", std::ios::out);
    //globalRoutingHelper.PrintRoutingTableAllAt(Seconds(0), routingTable);
    Ipv4StaticRoutingHelper staticRoutingHelper;
    staticRoutingHelper.PrintRoutingTableAllAt(Seconds(0), routingTable);
#endif

#if 0
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



string& FatTreeNetwork::EncodeDeviceName (DevDescriptor &desc, string& devName) {
    switch (desc.m_cat) {
        case DEV_CAT_HOST_EDGE:
            devName  = "dev_host_";
            devName += numberToString(desc.m_podId);
            devName += "_";
            devName += numberToString(desc.m_fromNodeId);
            devName += "_edge_";
            devName += numberToString(desc.m_podId);
            devName += "_";
            devName += numberToString(desc.m_toNodeId);
            break;

        case DEV_CAT_EDGE_HOST:
            devName  = "dev_edge_";
            devName += numberToString(desc.m_podId);
            devName += "_";
            devName += numberToString(desc.m_fromNodeId);
            devName += "_host_";
            devName += numberToString(desc.m_podId);
            devName += "_";
            devName += numberToString(desc.m_toNodeId);
            break;
        
        case DEV_CAT_EDGE_AGGR:
            devName  = "dev_edge_";
            devName += numberToString(desc.m_podId);
            devName += "_";
            devName += numberToString(desc.m_fromNodeId);
            devName += "_aggr_";
            devName += numberToString(desc.m_podId);
            devName += "_";
            devName += numberToString(desc.m_toNodeId);
            break;

        case DEV_CAT_AGGR_EDGE:
            devName  = "dev_aggr_";
            devName += numberToString(desc.m_podId);
            devName += "_";
            devName += numberToString(desc.m_fromNodeId);
            devName += "_edge_";
            devName += numberToString(desc.m_podId);
            devName += "_";
            devName += numberToString(desc.m_toNodeId);
            break;

        case DEV_CAT_AGGR_CORE:
            devName  = "dev_aggr_";
            devName += numberToString(desc.m_podId);
            devName += "_";
            devName += numberToString(desc.m_fromNodeId);
            devName += "_core_";
            devName += numberToString(desc.m_toNodeId);
            break;

        case DEV_CAT_CORE_AGGR:
            devName  = "dev_core_";
            devName += numberToString(desc.m_fromNodeId);
            devName += "_aggr_";
            devName += numberToString(desc.m_podId);
            devName += "_";
            devName += numberToString(desc.m_toNodeId);
            break;

        case DEV_CAT_INVALID:
                NS_LOG_ERROR("Invalid Device Category: " << desc.m_cat);
    }

    return devName;
}

string& FatTreeNetwork::EncodeNodeName (NodeDescriptor &desc, string& nodeName) {
    switch (desc.m_cat) {
        case NODE_CAT_HOST:
            nodeName = "host_";
            nodeName += numberToString(desc.m_podId) + "_" + numberToString(desc.m_nodeId);
            break;

        case NODE_CAT_EDGE:
            nodeName = "edge_";
            nodeName += numberToString(desc.m_podId) + "_" + numberToString(desc.m_nodeId);
            break;

        case NODE_CAT_AGGR:
            nodeName = "aggr_";
            nodeName += numberToString(desc.m_podId) + "_" + numberToString(desc.m_nodeId);
            break;

        case NODE_CAT_CORE:
            nodeName = "core_";
            nodeName += numberToString(desc.m_nodeId);
            break;

        default:
            NS_LOG_ERROR("INVALID NODE CATEGORY" << desc.m_cat);
            nodeName = "INVALID";
            break;
    }

    return nodeName;
}

string& FatTreeNetwork::GetHostNodeName(unsigned int podNum, unsigned int nodeNum, string &nodeName) {
    NodeDescriptor desc;
    desc.m_cat = NODE_CAT_HOST;
    desc.m_podId = podNum;
    desc.m_nodeId = nodeNum;
    
    return (EncodeNodeName(desc, nodeName));
}

string& FatTreeNetwork::GetEdgeNodeName(unsigned int podNum, unsigned int nodeNum, string &nodeName) {
    NodeDescriptor desc;
    desc.m_cat = NODE_CAT_EDGE;
    desc.m_podId = podNum;
    desc.m_nodeId = nodeNum;
    
    return (EncodeNodeName(desc, nodeName));
}

string& FatTreeNetwork::GetAggrNodeName(unsigned int podNum, unsigned int nodeNum, string &nodeName) {
    NodeDescriptor desc;
    desc.m_cat = NODE_CAT_AGGR;
    desc.m_podId = podNum;
    desc.m_nodeId = nodeNum;
    
    return (EncodeNodeName(desc, nodeName));
}

string& FatTreeNetwork::GetCoreNodeName(unsigned int nodeNum, string& nodeName) {
    NodeDescriptor desc;
    desc.m_cat = NODE_CAT_CORE;
    desc.m_nodeId = nodeNum;
    
    return (EncodeNodeName(desc, nodeName));
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
    DevDescriptor desc;
    desc.m_cat = DEV_CAT_HOST_EDGE;
    desc.m_podId = podNum;
    desc.m_fromNodeId = hostNum;
    desc.m_toNodeId = edgeNum;

    return (EncodeDeviceName(desc, devName));
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


void FatTreeNetwork::DecodeNodeName (string nodeName, NodeDescriptor& desc) {
#if 0	
    Ptr<OutputStreamWrapper> nodeNameVerifier = Create<OutputStreamWrapper> ("nodeNameVerefier.txt", std::ios::out);
#endif
    string tokens [10];
    unsigned int index = 0;
    size_t offset = 0;

    string myString(nodeName);
		
    //Now we tokenize the string to its components
    offset = myString.find("_");
    while (offset != string::npos) {
        tokens[index++] = myString.substr(0,offset);
        myString = myString.substr(offset+1);

        offset = myString.find("_");	
    }
    tokens[index++] = myString;

#if 0
        *nodeNameVerifier->GetStream() << "Node Name: " << nodeName << "tokens: ";
        for (int tokenId = 0; tokenId < index; ++tokenId) {
            *nodeNameVerifier->GetStream() << "(" << tokenId << ")" << tokens[tokenId] << endl;
        }		
#endif

    //Now we start to understand the node Name
    if ((index < 2) || (index > 3)) {
        NS_LOG_ERROR("Invalid Node Name: " << nodeName);
        desc.m_cat = NODE_CAT_INVALID;
        return;
    }

    if (tokens[0] == "host") {
        desc.m_cat = NODE_CAT_HOST;
        desc.m_podId = atoi(tokens[1].c_str());
        desc.m_nodeId = atoi(tokens[2].c_str());
    } else if (tokens[0] == "edge") {
        desc.m_cat = NODE_CAT_EDGE;
        desc.m_podId = atoi(tokens[1].c_str());
        desc.m_nodeId = atoi(tokens[2].c_str());
    } else if (tokens[0] == "aggr") {
        desc.m_cat = NODE_CAT_AGGR;
        desc.m_podId = atoi(tokens[1].c_str());
        desc.m_nodeId = atoi(tokens[2].c_str());
    } else if (tokens[0] == "core") {
        desc.m_cat = NODE_CAT_CORE;
        desc.m_nodeId = atoi(tokens[2].c_str());
    }
        

#if 0  
        *nodeNameVerifier->GetStream() << "CAT:POD:NODE " << desc.m_cat << ":" << desc.m_podId << ":" << desc.nodeId << endl;
#endif

    return;
}


void FatTreeNetwork::DecodeDeviceName (string devName, DevDescriptor& desc) {
#if 0	
    Ptr<OutputStreamWrapper> devNameVerifier = Create<OutputStreamWrapper> ("devNameVerefier.txt", std::ios::out);
#endif
    string tokens [10];
    unsigned int index = 0;
    size_t offset = 0;

    string myString(devName);
		
    //Now we tokenize the string to its components
    offset = myString.find("_");
    while (offset != string::npos) {
        tokens[index++] = myString.substr(0,offset);
        myString = myString.substr(offset+1);

        offset = myString.find("_");	
    }
    tokens[index++] = myString;

#if 0
        *devNameVerifier->GetStream() << "DevName: " << devName << "tokens: ";
        for (int tokenId = 0; tokenId < 7; ++tokenId) {
            *devNameVerifier->GetStream() << "(" << tokenId << ")" << tokens[tokenId] << endl;
        }		
#endif

    //Now we start to understand the device Name
    if ((index < 6) || (tokens[0] != "dev")) {
        NS_LOG_ERROR("Invalid Device Name: " << devName);
        desc.m_cat = DEV_CAT_INVALID;
        return;	
    }

    if (tokens[1] == "host") {
        //Category 1 Host to Edge
        desc.m_cat = DEV_CAT_HOST_EDGE;
        desc.m_podId = atoi(tokens[2].c_str());
        desc.m_fromNodeId = atoi(tokens[3].c_str());
        desc.m_toNodeId = atoi(tokens[6].c_str());

    } else if (tokens[1] == "edge") {
        if (tokens[4] == "host") {
            //Category 2 Edge to Host
            desc.m_cat = DEV_CAT_EDGE_HOST;
            desc.m_podId = atoi(tokens[2].c_str());
            desc.m_fromNodeId = atoi(tokens[3].c_str());
            desc.m_toNodeId = atoi(tokens[6].c_str());
        } else {
            //Category 3 Edge to Aggr
            desc.m_cat = DEV_CAT_EDGE_AGGR;
            desc.m_podId = atoi(tokens[2].c_str());
            desc.m_fromNodeId = atoi(tokens[3].c_str());
            desc.m_toNodeId = atoi(tokens[6].c_str());
        }
    } else if (tokens[1] == "aggr") {
        if (tokens[4] == "edge") {
            //Category 4 Aggr to Edge
            desc.m_cat = DEV_CAT_AGGR_EDGE;
            desc.m_podId = atoi(tokens[2].c_str());
            desc.m_fromNodeId = atoi(tokens[3].c_str());
            desc.m_toNodeId = atoi(tokens[6].c_str());
        } else {
            //Category 5 Aggr to Core
            desc.m_cat = DEV_CAT_AGGR_CORE;
            desc.m_podId = atoi(tokens[2].c_str());
            desc.m_fromNodeId = atoi(tokens[3].c_str());
            desc.m_toNodeId = atoi(tokens[5].c_str());
        }
    } else {
        //Category 6 Core to Aggr
        desc.m_cat = DEV_CAT_CORE_AGGR;
        desc.m_podId = atoi(tokens[4].c_str());
        desc.m_fromNodeId = atoi(tokens[2].c_str());
        desc.m_toNodeId = atoi(tokens[5].c_str());
    }
        

#if 0  
        *devNameVerifier->GetStream() << "POD:Host:Edge:Aggr:Core = " << desc.m_cat << ":" << desc.m_podId << ":" << desc.m_fromNode << ":" << desc.m_toNode << endl;
#endif

    return;
}

string FatTreeNetwork::GetDevIpAddress(unsigned int baseAddr, DevDescriptor desc ) {
    NS_LOG_FUNCTION (this);

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

    unsigned int ipAddr[4];
    string devIpAddr;
    ipAddr[0] = baseAddr;
        
    switch (desc.m_cat) {
        case DEV_CAT_HOST_EDGE:
            ipAddr[1] = (desc.m_podId << 1);
            ipAddr[2] = (desc.m_toNodeId << 2);
            ipAddr[3] = desc.m_fromNodeId;
            break;

        case DEV_CAT_EDGE_HOST:
            ipAddr[1] = (desc.m_podId << 1);
            ipAddr[2] = 2 + (desc.m_fromNodeId << 2);
            ipAddr[3] = desc.m_toNodeId;
            break;
        
        case DEV_CAT_EDGE_AGGR:
            ipAddr[1] = (desc.m_podId << 1);
            ipAddr[2] = 3 + (desc.m_fromNodeId << 2);
            ipAddr[3] = desc.m_toNodeId;
            break;

        case DEV_CAT_AGGR_EDGE:
            ipAddr[1] = (desc.m_podId << 1);
            ipAddr[2] = 1 + (desc.m_toNodeId << 2);
            ipAddr[3] = desc.m_fromNodeId;
            break;

        case DEV_CAT_AGGR_CORE:
            ipAddr[1] = 1 + (desc.m_podId << 1);
            ipAddr[2] = desc.m_fromNodeId;
            ipAddr[3] = desc.m_toNodeId;
            break;

        case DEV_CAT_CORE_AGGR:
            ipAddr[1] = 1 + (desc.m_podId << 1);
            ipAddr[2] = desc.m_fromNodeId + 64;
            ipAddr[3] = desc.m_toNodeId;
            break;

        case DEV_CAT_INVALID:
            NS_LOG_ERROR("Invalid Device Category");
            devIpAddr = "Invalid";
            return devIpAddr;
    }
        

    devIpAddr = numberToString(ipAddr[0]);
    devIpAddr += '.' + numberToString(ipAddr[1]);
    devIpAddr += '.' + numberToString(ipAddr[2]);
    devIpAddr += '.' + numberToString(ipAddr[3]);

    NS_LOG_LOGIC ("IP ADDRESS : " << devIpAddr);

    return devIpAddr;
}


void FatTreeNetwork::AssignIpAddrAll(unsigned int baseAddr) {
    NS_LOG_FUNCTION(this);

#if 0	
    Ptr<OutputStreamWrapper> devNameVerifier = Create<OutputStreamWrapper> ("devNameVerefier.txt", std::ios::out);
#endif

    for (unsigned int i = 0; i < m_allDevices.GetN(); ++i) {
        DevDescriptor desc;

        string devName = Names::FindName(m_allDevices.Get(i));
        DecodeDeviceName(devName, desc);

        string devIpAddr = GetDevIpAddress (baseAddr, desc);
        
        AssignIpAddr (m_allDevices.Get(i), devIpAddr);
#if 0
        *devNameVerifier->GetStream() << "CAT:POD:FROM:TO = " << desc.m_cat << ":" << desc.m_podId << ":" << desc.m_fromNodeId << ":" << desc.m_toNodeId << endl;
        *devNameVerifier->GetStream() << devIpAddr << endl;
#endif
    }
}


void FatTreeNetwork::AssignIpAddr(Ptr<NetDevice> dev, unsigned int  address) {
    NS_LOG_FUNCTION(this << dev << address );
    Ipv4AddressHelper addressHelper;
    addressHelper.SetBase (Ipv4Address(address & 0xFF000000), "255.0.0.0",Ipv4Address(address & 0x00FFFFFF));
    NetDeviceContainer devContainer;
    devContainer.Add(dev);
        
    addressHelper.Assign (devContainer);
}

void FatTreeNetwork::AssignIpAddr(Ptr<NetDevice> dev, string address) {

    NS_LOG_FUNCTION(this << dev << address);

    unsigned int offset = address.find('.');

    string baseAddress = address.substr(0,offset);
    baseAddress += ".0.0.0";
    
    string remainingAddress = "0";
    remainingAddress += address.substr(offset);
  
    Ipv4AddressHelper addressHelper;
    addressHelper.SetBase(Ipv4Address(baseAddress.data()), "255.0.0.0", Ipv4Address(remainingAddress.data()));
    NetDeviceContainer devContainer;
    devContainer.Add(dev);
    addressHelper.Assign (devContainer);
}



void FatTreeNetwork::InstallStaticRoutingTableAll () {
    /*
     * First routing table for Host-Edge links
     * Each host will have 1 route to the connected edge switch for all traffic
     * And Edge host will forward the host traffic to the proper host
     */
    unsigned int podCount = m_K;
    unsigned int perPodHostCount = m_K * m_K /4;
    unsigned int perDirectionPortCount = m_K /2;
    unsigned int perPodEdgeCount = m_K/2;
    unsigned int perPodAggrCount = m_K/2;
    unsigned int totalCoreCount  = m_K * m_K /4;
    
    Ipv4StaticRoutingHelper ipv4RoutingHelper;
                
    for (unsigned int podNum = 0; podNum < podCount; ++podNum) {
        for (unsigned int hostNum = 0; hostNum < perPodHostCount; ++hostNum) {
            string name;

            //Get the Host node
            Ptr<Node> host = Names::Find<Node> (m_prefix, GetHostNodeName(podNum, hostNum, name));
            if (!host) {
                NS_LOG_ERROR("Invalid Host Node " << podNum << ":" << hostNum);
            }

            unsigned int edgeNum = hostNum / perDirectionPortCount;
            Ptr<Node> edge = Names::Find<Node> (m_prefix, GetEdgeNodeName(podNum, edgeNum, name));
            if (!edge) {
                NS_LOG_ERROR("Invalid Edge Node " << podNum << ":" << edgeNum);
            }
             
            //Get the IPv4 stack for both nodes
            Ptr<Ipv4> hostIpv4 = host->GetObject<Ipv4> ();
            Ptr<Ipv4> edgeIpv4 = edge->GetObject<Ipv4> ();
             
            //Get the static Routing Table in both nodes
            Ptr<Ipv4StaticRouting> staticRoutingHost = ipv4RoutingHelper.GetStaticRouting (hostIpv4);
            Ptr<Ipv4StaticRouting> staticRoutingEdge = ipv4RoutingHelper.GetStaticRouting (edgeIpv4);

            //Now we need to get the IP address for the device in the edge connected to this host
            Ptr<NetDevice> hostDev  = Names::Find<NetDevice> (m_prefix, GetDevName(host, edge, name)); 
            Ptr<NetDevice> edgeDev  = Names::Find<NetDevice> (m_prefix, GetDevName(edge, host, name));

            //Get IP address for both devices
            Ipv4Address hostAddr = GetIpAddressForDevice (hostDev);
            //Ipv4Address edgeAddr = GetIpAddressForDevice (edgeDev);

            Ipv4Address baseAddr = hostAddr.CombineMask("255.0.0.0");

            //Get the interfaces
            int32_t hostInterface = hostIpv4->GetInterfaceForDevice(hostDev);
            int32_t edgeInterface = edgeIpv4->GetInterfaceForDevice(edgeDev);

             
            //Now add the route in the host node
            //All address space within the network is routed to the edge switch
            staticRoutingHost->AddNetworkRouteTo(baseAddr, "255.0.0.0", hostInterface);

            //Add the route in the edge switch
            //Only traffic to the host is routed to it
            staticRoutingEdge->AddHostRouteTo(hostAddr,edgeInterface);             
        }
    }

    //Adding routes for the Edge - Aggr links
    for (unsigned int podNum = 0; podNum < podCount; ++podNum) {
        for (unsigned int edgeNum = 0; edgeNum < perPodEdgeCount; ++edgeNum) {
            for (unsigned int aggrNum = 0; aggrNum < perPodAggrCount; ++aggrNum) {
                string name;

                //Get the edge switch node
                Ptr<Node> edge = Names::Find<Node> (m_prefix, GetEdgeNodeName(podNum, edgeNum, name));
                if (!edge) {
                    NS_LOG_ERROR("Invalid Edge Node " << podNum << ":" << edgeNum);
                }

                //Get the aggr switch node
                Ptr<Node> aggr = Names::Find<Node> (m_prefix, GetAggrNodeName(podNum, aggrNum, name));
                if (!aggr) {
                    NS_LOG_ERROR("Invalid Aggr Node " << podNum << ":" << aggrNum);
                }
             
                //Get the IPv4 stack for both nodes
                Ptr<Ipv4> edgeIpv4 = edge->GetObject<Ipv4> ();
                Ptr<Ipv4> aggrIpv4 = aggr->GetObject<Ipv4> ();
             
                //Get the static Routing Table in both nodes
                Ptr<Ipv4StaticRouting> staticRoutingEdge = ipv4RoutingHelper.GetStaticRouting (edgeIpv4);
                Ptr<Ipv4StaticRouting> staticRoutingAggr = ipv4RoutingHelper.GetStaticRouting (aggrIpv4);

                //We now get the devices
                Ptr<NetDevice> edgeDev  = Names::Find<NetDevice> (m_prefix, GetDevName(edge, aggr, name)); 
                Ptr<NetDevice> aggrDev  = Names::Find<NetDevice> (m_prefix, GetDevName(aggr, edge, name));

                //Get IP address for both devices
                Ipv4Address edgeAddr = GetIpAddressForDevice (edgeDev);
                //Ipv4Address aggrAddr = GetIpAddressForDevice (aggrDev);

                Ipv4Address baseAddr = edgeAddr.CombineMask("255.0.0.0");
                uint8_t buf[4];
                edgeAddr.Serialize(buf);
                buf[3] = 0;
                buf[2] &= 0xFC;

                Ipv4Address edgeNetworkAddr = Ipv4Address::Deserialize(buf);
                
                //Get the interfaces
                int32_t edgeInterface = edgeIpv4->GetInterfaceForDevice(edgeDev);
                int32_t aggrInterface = aggrIpv4->GetInterfaceForDevice(aggrDev);

             
                //Now add the route in the edge node
                //All address space within the network is routed to the aggr switch
                staticRoutingEdge->AddNetworkRouteTo(baseAddr, "255.0.0.0", edgeInterface);

                //Add the route in the aggr switch
                //Only traffic to the hosts connected to the edge switch
                staticRoutingAggr->AddNetworkRouteTo(edgeNetworkAddr,"255.255.252.0",aggrInterface);                  
            }
        }
    }

 
    for (unsigned int coreNum = 0; coreNum < totalCoreCount; ++coreNum) {
        for (unsigned int podNum = 0; podNum < podCount; ++podNum) {

            unsigned int aggrNum = coreNum / perDirectionPortCount;
            string name;

            //Get the aggr switch node
            Ptr<Node> aggr = Names::Find<Node> (m_prefix, GetAggrNodeName(podNum, aggrNum, name));
            if (!aggr) {
                NS_LOG_ERROR("Invalid Aggr Node " << podNum << ":" << aggrNum);
            }
 
            //Get the core switch node
            Ptr<Node> core = Names::Find<Node> (m_prefix, GetCoreNodeName(coreNum, name));
            if (!core) {
                NS_LOG_ERROR("Invalid Core Node " << coreNum);
            }
             
            //Get the IPv4 stack for both nodes
            Ptr<Ipv4> aggrIpv4 = aggr->GetObject<Ipv4> ();
            Ptr<Ipv4> coreIpv4 = core->GetObject<Ipv4> ();
             
            //Get the static Routing Table in both nodes
            Ptr<Ipv4StaticRouting> staticRoutingAggr = ipv4RoutingHelper.GetStaticRouting (aggrIpv4);
            Ptr<Ipv4StaticRouting> staticRoutingCore = ipv4RoutingHelper.GetStaticRouting (coreIpv4);

            //We now get the devices
            Ptr<NetDevice> aggrDev  = Names::Find<NetDevice> (m_prefix, GetDevName(aggr, core, name)); 
            Ptr<NetDevice> coreDev  = Names::Find<NetDevice> (m_prefix, GetDevName(core, aggr, name));

            //Get IP address for both devices
            Ipv4Address aggrAddr = GetIpAddressForDevice (aggrDev);
            //Ipv4Address coreAddr = GetIpAddressForDevice (coreDev);

            Ipv4Address baseAddr = aggrAddr.CombineMask("255.0.0.0");
            uint8_t buf[4];
            aggrAddr.Serialize(buf);
            buf[3] = 0;
            buf[2] = 0;
            buf[1] &= 0xFE;

            Ipv4Address aggrNetworkAddr = Ipv4Address::Deserialize(buf);
                
            //Get the interfaces
            int32_t aggrInterface = aggrIpv4->GetInterfaceForDevice(aggrDev);
            int32_t coreInterface = coreIpv4->GetInterfaceForDevice(coreDev);

             
            //Now add the route in the Aggr node
            //All address space within the network is routed to the core switch
            staticRoutingAggr->AddNetworkRouteTo(baseAddr, "255.0.0.0", aggrInterface);

            //Add the route in the aggr switch
            //Only traffic to the hosts connected to the edge switch
            staticRoutingCore->AddNetworkRouteTo(aggrNetworkAddr,"255.254.0.0",coreInterface);                             
        }
    }
}

}; //namespace ns3
