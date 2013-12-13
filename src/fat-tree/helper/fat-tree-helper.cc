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
  * Author: Ahmed ElArabawy <aelarabawy.git@lasilka.com>
  * 
  * AA: the code will be constituting a new module fat-tree
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

#include "fat-tree-helper.h"
#include "ns3/core-module.h"
#include "ns3/point-to-point-module.h"
#include "ns3/internet-module.h"

using namespace std;

NS_LOG_COMPONENT_DEFINE ("FatTreeHelper");

namespace ns3 {

NS_OBJECT_ENSURE_REGISTERED (FatTreeHelper);

TypeId FatTreeHelper::GetTypeId (void) {
    static TypeId tid = TypeId ("ns3::FatTreeHelper")
        .SetParent<Object> ()
        .AddConstructor<FatTreeHelper>()

        .AddAttribute ("Size",
                       "The Size of the Fat-Tree Network, representing number of Pods, number of ports per switch",
                       UintegerValue(4),
                       MakeUintegerAccessor(&FatTreeHelper::m_K),
                       MakeUintegerChecker <unsigned int>())

        .AddAttribute ("Core2AggrDelay",
                       "The delay on links between the core switches and the Aggregate switches",
                       TimeValue(Seconds(0)),
                       MakeTimeAccessor(&FatTreeHelper::m_a2cDelay),
                       MakeTimeChecker(Time(0)))

        .AddAttribute ("Aggr2EdgeDelay",
                       "The delay on links between the Aggregate switches and the Edge switches",
                       TimeValue(Seconds(0)),
                       MakeTimeAccessor(&FatTreeHelper::m_e2aDelay),
                       MakeTimeChecker(Time(0)))

        .AddAttribute ("Edge2HostDelay",
                       "The delay on links between the Edge switches and the Host Nodes",
                       TimeValue(Seconds(0)),
                       MakeTimeAccessor(&FatTreeHelper::m_h2eDelay),
                       MakeTimeChecker(Time(0)))

        .AddAttribute ("Core2AggrDataRate",
                       "The data rate of links between the core switches and the Aggregate switches",
                       DataRateValue(DataRate("100Mbps")),
                       MakeDataRateAccessor(&FatTreeHelper::m_a2cRate),
                       MakeDataRateChecker())

        .AddAttribute ("Aggr2EdgeDataRate",
                       "The data rate of links between the Aggregate switches and the Edge switches",
                       DataRateValue(DataRate("100Mbps")),
                       MakeDataRateAccessor(&FatTreeHelper::m_e2aRate),
                       MakeDataRateChecker())

        .AddAttribute ("Edge2HostDataRate",
                       "The data rate of links between the Edge switches and the Host Nodes",
                       DataRateValue(DataRate("100Mbps")),
                       MakeDataRateAccessor(&FatTreeHelper::m_h2eRate),
                       MakeDataRateChecker())
        ;
    return tid;
}

/*Default Constructor*/
FatTreeHelper::FatTreeHelper() {
	NS_LOG_FUNCTION(this);
    //DoNothing
}// Default Constructor

FatTreeHelper::~FatTreeHelper() {
	NS_LOG_FUNCTION(this);
    //Do Nothing
}// Desturctor

//Create a Fat-tree Netowrk
void FatTreeHelper::Create(void) {
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

    NS_LOG_LOGIC("Network contains:" << endl 
                  << "/t" << podCount << "pods" << endl
                  << "/t" << totalCoreCount << " Core Switches" << endl
                  << "/t" << totalEdgeCount << " Edge Switches" << endl
                  << "/t" << totalAggrCount  << " Aggregate Switches" << endl
                  << "/t" << totalHostCount << " Hosts" << endl);

    //Create the different nodes
    NS_LOG_LOGIC ("Creating " << totalNodeCount << " Nodes for the Fat -Tree Network");

    m_node.Create(totalNodeCount);

    //Assign the nodes to their respective containers
    unsigned int nodeIndex = 0;
    string nodeName;
    for (unsigned int podNum = 0; podNum < podCount; ++podNum) {
        //Aggregate Switches
        for (unsigned int aggrNum = 0; aggrNum < perPodAggrCount; ++aggrNum) {
            //Name exampe aggr_7_4
            Names::Add(GetAggrNodeName(podNum, aggrNum, nodeName) , m_node.Get(nodeIndex));
            m_aggr.Add(m_node.Get(nodeIndex++));
        }
        
        //Edge Switches
        for (unsigned int edgeNum = 0; edgeNum < perPodEdgeCount; ++edgeNum) {
            //Name exampe edge_7_4

            Names::Add(GetEdgeNodeName(podNum, edgeNum, nodeName) , m_node.Get(nodeIndex));
            m_edge.Add(m_node.Get(nodeIndex++));
        }
        
        //Host Nodes
        for (unsigned int hostNum = 0; hostNum < perPodHostCount; ++hostNum) {
            //Name exampe host_7_12
            Names::Add(GetHostNodeName(podNum, hostNum, nodeName) , m_node.Get(nodeIndex));
            m_host.Add(m_node.Get(nodeIndex++));
        }
    }
    
    //Core Switches
    for (unsigned int coreNum = 0; coreNum < totalCoreCount; ++coreNum)
    {
        //Name exampe core_7_4
        Names::Add(GetCoreNodeName(coreNum, nodeName) , m_node.Get(nodeIndex));
        m_core.Add(m_node.Get(nodeIndex++));
    }
      
    //Make sure we covered all nodes
    if (nodeIndex != totalNodeCount)
    {
        NS_LOG_ERROR("Error... Total NodeCount = " << totalNodeCount << endl
                      << "While #Nodes added = " << "nodeIndex" << endl);
    }
    
    
    //Create Net Devices and assign IP addresses   
    NS_LOG_LOGIC ("Creating connections and set-up addresses.");
    
    /*
     * Note:
     * I will start with basic point2point devices, 
     * later I will switch to more suitable type of devices, maybe csma, or 802.1qbb devices
     */

    //Install stack in all nodes 
    InternetStackHelper stack;
    stack .Install (m_node);

    //Preparing Helpers that will be used for different loops


    PointToPointHelper point2Point;
    
    Ipv4AddressHelper address;
    address.SetBase ("10.0.0.0", "255.0.0.0");
    
    string devName;
    
    //Connecting Hosts to Edge Switches
    for (unsigned int podNum = 0; podNum < podCount; ++podNum) {
        for (unsigned int edgeNum =0; edgeNum < perPodEdgeCount; ++edgeNum) {
            for (unsigned int portNum = 0; portNum < perDirectionPortCount; ++portNum) {

                point2Point.SetDeviceAttribute ("DataRate", DataRateValue(m_h2eRate));
                point2Point.SetChannelAttribute("Delay", TimeValue(m_h2eDelay));
                
                NetDeviceContainer devices;
                            
                NodeContainer nodePair;
                nodePair.Add(GetEdgeNodeName(podNum, edgeNum, nodeName));
                nodePair.Add(GetHostNodeName(podNum, portNum, nodeName));
                devices = point2Point.Install (nodePair);
                
                //Name the associated devices
                //Device Name = dev_<from_Node>_<to_Node>
                //So, if the device is on host_3_1 connected to edge_3_2
                //the device name will be dev_host_3_1_edge_3_2
                //Note that the pod number is repeated in each node

                SetDeviceNames (devices, nodePair);
                Names::Add(GetDevName(nodePair.Get(0),
                                      nodePair.Get(1),
                                      devName),
                           devices.Get(0));
                
                Names::Add(GetDevName(nodePair.Get(1),
                                      nodePair.Get(0),
                                      devName),
                           devices.Get(1));

                
                Ipv4InterfaceContainer interfaces = address.Assign (devices);               
            }
        }
    }
    
    //Connecting Edge Switches to Aggregate Switches
    for (unsigned int podNum = 0; podNum < podCount; ++podNum) {
        for (unsigned int aggrNum = 0; aggrNum < perPodAggrCount; ++aggrNum) {
            for (unsigned int edgeNum = 0; edgeNum < perPodEdgeCount; ++edgeNum) {

                point2Point.SetDeviceAttribute ("DataRate", DataRateValue(m_e2aRate));
                point2Point.SetChannelAttribute("Delay", TimeValue(m_e2aDelay));
                
                NetDeviceContainer devices;
                            
                NodeContainer nodePair;
                nodePair.Add(GetAggrNodeName(podNum, aggrNum, nodeName));
                nodePair.Add(GetEdgeNodeName(podNum, edgeNum, nodeName));
                devices = point2Point.Install (nodePair);
                
                
                Ipv4InterfaceContainer interfaces = address.Assign (devices);                               
            }
        }
    }
    
    //Connecting Core Switches to Aggregate Switches
    for (unsigned int coreNum = 0; coreNum < totalCoreCount; ++coreNum) {
        for (unsigned int podNum = 0; podNum < podCount; ++podNum) {
            for (unsigned int aggrNum = 0; aggrNum < perPodAggrCount; ++aggrNum) {

                point2Point.SetDeviceAttribute ("DataRate", DataRateValue(m_a2cRate));
                point2Point.SetChannelAttribute("Delay", TimeValue(m_a2cDelay));
                
                NetDeviceContainer devices;
                            
                NodeContainer nodePair;
                nodePair.Add(GetCoreNodeName(coreNum, nodeName));
                nodePair.Add(GetAggrNodeName(podNum, aggrNum, nodeName));
                devices = point2Point.Install (nodePair);
                
                
                Ipv4InterfaceContainer interfaces = address.Assign (devices);                               
            }
        }
    }

  //Build Routing tables in all nodes  
  Ipv4GlobalRoutingHelper::PopulateRoutingTables ();

  //Enable PCAP tracing on all devices
  point2Point.EnablePcapAll("fat-tree-");

  //Enable Ascii tracing on all Devices
  point2Point.EnableAsciiAll("fat-tree-");
    
}// Create()


//Private functions that construct the names for the nodes
string& FatTreeHelper::GetEdgeNodeName(unsigned int podNum, unsigned int nodeNum, string &nodeName) {
    nodeName = "edge_";
    nodeName += podNum + "_" + nodeNum;
    
    return nodeName;
}

string& FatTreeHelper::GetAggrNodeName(unsigned int podNum, unsigned int nodeNum, string &nodeName) {
    nodeName = "aggr_";
    nodeName += podNum + "_" + nodeNum;
    
    return nodeName;
}

string& FatTreeHelper::GetCoreNodeName(unsigned int nodeNum, string& nodeName) {
    nodeName = "core_";
    nodeName += nodeNum;
    
    return nodeName;
}

string& FatTreeHelper::GetHostNodeName(unsigned int podNum, unsigned int nodeNum, string &nodeName) {
    nodeName = "host_";
    nodeName += podNum + "_" + nodeNum;
    
    return nodeName;
}

void FatTreeHelper::SetDeviceNames (NetDeviceContainer& devices,
                                NodeContainer& nodePair) {
    string devName;
    
    Names::Add(GetDevName(nodePair.Get(0),
                          nodePair.Get(1),
                          devName),
                          devices.Get(0));

    Names::Add(GetDevName(nodePair.Get(1),
                          nodePair.Get(0),
                          devName),
                          devices.Get(1));
    
}

string& FatTreeHelper::GetDevName (Ptr<Node> nodeFrom,
                                   Ptr<Node> nodeTo,
                                   string& devName) {
        devName = "dev_";
        devName += Names::FindName(nodeFrom);
        devName += Names::FindName(nodeTo);

        return devName;
}

Ipv4Address FatTreeHelper::GetHostIpAddress (Ptr<Node> node) {
	Ptr<Ipv4> ipv4 = node->GetObject<Ipv4> ();
	int32_t interface = ipv4->GetInterfaceForDevice(node->GetDevice(0));	
	
	return ipv4->GetAddress(interface, 0).GetLocal();
}
	
}; //namespace ns3

