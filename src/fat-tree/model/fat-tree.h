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
 */
#ifndef FAT_TREE_H
#define FAT_TREE_H

using namespace std;

#include "ns3/core-module.h"
#include "ns3/network-module.h"


namespace ns3 {

class FatTreeNetwork : public Object
{
public:
    static TypeId GetTypeId (void);
    FatTreeNetwork();
    virtual ~FatTreeNetwork();

    //Create a Fat-tree Netowrk
    void Build(void);

    //Accessor functions for Different Node Containers
    NodeContainer& GetHosts (void) {return m_hosts;}

    //Accessor of Host IP Address and Node Name
    Ipv4Address GetHostIpAddress (Ptr<Node> host);
    Ipv4Address GetHostIpAddress (unsigned int podNum, unsigned int hostNum);
    string& GetHostNodeName(unsigned int podNum, unsigned int nodeNum, string& name);
    
    Ipv4Address GetIpAddressForDevice (Ptr<NetDevice> dev);

private:
    // Parameters
    
    //Size
    unsigned int  m_K;
    
    //Name
    string m_networkName;
    string m_prefix;

    //Node Containers
    NodeContainer  m_allNodes;      //All Nodes
    NodeContainer  m_coreSwitches;  //Core Switches
    NodeContainer  m_aggrSwitches;  //Aggregate Switches
    NodeContainer  m_edgeSwitches;  //Edge Switches
    NodeContainer  m_hosts;         //Host Nodes

    //Data Rates of links between the layers 
    DataRate  m_h2eRate;
    DataRate  m_e2aRate;
    DataRate  m_a2cRate;

    //Delay of links between the layers
    Time  m_h2eDelay;
    Time  m_e2aDelay;
    Time  m_a2cDelay;

    //Grouped Tracing
    bool m_enableGroupedTracing;

    //Private Functions

    //Accessor functions for Different Node Containers
    NodeContainer& GetAllNodes    (void) {return m_allNodes;}
    NodeContainer& GetCoreSwitches(void) {return m_coreSwitches;}
    NodeContainer& GetAggrSwitches(void) {return m_aggrSwitches;}
    NodeContainer& GetEdgeSwitches(void) {return m_edgeSwitches;}

    string& GetEdgeNodeName(unsigned int podNum, unsigned int nodeNum, string &name);
    string& GetAggrNodeName(unsigned int podNum, unsigned int nodeNum, string &name);
    string& GetCoreNodeName(unsigned int nodeNum, string &name);    

    void SetDeviceNames  (NetDeviceContainer& devices,
                          NodeContainer& nodePair);
    
    string& GetDevName (Ptr<Node> nodeFrom,
                        Ptr<Node> nodeTo,
                        string& devName);
    
    string& GetHostDevName (unsigned int podNum,
                            unsigned int edgeNum,
                            unsigned int hostNum,
                            string& devName);


};//class FatTreeNetwork

}; //namespace ns3

#endif /* FAT_TREE_H */
