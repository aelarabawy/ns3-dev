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
#ifndef FAT_TREE_HELPER_H
#define FAT_TREE_HELPER_H

using namespace std;

#include "ns3/core-module.h"
#include "ns3/network-module.h"


namespace ns3 {

class FatTreeHelper : public Object
{
public:
    static TypeId GetTypeId (void);
    FatTreeHelper();   
    virtual ~FatTreeHelper();

    //Create a Fat-tree Netowrk
    void Create(void);

    //Accessor functions for Different Node Containers
    NodeContainer& AllNodes(void)  {return m_node;}
    NodeContainer& CoreNodes(void) {return m_core;}
    NodeContainer& AggrNodes(void)  {return m_aggr;}
    NodeContainer& EdgeNodes(void) {return m_edge;}
    NodeContainer& GetHosts (void) {return m_host;}
    
    //Accessor of Host IP Address and Node Name
    Ipv4Address GetHostIpAddress (Ptr<Node> host);
    Ipv4Address GetHostIpAddress (unsigned int podNum, unsigned int hostNum);
    string& GetHostNodeName(unsigned int podNum, unsigned int nodeNum, string& name);
    
    Ipv4Address GetIpAddressForDevice (Ptr<NetDevice> dev);

        
private:
    // Parameters
    unsigned int  m_K;

    //Node Containers
    NodeContainer  m_node;  //All Nodes
    NodeContainer  m_core;  //Core Switches
    NodeContainer  m_aggr;  //Aggregate Switches
    NodeContainer  m_edge;  //Edge Switches
    NodeContainer  m_host;  //Host Nodes

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
}; //class FatTreeHelper

}; //namespace ns3

#endif /* FAT_TREE_HELPER_H */
