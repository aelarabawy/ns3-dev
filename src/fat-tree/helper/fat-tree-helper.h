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
 */

#ifndef FAT_TREE_HELPER_H
#define FAT_TREE_HELPER_H

using namespace std;

#include "ns3/core-module.h"
#include "ns3/network-module.h"

#include "ns3/fat-tree.h"


namespace ns3 {

class FatTreeHelper : public AsciiTraceHelperForDevice, public PcapHelperForDevice
{
public:
    FatTreeHelper();
    virtual ~FatTreeHelper();

    //Set an attribute for the FatTreeNetwork
    void setNetworkAttribute(string name, const AttributeValue &value);

    //Create a Fat-tree Netowrk
    Ptr<FatTreeNetwork> Install(const string networkName);
        
private:
    //Private Parameters
    ObjectFactory m_fatTreeNetworkFactory;
    
    //Private Functions
    //Overloading the pure virtual function of AsciiTraceHelperForDevice
    virtual void EnableAsciiInternal (Ptr<OutputStreamWrapper> stream, std::string prefix, Ptr<NetDevice> nd, bool explicitFilename);

    //Overloading the pure virtual function of PcapHelperForDevice
    virtual void EnablePcapInternal (std::string prefix, Ptr<NetDevice> nd, bool promiscuous, bool explicitFilename);

}; //class FatTreeHelper

}; //namespace ns3

#endif /* FAT_TREE_HELPER_H */
