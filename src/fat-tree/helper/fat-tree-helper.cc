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


#include <iostream>
#include <string>

#include "fat-tree-helper.h"
#include "ns3/core-module.h"
#include "ns3/point-to-point-module.h"
#include "ns3/internet-module.h"

using namespace std;

NS_LOG_COMPONENT_DEFINE ("FatTreeHelper");

namespace ns3 {

/*Default Constructor*/
FatTreeHelper::FatTreeHelper() {
	NS_LOG_FUNCTION(this);
	
	m_fatTreeNetworkFactory.SetTypeId ("ns3::FatTreeNetwork");
}// Default Constructor

FatTreeHelper::~FatTreeHelper() {
	NS_LOG_FUNCTION(this);
    //Do Nothing
}// Desturctor

void FatTreeHelper::setNetworkAttribute(string name, const AttributeValue &value) {
	NS_LOG_LOGIC (this);
	
	m_fatTreeNetworkFactory.Set(name, value);
}

//Create a Fat-tree Netowrk
Ptr<FatTreeNetwork> FatTreeHelper::Install(const string networkName) {
	NS_LOG_FUNCTION(this);

    m_fatTreeNetworkFactory.Set("Name", StringValue(networkName));

    Ptr<FatTreeNetwork> net;
    net = m_fatTreeNetworkFactory.Create<FatTreeNetwork> ();
    
    net->Build();
    
    return net;
} //Install()
   
}; //namespace ns3

