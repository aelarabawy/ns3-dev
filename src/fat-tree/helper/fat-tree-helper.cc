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

//Ascii Trace Helper function
void FatTreeHelper::EnableAsciiInternal (Ptr<OutputStreamWrapper> stream, string prefix, Ptr<NetDevice> nd, bool explicitFilename) {

    //As a sanity check
    //Make sure we are receiving a P2P device (the one we are using in fat-tree-networks)
    Ptr<PointToPointNetDevice> device = nd->GetObject<PointToPointNetDevice> ();
    if (device == 0) {
          NS_LOG_INFO ("PointToPointHelper::EnableAsciiInternal(): Device " << device << 
                       " not of type ns3::PointToPointNetDevice");
          return;
    }
    
    //
    // Our default trace sinks are going to use packet printing, so we have to 
    // make sure that is turned on.
    //
    Packet::EnablePrinting ();
    
    //
    // If we are not provided an OutputStreamWrapper, we are expected to create 
    // one using the usual trace filename conventions and do a Hook*WithoutContext
    // since there will be one file per context and therefore the context would
    // be redundant.
    //
    if (stream == 0) {
        //
        // Set up an output stream object to deal with private ofstream copy 
        // constructor and lifetime issues.  Let the helper decide the actual
        // name of the file given the prefix.
        //
        AsciiTraceHelper asciiTraceHelper;
    
        string filename;
        if (explicitFilename) {
            filename = prefix;
        }
        else {
            filename = asciiTraceHelper.GetFilenameFromDevice (prefix, device);
        }

        Ptr<OutputStreamWrapper> theStream = asciiTraceHelper.CreateFileStream (filename);
    
        //
        // The MacRx trace source provides our "r" event.
        //
        asciiTraceHelper.HookDefaultReceiveSinkWithoutContext<PointToPointNetDevice> (device, "MacRx", theStream);

        //
        // The "+", '-', and 'd' events are driven by trace sources actually in the
        // transmit queue.
        //
        Ptr<Queue> queue = device->GetQueue ();
        asciiTraceHelper.HookDefaultEnqueueSinkWithoutContext<Queue> (queue, "Enqueue", theStream);
        asciiTraceHelper.HookDefaultDropSinkWithoutContext<Queue>    (queue, "Drop"   , theStream);
        asciiTraceHelper.HookDefaultDequeueSinkWithoutContext<Queue> (queue, "Dequeue", theStream);
    
        // PhyRxDrop trace source for "d" event
        asciiTraceHelper.HookDefaultDropSinkWithoutContext<PointToPointNetDevice> (device, "PhyRxDrop", theStream);
    
        return;
    }
    
    //
    // If we are provided an OutputStreamWrapper, we are expected to use it, and
    // to providd a context.  We are free to come up with our own context if we
    // want, and use the AsciiTraceHelper Hook*WithContext functions, but for 
    // compatibility and simplicity, we just use Config::Connect and let it deal
    // with the context.
    //
    // Note that we are going to use the default trace sinks provided by the 
    // ascii trace helper.  There is actually no AsciiTraceHelper in sight here,
    // but the default trace sinks are actually publicly available static 
    // functions that are always there waiting for just such a case.
    //
    uint32_t nodeid = nd->GetNode ()->GetId ();
    uint32_t deviceid = nd->GetIfIndex ();

    ostringstream oss;
    
    oss << "/NodeList/" << nd->GetNode ()->GetId () << "/DeviceList/" << deviceid << "/$ns3::PointToPointNetDevice/MacRx";
    Config::Connect (oss.str (), MakeBoundCallback (&AsciiTraceHelper::DefaultReceiveSinkWithContext, stream));
    
    oss.str ("");
    oss << "/NodeList/" << nodeid << "/DeviceList/" << deviceid << "/$ns3::PointToPointNetDevice/TxQueue/Enqueue";
    Config::Connect (oss.str (), MakeBoundCallback (&AsciiTraceHelper::DefaultEnqueueSinkWithContext, stream));
    
    oss.str ("");
    oss << "/NodeList/" << nodeid << "/DeviceList/" << deviceid << "/$ns3::PointToPointNetDevice/TxQueue/Dequeue";
    Config::Connect (oss.str (), MakeBoundCallback (&AsciiTraceHelper::DefaultDequeueSinkWithContext, stream));
    
    oss.str ("");
    oss << "/NodeList/" << nodeid << "/DeviceList/" << deviceid << "/$ns3::PointToPointNetDevice/TxQueue/Drop";
    Config::Connect (oss.str (), MakeBoundCallback (&AsciiTraceHelper::DefaultDropSinkWithContext, stream));
    
    oss.str ("");
    oss << "/NodeList/" << nodeid << "/DeviceList/" << deviceid << "/$ns3::PointToPointNetDevice/PhyRxDrop";
    Config::Connect (oss.str (), MakeBoundCallback (&AsciiTraceHelper::DefaultDropSinkWithContext, stream));    
}

void FatTreeHelper::EnablePcapInternal (std::string prefix, Ptr<NetDevice> nd, bool promiscuous, bool explicitFilename) {
      
    //As a sanity check
    //Make sure we are receiving a P2P device (the one we are using in fat-tree-networks)
    Ptr<PointToPointNetDevice> device = nd->GetObject<PointToPointNetDevice> ();
    if (device == 0) {
        NS_LOG_INFO ("PointToPointHelper::EnablePcapInternal(): Device " << device << 
                           " not of type ns3::PointToPointNetDevice");
              return;
    }

    PcapHelper pcapHelper;

    string filename;
    if (explicitFilename) {
        filename = prefix;
    }
    else {
        filename = pcapHelper.GetFilenameFromDevice (prefix, device);
    }

    Ptr<PcapFileWrapper> file = pcapHelper.CreateFile (filename, std::ios::out, PcapHelper::DLT_PPP);
    pcapHelper.HookDefaultSink<PointToPointNetDevice> (device, "PromiscSniffer", file);
}
   
}; //namespace ns3

