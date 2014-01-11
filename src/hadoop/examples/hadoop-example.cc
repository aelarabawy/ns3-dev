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

#include "ns3/fat-tree-module.h"
#include "ns3/hadoop-module.h"
#include "ns3/applications-module.h"

using namespace ns3;


NS_LOG_COMPONENT_DEFINE ("HadoopExample");

int 
main (int argc, char *argv[])
{
  bool verbose = true;

  CommandLine cmd;
  cmd.AddValue ("verbose", "Tell application to log if true", verbose);

  cmd.Parse (argc,argv);

  Time::SetResolution (Time::NS);

  FatTreeHelper fatTreeHelper;
  Ptr<FatTreeNetwork> benchMarkNetwork = fatTreeHelper.Install("BenchMark");

  //Install a Hadoop NameNode on Pod0:Host0
  string nodeName;
  Ptr<Node> nameNodeHost = Names::Find<Node>(benchMarkNetwork->GetHostNodeName(0,0,nodeName));
  if (!nameNodeHost) {
      NS_LOG_ERROR ("Can not find a node with the name" + benchMarkNetwork->GetHostNodeName(0,0,nodeName));
  }

  HadoopHelper hadoop;
  Ipv4Address nameNodeAddress = benchMarkNetwork->GetHostIpAddress(nameNodeHost);
  hadoop.InstallNameNode (nameNodeHost , nameNodeAddress);

  //Install a Hadoop DataNode on Pod3:Host1
  Ptr<Node> dataNodeHost = Names::Find<Node> (benchMarkNetwork->GetHostNodeName(3,1,nodeName));
  if (!dataNodeHost) {
      NS_LOG_ERROR ("Can not find a node with the name" + benchMarkNetwork->GetHostNodeName(3,1,nodeName));
  }
  Ptr<HadoopDataNode> dataNodeApp = hadoop.InstallDataNode(dataNodeHost,3,0,1);
  dataNodeApp->SetStartTime (Seconds(3));
  dataNodeApp->SetStopTime  (Seconds(10));


  //Enable tracing
  NS_LOG_LOGIC("Enable ASCII Tracing on all devices");
  AsciiTraceHelper asciiTraceHelper;
  Ptr<OutputStreamWrapper> stream = asciiTraceHelper.CreateFileStream ("traceFile.tr");
  fatTreeHelper.EnableAsciiAll(stream);
  
  NS_LOG_LOGIC("Enable PCAP Tracing on all devices");
  fatTreeHelper.EnablePcapAll("pcapTraceFile");  

  
  Simulator::Run ();
  Simulator::Destroy ();
  return 0;
}

