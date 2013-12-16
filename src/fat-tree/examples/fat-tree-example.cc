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
#include "ns3/applications-module.h"

using namespace ns3;


NS_LOG_COMPONENT_DEFINE ("FatTreeExample");

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

  //Get the hosts for both client and server
  string nodeName;
  Ptr<Node> clientHost = Names::Find<Node> (benchMarkNetwork->GetHostNodeName(0,2,nodeName));
  if (!clientHost) {
	  NS_LOG_ERROR ("Can not find a node with the name" + benchMarkNetwork->GetHostNodeName(0,2,nodeName));
  }
  
  Ptr<Node> serverHost = Names::Find <Node> (benchMarkNetwork->GetHostNodeName(3,2, nodeName));
  if (!clientHost) {
	  NS_LOG_ERROR ("Can not find a node with the name" + benchMarkNetwork->GetHostNodeName(3,2,nodeName));
  }
  
  
  //Install the Server Application in the serverHost
  UdpEchoServerHelper echoServer (9);

  ApplicationContainer serverApps = echoServer.Install (serverHost);
  serverApps.Start (Seconds (1.0));
  serverApps.Stop (Seconds (10.0));

  //Install the Client Application in the clientHost
  UdpEchoClientHelper echoClient (benchMarkNetwork->GetHostIpAddress(serverHost), 9);
  //UdpEchoClientHelper echoClient (fatTreeHelper.GetHostIpAddress(3,2), 9);

  echoClient.SetAttribute ("MaxPackets", UintegerValue (1));
  echoClient.SetAttribute ("Interval", TimeValue (Seconds (1.0)));
  echoClient.SetAttribute ("PacketSize", UintegerValue (1024));

  ApplicationContainer clientApps = echoClient.Install (clientHost);
  clientApps.Start (Seconds (2.0));
  clientApps.Stop (Seconds (10.0));
  
  cout << "===========================================" << endl << "Server :";
  benchMarkNetwork->GetHostIpAddress(3,2).Print(cout);
  cout << endl << "Client: ";
  benchMarkNetwork->GetHostIpAddress(0,2).Print(cout);
  cout << endl << "===========================================" << endl;


  Simulator::Run ();
  Simulator::Destroy ();
  return 0;
}

