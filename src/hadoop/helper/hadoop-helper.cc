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

#include "hadoop-helper.h"

NS_LOG_COMPONENT_DEFINE ("HadoopHelper");

namespace ns3 {

HadoopHelper::HadoopHelper () {
    NS_LOG_FUNCTION (this);

    m_dataNodeFactory.SetTypeId("ns3::HadoopDataNode");
    m_hdfsClientFactory.SetTypeId("ns3::HadoopHdfsClient");
}

HadoopHelper::~HadoopHelper() {
    NS_LOG_FUNCTION (this);
}

void HadoopHelper::InstallNameNode (Ptr<Node> node, Ipv4Address addr) {
    NS_LOG_FUNCTION (this);

    Ptr<HadoopNameNode> nameNode = CreateObject<HadoopNameNode> ();
    NS_LOG_LOGIC("Before");
    nameNode->SetAttribute("IpAddress", Ipv4AddressValue(addr));
    NS_LOG_LOGIC("After");
    nameNode->SetStartTime(Seconds(1));
    node->AddApplication (nameNode);

    AddressValue addValDataNodes (InetSocketAddress(addr, 8000 ));
    m_dataNodeFactory.Set ("nameNodeAddress", addValDataNodes);

    AddressValue addValHdfsClients (InetSocketAddress (addr, 9000));
    m_hdfsClientFactory.Set ("nameNodeAddress",addValHdfsClients);
}


Ptr<HadoopDataNode> HadoopHelper::InstallDataNode (Ptr<Node> node, uint32_t podNum, uint32_t rackNum, uint32_t hostNum) {
    NS_LOG_FUNCTION (this);

    Ptr<HadoopDataNode> dataNode = m_dataNodeFactory.Create<HadoopDataNode> ();
    dataNode->SetLocation(podNum, rackNum, hostNum);
    node->AddApplication (dataNode);

    return dataNode;
}


};

#if 0
Address m_nameNodeAddress;
ObjectFactory m_dataNodeFactory;
}
OnOffHelper::OnOffHelper (std::string protocol, Address address)
{
  m_factory.SetTypeId ("ns3::OnOffApplication");
  m_factory.Set ("Protocol", StringValue (protocol));
  m_factory.Set ("Remote", AddressValue (address));
}

void 
OnOffHelper::SetAttribute (std::string name, const AttributeValue &value)
{
  m_factory.Set (name, value);
}

ApplicationContainer
OnOffHelper::Install (Ptr<Node> node) const
{
  return ApplicationContainer (InstallPriv (node));
}

ApplicationContainer
OnOffHelper::Install (std::string nodeName) const
{
  Ptr<Node> node = Names::Find<Node> (nodeName);
  return ApplicationContainer (InstallPriv (node));
}

ApplicationContainer
OnOffHelper::Install (NodeContainer c) const
{
  ApplicationContainer apps;
  for (NodeContainer::Iterator i = c.Begin (); i != c.End (); ++i)
    {
      apps.Add (InstallPriv (*i));
    }

  return apps;
}

Ptr<Application>
OnOffHelper::InstallPriv (Ptr<Node> node) const
{
  Ptr<Application> app = m_factory.Create<Application> ();
  node->AddApplication (app);

  return app;
}

int64_t
OnOffHelper::AssignStreams (NodeContainer c, int64_t stream)
{
  int64_t currentStream = stream;
  Ptr<Node> node;
  for (NodeContainer::Iterator i = c.Begin (); i != c.End (); ++i)
    {
      node = (*i);
      for (uint32_t j = 0; j < node->GetNApplications (); j++)
        {
          Ptr<OnOffApplication> onoff = DynamicCast<OnOffApplication> (node->GetApplication (j));
          if (onoff)
            {
              currentStream += onoff->AssignStreams (currentStream);
            }
        }
    }
  return (currentStream - stream);
}

void 
OnOffHelper::SetConstantRate (DataRate dataRate, uint32_t packetSize)
{
  m_factory.Set ("OnTime", StringValue ("ns3::ConstantRandomVariable[Constant=1000]"));
  m_factory.Set ("OffTime", StringValue ("ns3::ConstantRandomVariable[Constant=0]"));
  m_factory.Set ("DataRate", DataRateValue (dataRate));
  m_factory.Set ("PacketSize", UintegerValue (packetSize));
}

#endif 
