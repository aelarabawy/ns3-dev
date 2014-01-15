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

using namespace std;

#include "hadoop-data-node.h"

NS_LOG_COMPONENT_DEFINE ("HadoopDataNode");

namespace ns3 {

NS_OBJECT_ENSURE_REGISTERED(HadoopDataNode);

TypeId HadoopDataNode::GetTypeId() {
    static TypeId tid = TypeId ("ns3::HadoopDataNode")
        .SetParent<Application> ()
        .AddConstructor<HadoopDataNode> ()

        .AddAttribute ("nameNodeAddress",
                       "The address of the nameNode to connect to",
                       AddressValue (),
                       MakeAddressAccessor (&HadoopDataNode::m_nameNodeAddress),
                       MakeAddressChecker ())
    ;

    return tid;
}

HadoopDataNode::HadoopDataNode():
    m_podNum (0),
    m_rackNum (0),
    m_hostNum (0),
    m_socket2NameNode(0)   {
    NS_LOG_FUNCTION (this);
}

HadoopDataNode::~HadoopDataNode() {
    NS_LOG_FUNCTION (this);
}

void HadoopDataNode::SetLocation(uint32_t podNum, uint32_t rackNum, uint32_t hostNum) {
    NS_LOG_FUNCTION (this << podNum << rackNum << hostNum);

    m_podNum = podNum;
    m_rackNum = rackNum;
    m_hostNum = hostNum;
}

void HadoopDataNode::StartApplication() {
    NS_LOG_FUNCTION (this);

    //Create Socket and try to connect to the NameNode
    if (! m_socket2NameNode) {
        m_socket2NameNode = Socket::CreateSocket (GetNode (), TcpSocketFactory::GetTypeId() );
        m_socket2NameNode->Bind();
        m_socket2NameNode->Connect(m_nameNodeAddress);
     
        m_socket2NameNode->SetConnectCallback (
            MakeCallback (&HadoopDataNode::NameNodeConnectionSucceeded, this),
            MakeCallback (&HadoopDataNode::NameNodeConnectionFailed, this));


        //Set the callback for reception
        m_socket2NameNode->SetRecvCallback(MakeCallback (&HadoopDataNode::RecvFromNameNode, this));

        NS_LOG_LOGIC("DataNode Connecting Socket is: " << m_socket2NameNode);
    }
}

void HadoopDataNode::RecvFromNameNode (Ptr<Socket> socket) {
    NS_LOG_FUNCTION (this << socket);

    Ptr<Packet> p = socket->Recv();

    NameNodeDataNodeProtocolHeader header;
    p->RemoveHeader(header);

    switch (header.GetMsgType()) {
        case NameNodeDataNodeProtocolHeader::DATA_NODE_REGISTER_REP: {
            NS_LOG_LOGIC("Recieved DATA_NODE_REGISTER_REPLY message from the Name Node");

            RegisterReplyMsg repMsg;
            p->RemoveHeader(repMsg);

            NS_LOG_LOGIC ("Receiving result code " << repMsg.GetResultCode());
        }
        break;

        default:
            NS_LOG_LOGIC("Recieved UnIdentified Message From a Data Node.... " << header.GetMsgType());
    }
}


void HadoopDataNode::StopApplication() {
    NS_LOG_FUNCTION (this);

    if (m_socket2NameNode) {
        m_socket2NameNode->Close ();
        m_socket2NameNode = NULL;
    }
}

void HadoopDataNode::NameNodeConnectionSucceeded (Ptr<Socket> socket) {
    NS_LOG_FUNCTION (this << socket);

    //Now We need to register with the Name Node

    NS_LOG_LOGIC ("Preparing Data Node registration Message");
    Ptr<Packet> pkt = Create<Packet> ();

    RegisterRequestMsg regReq;
    regReq.SetPodNum (m_podNum);
    regReq.SetRackNum (m_rackNum);
    regReq.SetHostNum (m_hostNum);
 
    NS_LOG_LOGIC ("1"); 
    Ptr<Node> node = GetNode();
    NS_LOG_LOGIC ("2"); 
    Ptr<Ipv4> ipv4 = node->GetObject<Ipv4> ();
    NS_LOG_LOGIC ("3"); 
    int32_t interface = ipv4->GetInterfaceForDevice(node->GetDevice(0));
    NS_LOG_LOGIC ("4: Interface = " << interface); 
    Ipv4Address address = ipv4->GetAddress(interface,0).GetLocal();
    NS_LOG_LOGIC ("5"); 
    NS_LOG_LOGIC ("### Data Node IP Address is " << address.Get());
    regReq.SetIpAddress (address);

    pkt->AddHeader(regReq);

    NameNodeDataNodeProtocolHeader header;
    header.SetMsgType(NameNodeDataNodeProtocolHeader::DATA_NODE_REGISTER_REQ);
    pkt->AddHeader(header);

    NS_LOG_LOGIC ("Sending Data Node registration Message");
    socket->Send(pkt);
}

void HadoopDataNode::NameNodeConnectionFailed (Ptr<Socket> socket) {
    NS_LOG_FUNCTION (this << socket);
}

};


#if 0

TypeId
OnOffApplication::GetTypeId (void)
{
  static TypeId tid = TypeId ("ns3::OnOffApplication")
    .SetParent<Application> ()
    .AddConstructor<OnOffApplication> ()
    .AddAttribute ("DataRate", "The data rate in on state.",
                   DataRateValue (DataRate ("500kb/s")),
                   MakeDataRateAccessor (&OnOffApplication::m_cbrRate),
                   MakeDataRateChecker ())
    .AddAttribute ("PacketSize", "The size of packets sent in on state",
                   UintegerValue (512),
                   MakeUintegerAccessor (&OnOffApplication::m_pktSize),
                   MakeUintegerChecker<uint32_t> (1))
    .AddAttribute ("Remote", "The address of the destination",
                   AddressValue (),
                   MakeAddressAccessor (&OnOffApplication::m_peer),
                   MakeAddressChecker ())
    .AddAttribute ("OnTime", "A RandomVariableStream used to pick the duration of the 'On' state.",
                   StringValue ("ns3::ConstantRandomVariable[Constant=1.0]"),
                   MakePointerAccessor (&OnOffApplication::m_onTime),
                   MakePointerChecker <RandomVariableStream>())
    .AddAttribute ("OffTime", "A RandomVariableStream used to pick the duration of the 'Off' state.",
                   StringValue ("ns3::ConstantRandomVariable[Constant=1.0]"),
                   MakePointerAccessor (&OnOffApplication::m_offTime),
                   MakePointerChecker <RandomVariableStream>())
    .AddAttribute ("MaxBytes", 
                   "The total number of bytes to send. Once these bytes are sent, "
                   "no packet is sent again, even in on state. The value zero means "
                   "that there is no limit.",
                   UintegerValue (0),
                   MakeUintegerAccessor (&OnOffApplication::m_maxBytes),
                   MakeUintegerChecker<uint32_t> ())
    .AddAttribute ("Protocol", "The type of protocol to use.",
                   TypeIdValue (UdpSocketFactory::GetTypeId ()),
                   MakeTypeIdAccessor (&OnOffApplication::m_tid),
                   MakeTypeIdChecker ())
    .AddTraceSource ("Tx", "A new packet is created and is sent",
                     MakeTraceSourceAccessor (&OnOffApplication::m_txTrace))
  ;
  return tid;
}


OnOffApplication::OnOffApplication ()
  : m_socket (0),
    m_connected (false),
    m_residualBits (0),
    m_lastStartTime (Seconds (0)),
    m_totBytes (0)
{
  NS_LOG_FUNCTION (this);
}

OnOffApplication::~OnOffApplication()
{
  NS_LOG_FUNCTION (this);
}

void 
OnOffApplication::SetMaxBytes (uint32_t maxBytes)
{
  NS_LOG_FUNCTION (this << maxBytes);
  m_maxBytes = maxBytes;
}

Ptr<Socket>
OnOffApplication::GetSocket (void) const
{
  NS_LOG_FUNCTION (this);
  return m_socket;
}

int64_t 
OnOffApplication::AssignStreams (int64_t stream)
{
  NS_LOG_FUNCTION (this << stream);
  m_onTime->SetStream (stream);
  m_offTime->SetStream (stream + 1);
  return 2;
}

void
OnOffApplication::DoDispose (void)
{
  NS_LOG_FUNCTION (this);

  m_socket = 0;
  // chain up
  Application::DoDispose ();
}

// Application Methods
void OnOffApplication::StartApplication () // Called at time specified by Start
{
  NS_LOG_FUNCTION (this);

  // Create the socket if not already
  if (!m_socket)
    {
      m_socket = Socket::CreateSocket (GetNode (), m_tid);
      if (Inet6SocketAddress::IsMatchingType (m_peer))
        {
          m_socket->Bind6 ();
        }
      else if (InetSocketAddress::IsMatchingType (m_peer) ||
               PacketSocketAddress::IsMatchingType (m_peer))
        {
          m_socket->Bind ();
        }
      m_socket->Connect (m_peer);
      m_socket->SetAllowBroadcast (true);
      m_socket->ShutdownRecv ();

      m_socket->SetConnectCallback (
        MakeCallback (&OnOffApplication::ConnectionSucceeded, this),
        MakeCallback (&OnOffApplication::ConnectionFailed, this));
    }
  // Insure no pending event
  CancelEvents ();
  // If we are not yet connected, there is nothing to do here
  // The ConnectionComplete upcall will start timers at that time
  //if (!m_connected) return;
  ScheduleStartEvent ();
}

void OnOffApplication::StopApplication () // Called at time specified by Stop
{
  NS_LOG_FUNCTION (this);

  CancelEvents ();
  if(m_socket != 0)
    {
      m_socket->Close ();
    }
  else
    {
      NS_LOG_WARN ("OnOffApplication found null socket to close in StopApplication");
    }
}

void OnOffApplication::CancelEvents ()
{
  NS_LOG_FUNCTION (this);

  if (m_sendEvent.IsRunning ())
    { // Cancel the pending send packet event
      // Calculate residual bits since last packet sent
      Time delta (Simulator::Now () - m_lastStartTime);
      int64x64_t bits = delta.To (Time::S) * m_cbrRate.GetBitRate ();
      m_residualBits += bits.GetHigh ();
    }
  Simulator::Cancel (m_sendEvent);
  Simulator::Cancel (m_startStopEvent);
}

// Event handlers
void OnOffApplication::StartSending ()
{
  NS_LOG_FUNCTION (this);
  m_lastStartTime = Simulator::Now ();
  ScheduleNextTx ();  // Schedule the send packet event
  ScheduleStopEvent ();
}

void OnOffApplication::StopSending ()
{
  NS_LOG_FUNCTION (this);
  CancelEvents ();

  ScheduleStartEvent ();
}

// Private helpers
void OnOffApplication::ScheduleNextTx ()
{
  NS_LOG_FUNCTION (this);

  if (m_maxBytes == 0 || m_totBytes < m_maxBytes)
    {
      uint32_t bits = m_pktSize * 8 - m_residualBits;
      NS_LOG_LOGIC ("bits = " << bits);
      Time nextTime (Seconds (bits /
                              static_cast<double>(m_cbrRate.GetBitRate ()))); // Time till next packet
      NS_LOG_LOGIC ("nextTime = " << nextTime);
      m_sendEvent = Simulator::Schedule (nextTime,
                                         &OnOffApplication::SendPacket, this);
    }
  else
    { // All done, cancel any pending events
      StopApplication ();
    }
}

void OnOffApplication::ScheduleStartEvent ()
{  // Schedules the event to start sending data (switch to the "On" state)
  NS_LOG_FUNCTION (this);

  Time offInterval = Seconds (m_offTime->GetValue ());
  NS_LOG_LOGIC ("start at " << offInterval);
  m_startStopEvent = Simulator::Schedule (offInterval, &OnOffApplication::StartSending, this);
}

void OnOffApplication::ScheduleStopEvent ()
{  // Schedules the event to stop sending data (switch to "Off" state)
  NS_LOG_FUNCTION (this);

  Time onInterval = Seconds (m_onTime->GetValue ());
  NS_LOG_LOGIC ("stop at " << onInterval);
  m_startStopEvent = Simulator::Schedule (onInterval, &OnOffApplication::StopSending, this);
}


void OnOffApplication::SendPacket ()
{
  NS_LOG_FUNCTION (this);

  NS_ASSERT (m_sendEvent.IsExpired ());
  Ptr<Packet> packet = Create<Packet> (m_pktSize);
  m_txTrace (packet);
  m_socket->Send (packet);
  m_totBytes += m_pktSize;
  if (InetSocketAddress::IsMatchingType (m_peer))
    {
      NS_LOG_INFO ("At time " << Simulator::Now ().GetSeconds ()
                   << "s on-off application sent "
                   <<  packet->GetSize () << " bytes to "
                   << InetSocketAddress::ConvertFrom(m_peer).GetIpv4 ()
                   << " port " << InetSocketAddress::ConvertFrom (m_peer).GetPort ()
                   << " total Tx " << m_totBytes << " bytes");
    }
  else if (Inet6SocketAddress::IsMatchingType (m_peer))
    {
      NS_LOG_INFO ("At time " << Simulator::Now ().GetSeconds ()
                   << "s on-off application sent "
                   <<  packet->GetSize () << " bytes to "
                   << Inet6SocketAddress::ConvertFrom(m_peer).GetIpv6 ()
                   << " port " << Inet6SocketAddress::ConvertFrom (m_peer).GetPort ()
                   << " total Tx " << m_totBytes << " bytes");
    }
  m_lastStartTime = Simulator::Now ();
  m_residualBits = 0;
  ScheduleNextTx ();
}


void OnOffApplication::ConnectionSucceeded (Ptr<Socket> socket)
{
  NS_LOG_FUNCTION (this << socket);
  m_connected = true;
}

void OnOffApplication::ConnectionFailed (Ptr<Socket> socket)
{
  NS_LOG_FUNCTION (this << socket);
}


#endif
