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

#include "hadoop-name-node.h"

NS_LOG_COMPONENT_DEFINE ("HadoopNameNode");

namespace ns3 {

NS_OBJECT_ENSURE_REGISTERED(HadoopNameNode);

TypeId HadoopNameNode::GetTypeId(void) {
    static TypeId tid = TypeId ("ns3::HadoopNameNode")
        .SetParent<Application> ()
        .AddConstructor<HadoopNameNode> ()

        .AddAttribute ("IpAddress", 
                       "The IP address of the Name Node. ",
                       Ipv4AddressValue ("255.255.255.255"),
                       MakeIpv4AddressAccessor (&HadoopNameNode::m_ownIpAddress),
                       MakeIpv4AddressChecker ())
    ;

    return tid;
}

HadoopNameNode::HadoopNameNode():
    m_socket2DataNodes(0),
    m_socket2HdfsClients(0)  {
    NS_LOG_FUNCTION (this);
}

HadoopNameNode::~HadoopNameNode() {
    NS_LOG_FUNCTION (this);
}

/* Called at Application start */
void HadoopNameNode::StartApplication (void) {
    NS_LOG_FUNCTION (this);

    // Create the socket listening to DataNodes Registration
    if (!m_socket2DataNodes) {
        m_socket2DataNodes = Socket::CreateSocket (GetNode(), TcpSocketFactory::GetTypeId());

        Address ownAddress = Address(InetSocketAddress(m_ownIpAddress, 8000));
        m_socket2DataNodes->Bind(ownAddress);

        m_socket2DataNodes->Listen();

        m_socket2DataNodes->SetAcceptCallback (
            MakeCallback (&HadoopNameNode::AcceptDataNodeConnection, this ),
            MakeCallback (&HadoopNameNode::NewDataNodeConnectionCreated, this) );


        NS_LOG_LOGIC("NameNode Listening socket to DataNodes is: " << m_socket2DataNodes);

    }

    // Create the socket listening to HDFS Clients
    if (!m_socket2HdfsClients) {
        m_socket2HdfsClients = Socket::CreateSocket (GetNode(), TcpSocketFactory::GetTypeId());

        Address ownAddress = Address(InetSocketAddress(m_ownIpAddress, 9000));
        m_socket2HdfsClients->Bind(ownAddress);

        m_socket2HdfsClients->Listen();

        m_socket2HdfsClients->SetAcceptCallback (
            MakeCallback (&HadoopNameNode::AcceptHdfsClientConnection, this ),
            MakeCallback (&HadoopNameNode::NewHdfsClientConnectionCreated, this) );

    }
    
}

/* Called at Application Stop*/
void HadoopNameNode::StopApplication (void) {
    NS_LOG_FUNCTION (this);
  
    if(m_socket2DataNodes) {
        m_socket2DataNodes->Close ();
    }
}

bool HadoopNameNode::AcceptDataNodeConnection (Ptr<Socket> socket, const Address& addr) {
    NS_LOG_FUNCTION (this << socket << addr);

    //Note that this socket is the listening socket which accepted the new connection

    return true;
}

void HadoopNameNode::NewDataNodeConnectionCreated (Ptr<Socket> socket, const Address& addr) {
    NS_LOG_FUNCTION (this << socket << addr);

    //Note that the socket passed to this function is the new socket for the new connction
    
    //Wait for messages from the DataNode
    socket->SetRecvCallback(MakeCallback (&HadoopNameNode::RecvFromDataNode, this));

    return;
}

void HadoopNameNode::RecvFromDataNode (Ptr<Socket> socket) {
    NS_LOG_FUNCTION (this << socket);

    Ptr<Packet> p = socket->Recv();

    NameNodeDataNodeProtocolHeader header;
    p->RemoveHeader(header);

    switch (header.GetMsgType()) {
        case NameNodeDataNodeProtocolHeader::DATA_NODE_REGISTER_REQ: {
            NS_LOG_LOGIC("Recieved DATA_NODE_REGISTER_REQUEST message from a Data Node");

            RegisterRequestMsg reqMsg;
            p->RemoveHeader(reqMsg);

            NS_LOG_LOGIC ("Registering Node Pod:Rack:Host = " << reqMsg.GetPodNum() << ":" << reqMsg.GetRackNum() << ":" << reqMsg.GetHostNum());
#if 0
            Ptr<Packet> copy = p->Copy();
            Ipv4Header iph;
            copy->RemoveHeader (iph);

            m_dataNodeAddresses [m_dataNodeCount++] = iph.GetSource ();
#else
            m_dataNodeAddresses [m_dataNodeCount++] = reqMsg.GetIpAddress ();
#endif
            //Send Register Reply Message
            Ptr<Packet> repPkt = Create<Packet> ();
            
            RegisterReplyMsg repMsg;
            repMsg.SetResultCode(0); //zero for success
            repPkt->AddHeader (repMsg);

            NameNodeDataNodeProtocolHeader repHeader;
            repHeader.SetMsgType (NameNodeDataNodeProtocolHeader::DATA_NODE_REGISTER_REP);
            repPkt->AddHeader (repHeader);

            socket->Send(repPkt);
        }
        break;

        default:
            NS_LOG_LOGIC("Recieved UnIdentified Message From a Data Node.... " << header.GetMsgType());
    }
}

bool HadoopNameNode::AcceptHdfsClientConnection (Ptr<Socket> socket, const Address& addr) {
    NS_LOG_FUNCTION (this << socket << addr);

    return true;
}

void HadoopNameNode::NewHdfsClientConnectionCreated (Ptr<Socket> socket, const Address& addr) {
    NS_LOG_FUNCTION (this << socket << addr);

    //Wait for messages from the HDFS Client
    socket->SetRecvCallback(MakeCallback (&HadoopNameNode::RecvFromHdfsClient, this));
    return;
}

void HadoopNameNode::RecvFromHdfsClient (Ptr<Socket> socket) {
    NS_LOG_FUNCTION (this << socket);
    
    NS_LOG_LOGIC ("Receiving a Packet...");
    Ptr<Packet> p = socket->Recv();

    NameNodeHdfsClientProtocolHeader header;
    p->RemoveHeader(header);

    switch (header.GetMsgType()) {
        case NameNodeHdfsClientProtocolHeader::HDFS_CLIENT_FILE_CREATE_REQ: {
            NS_LOG_LOGIC("Recieved HDFS_CLIENT_FILE_CREATE_REQUEST message from an HDFS Client");

            HdfsClientFileCreateReqMsg reqMsg;
            p->RemoveHeader(reqMsg);

            NS_LOG_LOGIC ("HDFS Client Adding the file: " << reqMsg.GetFileName());

            //Send Reply Message
            Ptr<Packet> repPkt = Create<Packet> ();
            
            HdfsClientFileCreateRepMsg repMsg;
            repMsg.SetResultCode(0); //zero for success
            repMsg.SetFileName (reqMsg.GetFileName());
            repMsg.SetFileId (1);
            repPkt->AddHeader (repMsg);

            NameNodeHdfsClientProtocolHeader repHeader;
            repHeader.SetMsgType (NameNodeHdfsClientProtocolHeader::HDFS_CLIENT_FILE_CREATE_REP);
            repPkt->AddHeader (repHeader);

            socket->Send(repPkt);
        }
        break;

        case NameNodeHdfsClientProtocolHeader::HDFS_CLIENT_FILE_BLOCK_ADD_REQ: {
            NS_LOG_LOGIC("Recieved HDFS_CLIENT_FILE_BLOCK_ADD_REQ  message from an HDFS Client");

            HdfsClientFileBlockAddReqMsg reqMsg;
            p->RemoveHeader (reqMsg);

            NS_LOG_LOGIC ("HDFS Client Adding a block to FileId = " << reqMsg.GetFileId());

            //Send Reply Message
            Ptr<Packet> repPkt = Create<Packet> ();
            
            HdfsClientFileBlockAddRepMsg repMsg;
            repMsg.SetResultCode(0); //zero for success
            repMsg.SetFileId (reqMsg.GetFileId());
            repMsg.SetBlockId (1);
            repMsg.SetBlockSize (64);
            repMsg.SetPipeline (m_dataNodeAddresses[0] , 0);
            repMsg.SetPipeline (m_dataNodeAddresses[1] , 1);
            repMsg.SetPipeline (m_dataNodeAddresses[2] , 2);

            repPkt->AddHeader (repMsg);

            NameNodeHdfsClientProtocolHeader repHeader;
            repHeader.SetMsgType (NameNodeHdfsClientProtocolHeader::HDFS_CLIENT_FILE_BLOCK_ADD_REP);
            repPkt->AddHeader (repHeader);

            socket->Send(repPkt);
        }
        break;

        default:
            NS_LOG_LOGIC("Recieved UnIdentified Message From an HDFS Client.... " << header.GetMsgType());
    }
}

};


#if 0
    .AddAttribute ("IpAddress", 
                   "The IP address to assign to the tap device, when in ConfigureLocal mode.  "
                   "This address will override the discovered IP address of the simulated device.",
                   Ipv4AddressValue ("255.255.255.255"),
                   MakeIpv4AddressAccessor (&TapBridge::m_tapIp),
                   MakeIpv4AddressChecker ())



    nameNode->SetAttribute("Address", addr);
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
