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
            repMsg.SetBlockSize (64000);
            repMsg.SetPipelineLen (3);
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

        case NameNodeHdfsClientProtocolHeader::HDFS_CLIENT_BLOCK_COMPLETE: {
            NS_LOG_LOGIC("Recieved HDFS_CLIENT_BLOCK_COMPLETE  message from an HDFS Client");

            HdfsClientBlockCompleteMsg msg;
            p->RemoveHeader (msg);
            NS_LOG_LOGIC ("BLOCK # " << msg.GetBlockId() << " Has completed its transfer");
        }
        break;


        default:
            NS_LOG_LOGIC("Recieved UnIdentified Message From an HDFS Client.... " << header.GetMsgType());
    }
}

};


