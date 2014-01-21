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
    m_socket2NameNode(0),
    m_serverSocketPipeline (NULL),
    m_blockCount (0)   {
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

    //Get my own Ip Address
    Ptr<Node> node = GetNode();
    Ptr<Ipv4> ipv4 = node->GetObject<Ipv4> ();
    int32_t interface = ipv4->GetInterfaceForDevice(node->GetDevice(0));
    m_ownIpAddress = ipv4->GetAddress(interface,0).GetLocal();
    NS_LOG_LOGIC ("### Data Node IP Address is " << m_ownIpAddress.Get());

    //Start listening for pipeline connections 
    if (!m_serverSocketPipeline) {
        m_serverSocketPipeline = Socket::CreateSocket (GetNode(), TcpSocketFactory::GetTypeId());

        Address ownAddress = Address(InetSocketAddress(m_ownIpAddress, DATA_NODE_TRAFFIC_LISTENING_PORT));
        m_serverSocketPipeline->Bind(ownAddress);

        m_serverSocketPipeline->Listen();

        m_serverSocketPipeline->SetAcceptCallback (
            MakeCallback (&HadoopDataNode::AcceptPipelineConnection, this ),
            MakeCallback (&HadoopDataNode::NewPipelineConnectionCreated, this) );


        NS_LOG_LOGIC("Data Node Listening socket for Pipelines is: " << m_serverSocketPipeline);
    }

}

void HadoopDataNode::StopApplication() {
    NS_LOG_FUNCTION (this);

    if (m_socket2NameNode) {
        m_socket2NameNode->Close ();
        m_socket2NameNode = NULL;
    }

    if (m_serverSocketPipeline) {
        m_serverSocketPipeline->Close ();
        m_serverSocketPipeline = NULL;
    }

}

void HadoopDataNode::NameNodeConnectionSucceeded (Ptr<Socket> socket) {
    NS_LOG_FUNCTION (this << socket);

    //Now We need to register with the Name Node
    SendRegisterReqMsg (socket);
}

void HadoopDataNode::NameNodeConnectionFailed (Ptr<Socket> socket) {
    NS_LOG_FUNCTION (this << socket);

    NS_LOG_ERROR ("ERROR: Connection to Name node FAILED");
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
            NS_LOG_LOGIC("Recieved UnIdentified Message From the Name Node.... " << header.GetMsgType());
    }
}

bool HadoopDataNode::AcceptPipelineConnection (Ptr<Socket> socket, const Address& addr){
    NS_LOG_FUNCTION (this << socket << addr);

    //Note that this socket is the listening socket which accepted the new connection

    return true;
}

void HadoopDataNode::NewPipelineConnectionCreated (Ptr<Socket> socket, const Address& addr) {
    NS_LOG_FUNCTION (this << socket << addr);

    //Note that the socket passed to this function is the new socket for the new connction
    
    socket->SetRecvCallback(MakeCallback (&HadoopDataNode::RecvFromPipelinePrev, this));

    return;
}

void HadoopDataNode::RecvFromPipelinePrev (Ptr<Socket> socket) {
    NS_LOG_FUNCTION (this << socket);
        
    Ptr<Packet> p = socket->Recv();
    NS_LOG_LOGIC (socket << " :Received Packet of Size = " <<  p->GetSize());

    //Try to find the Block through the socket
    //bool blockFound = false;
    for (uint32_t index = 0; index < m_blockCount; ++index) {
        if (m_blocks[index].m_socketPrev == socket) {
            //blockFound = true;
            NS_LOG_LOGIC ("Socket Matching block # " << index << " Raw Traffic = " << m_blocks[index].m_rawTraffic);
            if (m_blocks[index].m_rawTraffic) {
                HandleDataPacket (p, m_blocks[index]);
                return;
            }
            break;
        }
    }

    HdfsClientDataNodeProtocolHeader header;
    p->RemoveHeader(header);
     
    switch (header.GetMsgType()) {
        case HdfsClientDataNodeProtocolHeader::HDFS_CLIENT_PIPELINE_CREATE_REQ: {
            NS_LOG_LOGIC("Recieved HDFS_CLIENT_PIPELINE_CREATE_REQ message");

            HdfsClientPipelineCreateReqMsg msg;
            p->RemoveHeader(msg);

            //Create a new block
            if (m_blockCount < MAX_PER_DATA_NODE_BLOCK_COUNT) {
                m_blocks[m_blockCount].m_state  = BlockInfo::BLOCK_STATE_PIPELINE_REQUESTED; 
                m_blocks[m_blockCount].m_blockId = msg.GetBlockId ();
                m_blocks[m_blockCount].m_socketPrev = socket;

                //Check location in the pipeline
                bool foundInPipeline = false;
                for (uint32_t index = 0; index < msg.GetPipelineLen(); ++index) {
                    if (msg.GetPipeline(index) == m_ownIpAddress) {
                        foundInPipeline = true;
                        if (index == msg.GetPipelineLen() - 1) {
                            //This is the last data node in the pipeline
                            m_blocks[m_blockCount].m_socketNext = NULL;
                            m_blocks[m_blockCount].m_state = BlockInfo::BLOCK_STATE_PIPELINE_ESTABLISHED;
                            //Send the reply message
                            SendPipelineCreateRepMsg (socket, msg.GetBlockId());
                        } else {
                            //Not the last data node
                            Ptr<Socket> newSocket = Socket::CreateSocket (GetNode (), TcpSocketFactory::GetTypeId() );
                            m_blocks[m_blockCount].m_socketNext = newSocket;

                            ConnectPipelineSocketNext (newSocket, msg.GetPipeline(index + 1));
                            
                            // Store the packet for future forward 
                            Ptr<Packet> storedPkt = Create<Packet> ();
                            storedPkt->AddHeader (msg);
                            storedPkt->AddHeader(header);
                            m_blocks[m_blockCount].m_packet = storedPkt;
                        }
                    }
                }
                if (!foundInPipeline) {
                    NS_LOG_ERROR ("ERROR: Could not find address in Pipeline");
                    break;
                }

                m_blockCount ++;
            }
            else {
                NS_LOG_ERROR ("ERROR: Max block count reached ");
                break;
            }
        }
        break;

        case HdfsClientDataNodeProtocolHeader::HDFS_CLIENT_PACKET: {

            HdfsClientPacketMsg msg;
            p->RemoveHeader(msg);

            NS_LOG_LOGIC ("Recieved HDFS_CLIENT_PACKET message blockId:PacketId:SegmentId = " << msg.GetBlockId() << ":" << msg.GetPacketId() << ":" << msg.GetSegmentId());
            NS_LOG_LOGIC ("Recieved HDFS_CLIENT_PACKET message(cont.) lastSegment:lastPacket = "  << msg.GetLastSegment() << ":" << msg.GetLastPacket());
            //Find the associated block

            bool foundBlock = false;
            for (uint32_t index = 0; index < m_blockCount; ++index) {
                if (m_blocks[index].m_blockId == msg.GetBlockId()) {
                    //Found the block
                    foundBlock = true;
                    switch (m_blocks[index].m_state) {
                        case BlockInfo::BLOCK_STATE_PIPELINE_ESTABLISHED: {
                            m_blocks[index].m_state = BlockInfo::BLOCK_STATE_TRANSFER_IN_PROGRESS;
                        }
                        //Fall Through
                        case BlockInfo::BLOCK_STATE_TRANSFER_IN_PROGRESS: {

                                m_blocks[index].m_currentPacketSize = msg.GetPacketSize();
                                m_blocks[index].m_lastPacket = msg.GetLastPacket ();
                                m_blocks[index].m_currentPacketId = msg.GetPacketId ();
                                m_blocks[index].m_bytesRcvdInPacket = 0;

                            if (m_blocks[index].m_socketNext) {
                                //return the header and send it 
                                NS_LOG_LOGIC ("Forwarding to the Next hop...");
                                Ptr<Packet> forwardPkt = Create<Packet> ();
                                forwardPkt->AddHeader (msg);
                                forwardPkt->AddHeader (header);
                                uint32_t bytesSent = m_blocks[index].m_socketNext->Send (forwardPkt);
                                NS_LOG_LOGIC ("Forwarding " << bytesSent << " Bytes to the next Data Node");
                            } else {
                                SendPacketAck (m_blocks[index].m_socketPrev, m_blocks[index].m_blockId, msg.GetPacketId(), msg.GetLastPacket(), msg.GetPacketSize());
                                m_blocks[index].m_rawTraffic = true;
                            }
                        }
                        break;

                        default: {
                            NS_LOG_ERROR ("ERROR: Invalid state for block info");
                        }
                        break;
                    }
                }
            }

            if (!foundBlock) {
                NS_LOG_ERROR ("ERROR: Could not find block for the connection..");
                break;
            }
        }
        break;

        default:
            NS_LOG_LOGIC("Recieved UnIdentified Message From a Data Node.... " << header.GetMsgType());
    }
}

void HadoopDataNode::ConnectPipelineSocketNext (Ptr<Socket> socket, Ipv4Address address) {
    NS_LOG_FUNCTION (this << socket);

    socket->Bind();
    socket->Connect(Address(InetSocketAddress(address, DATA_NODE_TRAFFIC_LISTENING_PORT)));
     
    socket->SetConnectCallback (
        MakeCallback (&HadoopDataNode::pipelineConnectionSucceeded, this),
        MakeCallback (&HadoopDataNode::pipelineConnectionFailed, this));


    //Set the callback for reception
    socket->SetRecvCallback(MakeCallback (&HadoopDataNode::RecvFromPipelineNext, this));

    NS_LOG_LOGIC("A new Pipeline Connecting Socket is: " << socket);
}

void HadoopDataNode::pipelineConnectionSucceeded (Ptr<Socket> socket) {
    NS_LOG_FUNCTION (this << socket);


    //Find the associated block info record
    bool foundBlock = false;
    for (uint32_t index = 0; index < m_blockCount; ++index) {
        if (m_blocks[index].m_socketNext == socket) {
            foundBlock = true;
            if (m_blocks[index].m_packet) {
                NS_LOG_LOGIC ("Forwarding PipeLineCreate to the Next Hop ... ");
                socket->Send (m_blocks[index].m_packet);
            } else {
                NS_LOG_ERROR ("ERROR: Could not find packet");
            }

            break;
        }
    }
    
    if (!foundBlock) {
        NS_LOG_ERROR ("ERROR: Could not find block for the connection..");
    }
}



void HadoopDataNode::pipelineConnectionFailed (Ptr<Socket> socket) {
    NS_LOG_FUNCTION (this << socket);

    NS_LOG_ERROR ("ERROR: Failed to connect to DATA NODE");
}

void HadoopDataNode::RecvFromPipelineNext (Ptr<Socket> socket) {
    NS_LOG_FUNCTION (this << socket);

    Ptr<Packet> p = socket->Recv();

    HdfsClientDataNodeProtocolHeader header;
    p->RemoveHeader(header);
 
    switch (header.GetMsgType()) {
        case HdfsClientDataNodeProtocolHeader::HDFS_CLIENT_PIPELINE_CREATE_REP: {
            NS_LOG_LOGIC("Recieved HDFS_CLIENT_PIPELINE_CREATE_REP message");

            HdfsClientPipelineCreateRepMsg msg;
            p->PeekHeader(msg);

            //Find the associated block

            bool foundBlock = false;
            for (uint32_t index = 0; index < m_blockCount; ++index) {
                if (m_blocks[index].m_blockId == msg.GetBlockId()) {
                    //Found the block
                    foundBlock = true;
                    if (m_blocks[index].m_state != BlockInfo::BLOCK_STATE_PIPELINE_REQUESTED ) {
                        NS_LOG_ERROR ("ERROR: Invalid state of block");
                        break;
                    } else {
                        m_blocks[index].m_state = BlockInfo::BLOCK_STATE_PIPELINE_ESTABLISHED;
                        //Return the header
                        p->AddHeader (header);
                        NS_LOG_LOGIC ("Forwarding the Pipeline Create Reply to the following Hop ...");
                        m_blocks[index].m_socketPrev->Send (p);
                    }
                }
            }

            if (!foundBlock) {
                NS_LOG_ERROR ("ERROR: Could not find block for the connection..");
            }
        }
        break;

        case HdfsClientDataNodeProtocolHeader::HDFS_CLIENT_PACKET_ACK: {

            HdfsClientPacketAckMsg msg;
            p->PeekHeader(msg);
            
            NS_LOG_LOGIC("Recieved HDFS_CLIENT_PACKET_ACK message blockId:PacketId:lastPacket = " << msg.GetBlockId() << ":" << msg.GetPacketId() << ":" << msg.GetLastPacket());

            //Find the associated block

            bool foundBlock = false;
            for (uint32_t index = 0; index < m_blockCount; ++index) {
                if (m_blocks[index].m_blockId == msg.GetBlockId()) {
                    //Found the block
                    foundBlock = true;
                    if (m_blocks[index].m_state != BlockInfo::BLOCK_STATE_TRANSFER_IN_PROGRESS) {
                        NS_LOG_ERROR ("ERROR: Invalid state for block info");
                    } else {
                        //return the header and send it 
                        p->AddHeader (header);

                        NS_LOG_LOGIC ("Forwarding the PacketAck to the following hop...");
                        m_blocks[index].m_socketPrev->Send (p);

                        m_blocks[index].m_rawTraffic = true;
                    }
                }
            }

            if (!foundBlock) {
                NS_LOG_ERROR ("ERROR: Could not find block for the connection..");
            }
        }
        break;

        case HdfsClientDataNodeProtocolHeader::HDFS_CLIENT_PACKET_COMPLETE: {

            HdfsClientPacketCompleteMsg msg;
            p->PeekHeader(msg);
            
            NS_LOG_LOGIC("Recieved HDFS_CLIENT_PACKET_COMPLETE message blockId:PacketId:lastPacket = " << msg.GetBlockId() << ":" << msg.GetPacketId() << ":" << msg.GetLastPacket());

            //Find the associated block

            bool foundBlock = false;
            for (uint32_t index = 0; index < m_blockCount; ++index) {
                if (m_blocks[index].m_blockId == msg.GetBlockId()) {
                    //Found the block
                    foundBlock = true;
                    if (m_blocks[index].m_state != BlockInfo::BLOCK_STATE_TRANSFER_IN_PROGRESS) {
                        NS_LOG_ERROR ("ERROR: Invalid state for block info");
                    } else {
                        //return the header and send it 
                        p->AddHeader (header);

                        NS_LOG_LOGIC ("Forwarding the HDFS_CLIENT_PACKET_COMPLETE to the following hop...");
                        m_blocks[index].m_socketPrev->Send (p);

                        m_blocks[index].m_currentPacketSize = 0;
                        m_blocks[index].m_lastPacket = 0; 
                        m_blocks[index].m_rawTraffic = 0;
                        m_blocks[index].m_bytesRcvdInPacket = 0;
                        m_blocks[index].m_currentPacketId = 0;

                        if (m_blocks[index].m_lastPacket) {
                            m_blocks[index].m_state = BlockInfo::BLOCK_STATE_TRANSFER_COMPLETED;

                            //Close the socket
                            socket->Close ();
                            m_blocks[index].m_socketNext = NULL;
                        }
                    }
                }
            }

            if (!foundBlock) {
                NS_LOG_ERROR ("ERROR: Could not find block for the connection..");
            }
        }
        break;

        default:
            NS_LOG_LOGIC("Recieved UnIdentified Message From a Data Node.... " << header.GetMsgType());
    }
}

void HadoopDataNode::SendPacketAck (Ptr<Socket> socket, uint32_t blockId, uint32_t packetId, bool isLastPacket, uint32_t packetSize) {
    NS_LOG_FUNCTION (this << socket << blockId);

    NS_LOG_LOGIC ("Preparing Packet Ack Message");
    Ptr<Packet> p = Create<Packet> ();

    HdfsClientPacketAckMsg msg;
    msg.SetBlockId (blockId);
    msg.SetPacketId (packetId);
    msg.SetLastPacket (isLastPacket);
    msg.SetPacketSize (packetSize);
    msg.SetResultCode (0);
    p->AddHeader(msg);

    HdfsClientDataNodeProtocolHeader header;
    header.SetMsgType(HdfsClientDataNodeProtocolHeader::HDFS_CLIENT_PACKET_ACK);
    p->AddHeader(header);

    NS_LOG_LOGIC ("Sending Packet Ack Message");
    socket->Send(p);
}

void HadoopDataNode::SendPipelineCreateRepMsg (Ptr<Socket> socket, uint32_t blockId) {
    NS_LOG_FUNCTION (this << socket << blockId);

    NS_LOG_LOGIC ("Preparing HDFS_CLIENT_PIPELINE_CREATE_REP Message");
    Ptr<Packet> p = Create<Packet> ();

    HdfsClientPipelineCreateRepMsg msg;
    msg.SetBlockId (blockId);
    msg.SetResultCode (0);
    p->AddHeader(msg);

    HdfsClientDataNodeProtocolHeader header;
    header.SetMsgType(HdfsClientDataNodeProtocolHeader::HDFS_CLIENT_PIPELINE_CREATE_REP);
    p->AddHeader(header);

    NS_LOG_LOGIC ("Sending HDFS_CLIENT_PIPELINE_CREATE_REP Message");
    socket->Send(p);
}

void HadoopDataNode::SendRegisterReqMsg (Ptr<Socket> socket) {
    NS_LOG_FUNCTION (this << socket);

    NS_LOG_LOGIC ("Preparing Data Node registration Message");
    Ptr<Packet> p = Create<Packet> ();

    RegisterRequestMsg regReq;
    regReq.SetPodNum (m_podNum);
    regReq.SetRackNum (m_rackNum);
    regReq.SetHostNum (m_hostNum);
 
    NS_LOG_LOGIC ("### Data Node IP Address is " << m_ownIpAddress.Get());
    regReq.SetIpAddress (m_ownIpAddress);
    
    p->AddHeader(regReq);

    NameNodeDataNodeProtocolHeader header;
    header.SetMsgType(NameNodeDataNodeProtocolHeader::DATA_NODE_REGISTER_REQ);
    p->AddHeader(header);

    NS_LOG_LOGIC ("Sending Data Node registration Message");
    socket->Send(p);
}


void HadoopDataNode::HandleDataPacket (Ptr<Packet> p, BlockInfo& block) {
    NS_LOG_FUNCTION (this << p->GetSize() << block.m_blockId);

    block.m_bytesRcvdInPacket += p->GetSize();
    if (block.m_socketNext) {
        block.m_socketNext->Send (p);
    } else if (block.m_bytesRcvdInPacket == block.m_currentPacketSize) {
        SendPacketComplete (block.m_socketPrev, block.m_blockId, block.m_currentPacketId, block.m_lastPacket, block.m_currentPacketSize);

        block.m_currentPacketSize = 0;
        block.m_lastPacket = 0; 
        block.m_rawTraffic = 0;
        block.m_bytesRcvdInPacket = 0;
        block.m_currentPacketId = 0;

        if (block.m_lastPacket) {
            block.m_state = BlockInfo::BLOCK_STATE_TRANSFER_COMPLETED;
            
        }
    }
}

void HadoopDataNode::SendPacketComplete (Ptr<Socket> socket, uint32_t blockId, uint32_t packetId, bool lastPacket, uint32_t packetSize) {
    NS_LOG_FUNCTION (this << socket << blockId <<packetId << lastPacket << packetSize);

    NS_LOG_LOGIC ("Preparing HDFS_CLIENT_PACKET_COMPLETE  Message");
    Ptr<Packet> p = Create<Packet> ();

    HdfsClientPacketCompleteMsg msg;
    msg.SetResultCode (0);
    msg.SetBlockId (blockId);
    msg.SetPacketId (packetId);
    msg.SetLastPacket (lastPacket);
    msg.SetPacketSize (packetSize);
 
    p->AddHeader(msg);

    HdfsClientDataNodeProtocolHeader header;
    header.SetMsgType(HdfsClientDataNodeProtocolHeader::HDFS_CLIENT_PACKET_COMPLETE);
    p->AddHeader(header);

    NS_LOG_LOGIC ("Sending HDFS_CLIENT_PACKET_COMPLETE Message");
    socket->Send(p);
}
};


