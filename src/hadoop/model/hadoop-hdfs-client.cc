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

#include "hadoop-hdfs-client.h"

NS_LOG_COMPONENT_DEFINE ("HadoopHdfsClient");

namespace ns3 {


//Setting TypeId
NS_OBJECT_ENSURE_REGISTERED(HadoopHdfsClient);

TypeId HadoopHdfsClient::GetTypeId() {
    static TypeId tid = TypeId ("ns3::HadoopHdfsClient")
        .SetParent<Application> ()
        .AddConstructor<HadoopHdfsClient> ()
        
        .AddAttribute ("nameNodeAddress",
                       "The address of the nameNode to connect to",
                       AddressValue (),
                       MakeAddressAccessor (&HadoopHdfsClient::m_nameNodeAddress),
                       MakeAddressChecker ())
    ;

    return tid;
}

//Constructor
HadoopHdfsClient::HadoopHdfsClient(): 
    m_podNum (0),
    m_rackNum (0),
    m_hostNum (0),
    m_socket2NameNode(0),
    m_fileCount (0),
    m_blockCount (0) {
    NS_LOG_FUNCTION (this);
}

//Destructor
HadoopHdfsClient::~HadoopHdfsClient() {
    NS_LOG_FUNCTION (this);
}


//Set the location in the Data Center
void HadoopHdfsClient::SetLocation(uint32_t podNum, uint32_t rackNum, uint32_t hostNum) {
    NS_LOG_FUNCTION (this << podNum << rackNum << hostNum);

    m_podNum = podNum;
    m_rackNum = rackNum;
    m_hostNum = hostNum;
}


//Adding a file to the HDFS list (Runs at setup time)
void HadoopHdfsClient::AddFile (string fileName, Time scheduledTime) {
    NS_LOG_FUNCTION (this << fileName << scheduledTime);
    
    //Check if we are exceeding the max file count
    if (m_fileCount >= MAX_PER_CLIENT_FILE_COUNT) {
        NS_LOG_ERROR ("ERROR Can not add file with name " << fileName << " : Max File Count reached");
        return;
    }

    //Scheduling the file into the simulator
    Simulator::Schedule(scheduledTime, &HadoopHdfsClient::RegisterFileWithNameNode, this);

    //Fill the file entry
    m_files[m_fileCount].m_fileName = fileName;
    m_files[m_fileCount].m_state = FileInfo::FILE_STATE_SCHEDULED;
    m_files[m_fileCount].m_startTime = scheduledTime;

    //Increment number of files
    m_fileCount++;
}


/* Callback function called by simulator
   When the time for starting the client is reached */
void HadoopHdfsClient::StartApplication() {
    NS_LOG_FUNCTION (this);

    //Create Socket and connect to the NameNode
    if (! m_socket2NameNode) {
        m_socket2NameNode = Socket::CreateSocket (GetNode (), TcpSocketFactory::GetTypeId() );
        m_socket2NameNode->Bind();
        m_socket2NameNode->Connect(m_nameNodeAddress);
     
        m_socket2NameNode->SetConnectCallback (
            MakeCallback (&HadoopHdfsClient::NameNodeConnectionSucceeded, this),
            MakeCallback (&HadoopHdfsClient::NameNodeConnectionFailed, this));

        //Set the callback for reception
        m_socket2NameNode->SetRecvCallback(MakeCallback (&HadoopHdfsClient::RecvFromNameNode, this));

        NS_LOG_LOGIC("DataNode Connecting Socket is: " << m_socket2NameNode);
    }
}

void HadoopHdfsClient::StopApplication() {
    NS_LOG_FUNCTION (this);

    if (m_socket2NameNode) {
        m_socket2NameNode->Close ();
        m_socket2NameNode = NULL;
    }
}


/* Callback function called by simulator
   when Connection to the name node is successful */
void HadoopHdfsClient::NameNodeConnectionSucceeded (Ptr<Socket> socket) {
    NS_LOG_FUNCTION (this << socket);
}

/* Callback funciton called by simulator
   When Connection to the name node fails */
void HadoopHdfsClient::NameNodeConnectionFailed (Ptr<Socket> socket) {
    NS_LOG_FUNCTION (this << socket);

    NS_LOG_ERROR ("ERROR: Failed to connect to Name Node...");
}


/* Callback function called by simulator
   when the scheduled time of a file is reached */
void HadoopHdfsClient::RegisterFileWithNameNode () {
    NS_LOG_FUNCTION (this << m_fileCount << Simulator::Now());

    //Find the file to be Registered with name node
    for (uint32_t i = 0; i < m_fileCount ; ++i) {
        if (m_files[i].m_state == FileInfo::FILE_STATE_SCHEDULED) {
            if (m_files[i].m_startTime == Simulator::Now()) {
                SendFileCreateReqMsg (m_socket2NameNode, m_files[i].m_fileName);
                m_files[i].m_state = FileInfo::FILE_STATE_REGISTRATION_REQUESTED;
            }
        }
    }
}

/* Callback function called by the Simulator 
   When a packet is received from the Name node*/
void HadoopHdfsClient::RecvFromNameNode (Ptr<Socket> socket) {
    NS_LOG_FUNCTION (this << socket);
    
    NS_LOG_LOGIC ("Receiving a Packet ....");
    Ptr<Packet> p = socket->Recv();

    NameNodeHdfsClientProtocolHeader header;
    p->RemoveHeader(header);
    
    switch (header.GetMsgType()) {
        case NameNodeHdfsClientProtocolHeader::HDFS_CLIENT_FILE_CREATE_REP: {
            NS_LOG_LOGIC("Recieved  HDFS_CLIENT_FILE_CREATE_REP message from the Name Node");

            HdfsClientFileCreateRepMsg repMsg;
            p->RemoveHeader(repMsg);

            NS_LOG_LOGIC ("File:Id " << repMsg.GetFileName() << ":" << repMsg.GetFileId() << " Added with result code " << repMsg.GetResultCode());
            
            //Find File
            for (uint32_t i = 0; i < m_fileCount ; ++i) {
                if (m_files[i].m_fileName == repMsg.GetFileName()) {
                    if (m_files[i].m_state == FileInfo::FILE_STATE_REGISTRATION_REQUESTED) {
                        m_files[i].m_state =  FileInfo::FILE_STATE_REGISTERED;
                        m_files[i].m_fileId = repMsg.GetFileId();
                       
                        //Creating a Block
                        if (m_blockCount >= MAX_PER_CLIENT_BLOCK_COUNT) {
                            NS_LOG_ERROR ("ERROR: MAX Block Count reached");
                            break;
                        }

                        //Assign a new block
                        m_blocks[m_blockCount].m_state = BlockInfo::BLOCK_STATE_REGISTRATION_REQUESTED;
                        m_blocks[m_blockCount].m_fileInfo = &m_files[i];
                        m_blockCount++;

                        //Sending HDFS_CLIENT_FILE_BLOCK_ADD_REQ
                        SendBlockAddReqMsg (m_socket2NameNode, repMsg.GetFileId());

                        break;
                    } else {
                        NS_LOG_ERROR ("ERROR: Invalid state for file " << repMsg.GetFileName());
                        break;
                    }
                }
            }
        }
        break;

        case  NameNodeHdfsClientProtocolHeader::HDFS_CLIENT_FILE_BLOCK_ADD_REP: {
            NS_LOG_LOGIC ("Receiving HDFS_CLIENT_FILE_BLOCK_ADD_REP ......");

            HdfsClientFileBlockAddRepMsg repMsg;
            p->RemoveHeader (repMsg);

            NS_LOG_LOGIC ("Result Code = " << repMsg.GetResultCode() << " FileId:Block Id = " << repMsg.GetFileId() << ":" << repMsg.GetBlockId() << "Block Size = " << repMsg.GetBlockSize());
            
            NS_LOG_LOGIC ("Placements = " << repMsg.GetPipeline(0).Get() << ":" <<  repMsg.GetPipeline(1).Get() << ":" <<  repMsg.GetPipeline (2).Get() ) ;

            //Find the block in the list
            bool foundBlock = false;
            for (uint32_t i = 0; i < m_blockCount; ++i) {
                if (m_blocks[i].m_fileInfo->m_fileId == repMsg.GetFileId() ) {
                    foundBlock = true;
                    if (m_blocks[i].m_state == BlockInfo::BLOCK_STATE_REGISTRATION_REQUESTED) {
                        m_blocks[i].m_state = BlockInfo::BLOCK_STATE_REGISTERED;
                        m_blocks[i].m_blockId = repMsg.GetBlockId();
                        m_blocks[i].m_blockSize = repMsg.GetBlockSize();

                        m_blocks[i].m_pipelineLen = repMsg.GetPipelineLen();
                        for (uint32_t j = 0; j < repMsg.GetPipelineLen() ; ++j) {
                            m_blocks[i].m_pipeline[j] = repMsg.GetPipeline(j);
                        }

                        //Now Connect to the first Data node in the pipeline
                        m_blocks[i].m_socket = ConnectToDataNode (m_blocks[i].m_pipeline[0]);
                    } else {
                        NS_LOG_ERROR ("ERROR: Invalid state for Block of fileId = " << repMsg.GetFileId());
                        break;
                    }
                }
            }
            if (!foundBlock) {
                NS_LOG_ERROR ("ERROR: Could not find block with file Id = " << repMsg.GetFileId());
            }
        }
        break;

        default:
            NS_LOG_LOGIC("Recieved UnIdentified Message From a Data Node.... " << header.GetMsgType());
    }
}

Ptr<Socket> HadoopHdfsClient::ConnectToDataNode (Ipv4Address dataNodeAddress) {
    NS_LOG_FUNCTION (this << dataNodeAddress.Get());

    Ptr<Socket> socket = Socket::CreateSocket (GetNode (), TcpSocketFactory::GetTypeId() );

    socket->Bind();
    socket->Connect(Address(InetSocketAddress(dataNodeAddress, DATA_NODE_TRAFFIC_LISTENING_PORT)));
     
    socket->SetConnectCallback (
        MakeCallback (&HadoopHdfsClient::PipelineConnectionSucceeded, this),
        MakeCallback (&HadoopHdfsClient::PipelineConnectionFailed, this));


    //Set the callback for reception
    socket->SetRecvCallback(MakeCallback (&HadoopHdfsClient::RecvFromPipeline, this));

    NS_LOG_LOGIC("Pipeline Connecting Socket is: " << socket);
    return socket;
}


void HadoopHdfsClient::PipelineConnectionSucceeded (Ptr<Socket> socket) {
    NS_LOG_FUNCTION (this << socket);

    //First Find the pipeline for this socket
    bool blockFound = false;
    for (uint32_t index = 0; index < m_blockCount; ++index) {
        if (m_blocks [index].m_socket == socket) {
            blockFound = true;
            if (m_blocks [index].m_state != BlockInfo::BLOCK_STATE_REGISTERED ) {
                NS_LOG_ERROR ("ERROR: Invalid state for Block Id = " << m_blocks[index].m_blockId);
                break; 
            } else {
                SendPipelineCreateReqMsg (socket, m_blocks[index]);
                m_blocks[index].m_state = BlockInfo::BLOCK_STATE_PIPELINE_INITIATED;
            }
        }
    }

    if (!blockFound) {
        NS_LOG_ERROR ("Error: Can not find the pipeline for this socket...");
    }
}
 
void HadoopHdfsClient::PipelineConnectionFailed (Ptr<Socket> socket) {
    NS_LOG_FUNCTION (this << socket);
}

void HadoopHdfsClient::RecvFromPipeline (Ptr<Socket> socket) {
    NS_LOG_FUNCTION (this << socket);
    
    NS_LOG_LOGIC ("Receiving a Packet from Data node ....");

    Ptr<Packet> p = socket->Recv();

    HdfsClientDataNodeProtocolHeader header;
    p->RemoveHeader(header);
    
    switch (header.GetMsgType()) {
        case HdfsClientDataNodeProtocolHeader::HDFS_CLIENT_PIPELINE_CREATE_REP: {
            NS_LOG_LOGIC("Recieved  HDFS_CLIENT_PIPELINE_CREATE_REP message from the Data node");

            HdfsClientPipelineCreateRepMsg repMsg;
            p->RemoveHeader(repMsg);

            NS_LOG_LOGIC ("ResultCode = " << repMsg.GetResultCode() << "Block Id = " << repMsg.GetBlockId());
            
            //Find Block
            bool blockFound = false;
            for (uint32_t i = 0; i < m_blockCount ; ++i) {
                if (m_blocks[i].m_blockId == repMsg.GetBlockId()) {
                    blockFound = true;
                    if (m_blocks[i].m_state == BlockInfo::BLOCK_STATE_PIPELINE_INITIATED) {
                        m_blocks[i].m_state =  BlockInfo::BLOCK_STATE_PIPELINE_ESTABLISHED;
                       
                        //Starting Data Transfer
                        m_blocks[i].m_state = BlockInfo::BLOCK_STATE_TRANSFER_IN_PROGRESS;
                        TransferData (socket, m_blocks[i]);
                        break;
                    } else {
                        NS_LOG_ERROR ("ERROR: Invalid state for Block " << repMsg.GetBlockId());
                        break;
                    }
                }
            }
            if (!blockFound) {
                NS_LOG_ERROR ("ERROR: Can not find block " << repMsg.GetBlockId());
                break;
            }
        }
        break;

        case HdfsClientDataNodeProtocolHeader::HDFS_CLIENT_PACKET_ACK: {
            NS_LOG_LOGIC("Recieved  HDFS_CLIENT_PACKET_ACK message from the Data node");

            HdfsClientPacketAckMsg ackMsg;
            p->RemoveHeader(ackMsg);

            NS_LOG_LOGIC ("ResultCode = " << ackMsg.GetResultCode() << "Block Id:Packet Id = " << ackMsg.GetBlockId() << ":" << ackMsg.GetPacketId() );
            
            //Find Block
            bool blockFound = false;
            for (uint32_t i = 0; i < m_blockCount ; ++i) {
                if (m_blocks[i].m_blockId == ackMsg.GetBlockId()) {
                    blockFound = true;
                    if (m_blocks[i].m_state == BlockInfo::BLOCK_STATE_TRANSFER_IN_PROGRESS) {
                        m_blocks[i].m_packetAckedCount ++;
                        SendHadoopPacketData (m_blocks[i].m_socket, ackMsg.GetPacketSize());
                    } else {
                        NS_LOG_ERROR ("ERROR: Invalid state for Block " << ackMsg.GetBlockId() << m_blocks[i].m_state);
                        break;
                    }
                }
            }

            if (!blockFound) {
                NS_LOG_ERROR ("ERROR: Can not find block");
                break;
            }
        }
        break;

        case HdfsClientDataNodeProtocolHeader::HDFS_CLIENT_PACKET_COMPLETE: {
            NS_LOG_LOGIC("Recieved  HDFS_CLIENT_PACKET_COMPLETE message from the Data node");

            HdfsClientPacketCompleteMsg completeMsg;
            p->RemoveHeader(completeMsg);

            NS_LOG_LOGIC ("ResultCode = " << completeMsg.GetResultCode() << "Block Id:Packet Id = " << completeMsg.GetBlockId() << ":" << completeMsg.GetPacketId() );
            
            //Find Block
            bool blockFound = false;
            for (uint32_t i = 0; i < m_blockCount ; ++i) {
                if (m_blocks[i].m_blockId == completeMsg.GetBlockId()) {
                    blockFound = true;
                    if (m_blocks[i].m_state == BlockInfo::BLOCK_STATE_TRANSFER_IN_PROGRESS) {
                        m_blocks[i].m_packetCompletedCount ++;
                        if (completeMsg.GetLastPacket()) {
                            m_blocks[i].m_state =  BlockInfo::BLOCK_STATE_TRANSFER_COMPLETED;
                            NS_LOG_INFO ("Block Id " << completeMsg.GetBlockId() << " Has completed transfer");
                          
                            //Send BLOCK_COMPLETE to name node
                            SendBlockComplete (m_socket2NameNode, completeMsg.GetBlockId());
                        } else {
                            //Prepare to send the following packet
                            bool lastPacket;
                            uint32_t packetSize;
                            if (m_blocks[i].m_totalPacketCount == completeMsg.GetPacketId() + 1) {
                                lastPacket = true;
                                packetSize = m_blocks[i].m_lastPacketSize;
                            } else {
                                lastPacket = false;
                                packetSize = PACKET_SIZE;
                            }

                            //Sending PacketRequest
                            SendHadoopPacketReq (socket, m_blocks[i].m_blockId, completeMsg.GetPacketId() + 1, lastPacket, packetSize);
                        }
                    } else {
                        NS_LOG_ERROR ("ERROR: Invalid state for Block " << completeMsg.GetBlockId() << m_blocks[i].m_state);
                        break;
                    }
                }
            }

            if (!blockFound) {
                NS_LOG_ERROR ("ERROR: Could not find block ");
                break;
            }
        }
        break;



        default:
            NS_LOG_LOGIC("Recieved UnIdentified Message From a Data Node.... " << header.GetMsgType());
    }
}

/* Send FILE_CREATE_REQ message to name node */
void HadoopHdfsClient::SendFileCreateReqMsg (Ptr<Socket> socket, string fileName) {
    NS_LOG_FUNCTION (this << fileName);
    
    NS_LOG_LOGIC ("Preparing HDFS_CLIENT_FILE_CREATE_REQ");
    Ptr<Packet> p = Create<Packet> ();

    HdfsClientFileCreateReqMsg msg;
    msg.SetFileName (fileName);
    p->AddHeader (msg);

    NameNodeHdfsClientProtocolHeader header;
    header.SetMsgType (NameNodeHdfsClientProtocolHeader::HDFS_CLIENT_FILE_CREATE_REQ);
    p->AddHeader (header);
    
    NS_LOG_LOGIC ("Sending HDFS_CLIENT_FILE_CREATE_REQ");
    socket->Send(p);    
}

/* Send BLOCK_ADD_REQ message to name node */
void HadoopHdfsClient::SendBlockAddReqMsg (Ptr<Socket> socket, uint32_t fileId) {
    NS_LOG_FUNCTION (this << fileId);

    NS_LOG_LOGIC ("Preparing HDFS_CLIENT_FILE_BLOCK_ADD_REQ ......");
    Ptr<Packet> p = Create<Packet> ();
            
    HdfsClientFileBlockAddReqMsg msg;
    msg.SetFileId(fileId);
    p->AddHeader(msg);

    NameNodeHdfsClientProtocolHeader header;
    header.SetMsgType (NameNodeHdfsClientProtocolHeader::HDFS_CLIENT_FILE_BLOCK_ADD_REQ);
    p->AddHeader (header);

    NS_LOG_LOGIC ("Sending HDFS_CLIENT_FILE_BLOCK_ADD_REQ ......");
    socket->Send (p);
}

/* Send PIPELINE_CREATE_REQ message to Data node*/ 
void HadoopHdfsClient::SendPipelineCreateReqMsg (Ptr<Socket> socket, BlockInfo & block) {            
    NS_LOG_FUNCTION (this << socket << block.m_blockId);

    NS_LOG_LOGIC ("Preparing PIPELINE_CREATE_REQ ......");
    Ptr<Packet> p = Create<Packet> ();

    HdfsClientPipelineCreateReqMsg msg;

    msg.SetBlockId(block.m_blockId);
    msg.SetPipelineLen(block.m_pipelineLen);

    for (uint32_t index = 0; index < block.m_pipelineLen; ++index) {
        msg.SetPipeline(block.m_pipeline[index],index);
    }

    p->AddHeader(msg);

    HdfsClientDataNodeProtocolHeader header;
    header.SetMsgType(HdfsClientDataNodeProtocolHeader::HDFS_CLIENT_PIPELINE_CREATE_REQ);
    p->AddHeader (header);

    NS_LOG_LOGIC ("Sending HDFS_CLIENT_PIPELINE_CREATE_REQ ......");
    socket->Send (p);
}


void HadoopHdfsClient::SendBlockComplete (Ptr<Socket> socket, uint32_t blockId) {
    NS_LOG_FUNCTION (this << socket << blockId);


    NS_LOG_LOGIC ("Preparing BLOCK_COMPLETE ......");
    Ptr<Packet> p = Create<Packet> ();

    HdfsClientBlockCompleteMsg msg;

    msg.SetBlockId(blockId);
    msg.SetResultCode (0);

    p->AddHeader(msg);

    NameNodeHdfsClientProtocolHeader header;
    header.SetMsgType(NameNodeHdfsClientProtocolHeader::HDFS_CLIENT_BLOCK_COMPLETE);
    p->AddHeader (header);

    NS_LOG_LOGIC ("Sending HDFS_CLIENT_BLOCK_COMPLETE ......");
    socket->Send (p);
}

void HadoopHdfsClient::TransferData (Ptr<Socket> socket, BlockInfo & block) {
    NS_LOG_FUNCTION (this << socket << block.m_blockId);

    uint32_t packetCount = block.m_blockSize / PACKET_SIZE;
    uint32_t remainingSize = block.m_blockSize % PACKET_SIZE;

    if (remainingSize == 0) {
        block.m_totalPacketCount = packetCount;
        block.m_lastPacketSize = PACKET_SIZE; 
    } else {
        block.m_totalPacketCount = packetCount + 1;
        block.m_lastPacketSize = remainingSize; 
    }

    //Prepare to send the first packet
    bool lastPacket;
    uint32_t packetSize;
    if (block.m_totalPacketCount == 1) {
        lastPacket = true;
        packetSize = block.m_lastPacketSize;
    } else {
        lastPacket = false;
        packetSize = PACKET_SIZE;
    }

    //Sending PacketRequest
    SendHadoopPacketReq (socket, block.m_blockId, 1, lastPacket, packetSize);
}

void HadoopHdfsClient::SendHadoopPacketData (Ptr<Socket> socket, uint32_t packetSize) {
    NS_LOG_FUNCTION (this << socket << packetSize );

    Ptr<Packet> p = Create<Packet> (packetSize);

    socket->Send (p);
}

void HadoopHdfsClient::SendHadoopPacketReq (Ptr<Socket> socket, uint32_t blockId , uint32_t packetId, bool lastPacket, uint32_t packetSize) {

    NS_LOG_FUNCTION (this << socket << blockId << packetId << lastPacket << packetSize );
    Ptr<Packet> p = Create<Packet> ();
       
    HdfsClientPacketMsg msg;
    msg.SetBlockId (blockId);
    msg.SetPacketId (packetId);
    msg.SetSegmentId (1);
    msg.SetLastSegment (true);
    msg.SetLastPacket(lastPacket);
    msg.SetPacketSize (packetSize);
    p->AddHeader (msg);

    HdfsClientDataNodeProtocolHeader header;
    header.SetMsgType(HdfsClientDataNodeProtocolHeader::HDFS_CLIENT_PACKET);
    p->AddHeader (header);

    NS_LOG_LOGIC ("Sending  HDFS_CLIENT_PACKET ......");

    int retVal = socket->Send (p);
    if (retVal == -1) {
        NS_LOG_ERROR ("ERROR: FAILED TO SEND A Packet With Error " << socket->GetErrno());
    } else {
        NS_LOG_LOGIC ("RETVAL = " << retVal);
    }
    
}

};

