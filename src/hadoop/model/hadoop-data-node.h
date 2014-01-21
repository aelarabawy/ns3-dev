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
#ifndef HADOOP_DATA_NODE_H
#define HADOOP_DATA_NODE_H

#include "ns3/core-module.h"
#include "ns3/applications-module.h"
#include "ns3/internet-module.h"
#include "ns3/hadoop-module.h"

using namespace std;

namespace ns3 {

#define MAX_PER_DATA_NODE_BLOCK_COUNT 16

class HadoopDataNode : public Application {

public:
    static TypeId GetTypeId(void);
    HadoopDataNode();
    virtual ~HadoopDataNode();

    void SetLocation(uint32_t podNum, uint32_t rackNum, uint32_t hostNum);

private:
    class BlockInfo {
    public:

        BlockInfo() {
            m_state = BlockInfo::BLOCK_STATE_IDLE;
            m_blockId = 0;
            m_socketPrev = NULL;
            m_socketNext = NULL;

            m_packet = NULL;

            m_currentPacketSize = 0;
            m_currentPacketId = 0;
            m_lastPacket = false;
            m_rawTraffic = false;

            m_packetRcvdCount = 0;
            m_packetSentCount = 0;

            m_bytesRcvdInPacket = 0;
        }

        ~BlockInfo() {
        }

        enum BlockState {
            BLOCK_STATE_IDLE = 0,
            BLOCK_STATE_PIPELINE_REQUESTED = 1,
            BLOCK_STATE_PIPELINE_ESTABLISHED = 2,
            BLOCK_STATE_TRANSFER_IN_PROGRESS = 3,
            BLOCK_STATE_TRANSFER_COMPLETED = 4
        };

        BlockState m_state;

        uint32_t m_blockId;
        Ptr<Socket> m_socketPrev;
        Ptr<Socket> m_socketNext;
        
        Ptr<Packet> m_packet;

        uint32_t m_currentPacketSize;
        uint32_t m_currentPacketId;
        bool m_lastPacket;
        bool m_rawTraffic;

        uint32_t m_bytesRcvdInPacket;

        uint32_t m_packetRcvdCount;
        uint32_t m_packetSentCount;
    };


    uint32_t m_podNum;
    uint32_t m_rackNum;
    uint32_t m_hostNum;

    Ptr<Socket> m_socket2NameNode;  //Socket connecting to Name Node
    Address m_nameNodeAddress; //Address for Name node
 
    Ptr<Socket> m_serverSocketPipeline;  //Socket listening to Data Nodes and HdfsClients
    Ipv4Address m_ownIpAddress;

    uint32_t m_blockCount;
    BlockInfo m_blocks[MAX_PER_DATA_NODE_BLOCK_COUNT];

    // inherited from Application base class.
    virtual void StartApplication (void);    // Called at time specified by Start
    virtual void StopApplication  (void);    // Called at time specified by Stop

    void NameNodeConnectionSucceeded (Ptr<Socket> socket);
    void NameNodeConnectionFailed (Ptr<Socket> socket);
    void RecvFromNameNode (Ptr<Socket> socket); 

    bool AcceptPipelineConnection (Ptr<Socket> socket, const Address& addr);
    void NewPipelineConnectionCreated (Ptr<Socket> socket, const Address& addr);
    void RecvFromPipelinePrev (Ptr<Socket> socket); 

    void ConnectPipelineSocketNext (Ptr<Socket> socket, Ipv4Address address);

    void pipelineConnectionSucceeded (Ptr<Socket> socket);
    void pipelineConnectionFailed (Ptr<Socket> socket);
    void RecvFromPipelineNext (Ptr<Socket> socket); 


    void SendRegisterReqMsg (Ptr<Socket> socket);
    void SendPacketAck (Ptr<Socket> socket, uint32_t blockId, uint32_t packetId, bool isLastPacket, uint32_t packetSize);
    void SendPipelineCreateRepMsg (Ptr<Socket> socket, uint32_t blockId);


    void HandleDataPacket (Ptr<Packet> p, BlockInfo& block); 
    void SendPacketComplete (Ptr<Socket> socket, uint32_t blockId, uint32_t packetId, bool lastPacket, uint32_t packetSize);
};

};

#endif //HADOOP_DATA_NODE_H

