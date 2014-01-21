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
#ifndef HADOOP_HDFS_CLIENT_H
#define HADOOP_HDFS_CLIENT_H

#include "ns3/core-module.h"
#include "ns3/applications-module.h"
#include "ns3/internet-module.h"
#include "ns3/hadoop-module.h"

using namespace std;

namespace ns3 {

#define MAX_PER_CLIENT_FILE_COUNT  10
#define MAX_PER_CLIENT_BLOCK_COUNT 16


class HadoopHdfsClient : public Application {

public:
    static TypeId GetTypeId(void);
    HadoopHdfsClient();
    virtual ~HadoopHdfsClient();

    void SetLocation(uint32_t podNum, uint32_t rackNum, uint32_t hostNum);
    void AddFile (string fileName, Time scheduledTime);

private:

    class FileInfo {
    public:
        FileInfo () {
            m_state = FileInfo::FILE_STATE_IDLE;
            m_fileName = "";
            m_fileId = 0;
            m_startTime = Seconds(0);
            m_completionTime = Seconds (0);
        }


        ~FileInfo () {
        }

        enum FileState {
            FILE_STATE_IDLE = 0,
            FILE_STATE_SCHEDULED = 1,
            FILE_STATE_REGISTRATION_REQUESTED = 2,
            FILE_STATE_REGISTERED = 3,
            FILE_STATE_COMPLETED = 4
        };

        FileState m_state;
    
        string m_fileName;
        uint32_t m_fileId;
        Time m_startTime;
        Time m_completionTime;
    };

    class BlockInfo {
    public:

        BlockInfo() {
            m_state = BlockInfo::BLOCK_STATE_IDLE;
            m_blockId = 0;
            m_blockSize = 0;
            m_fileInfo = NULL;
            m_socket = NULL;

            m_dataTransfered = 0;
            m_pipelineLen = 0;

            m_totalPacketCount = 0;
            m_packetSentCount = 0;
            m_packetAckedCount = 0;
            m_packetCompletedCount = 0;
            m_lastPacketSize = 0;
        }

        ~BlockInfo() {
        }

        enum BlockState {
            BLOCK_STATE_IDLE = 0,
            BLOCK_STATE_REGISTRATION_REQUESTED = 1,
            BLOCK_STATE_REGISTERED = 2,
            BLOCK_STATE_PIPELINE_INITIATED = 3,
            BLOCK_STATE_PIPELINE_ESTABLISHED = 4,
            BLOCK_STATE_TRANSFER_IN_PROGRESS = 5,
            BLOCK_STATE_TRANSFER_COMPLETED = 6
        };

        BlockState m_state;
        uint32_t m_blockId;
        uint32_t m_blockSize;
        FileInfo*  m_fileInfo;
        Ptr<Socket> m_socket;

        uint32_t m_dataTransfered;
        uint32_t m_pipelineLen;
        Ipv4Address m_pipeline [MAX_PIPELINE_LEN];

        uint32_t m_totalPacketCount;
        uint32_t m_packetSentCount;
        uint32_t m_packetAckedCount;
        uint32_t m_packetCompletedCount;
        uint32_t m_lastPacketSize;
    };


    uint32_t m_podNum;
    uint32_t m_rackNum;
    uint32_t m_hostNum;

    Ptr<Socket> m_socket2NameNode;  //Socket connecting to Name Node
    Address m_nameNodeAddress;

    Ipv4Address m_ownAddress;
  
    uint32_t m_fileCount;
    FileInfo m_files [MAX_PER_CLIENT_FILE_COUNT];

    uint32_t m_blockCount;
    BlockInfo m_blocks [MAX_PER_CLIENT_BLOCK_COUNT];

    // inherited from Application base class.
    virtual void StartApplication (void);    // Called at time specified by Start
    virtual void StopApplication  (void);    // Called at time specified by Stop

    void RegisterFileWithNameNode(void);

    void NameNodeConnectionSucceeded (Ptr<Socket> socket);
    void NameNodeConnectionFailed (Ptr<Socket> socket);
    void RecvFromNameNode (Ptr<Socket> socket); 

    void PipelineConnectionSucceeded (Ptr<Socket> socket);
    void PipelineConnectionFailed (Ptr<Socket> socket); 
    void RecvFromPipeline (Ptr<Socket> socket); 

    Ptr<Socket> ConnectToDataNode (Ipv4Address dataNodeAddress);

    void SendFileCreateReqMsg (Ptr<Socket> socket, string fileName);
    void SendBlockAddReqMsg (Ptr<Socket> socket, uint32_t fileId);
    void SendPipelineCreateReqMsg (Ptr<Socket> socket, BlockInfo & block);
    void SendBlockComplete (Ptr<Socket> socket, uint32_t blockId);

    void TransferData (Ptr<Socket> socket, BlockInfo & block);
    void SendHadoopPacketReq (Ptr<Socket> socket, uint32_t blockId , uint32_t packetId, bool lastPacket, uint32_t packetSize);


    void SendHadoopPacketData (Ptr<Socket> socket, uint32_t packetSize);
};

};

#endif //HADOOP_HDFS_CLIENT_H

