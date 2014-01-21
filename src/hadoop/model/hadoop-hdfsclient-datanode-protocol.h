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

#ifndef HADOOP_HDFS_CLIENT_DATA_NODE_PROTOCOL
#define HADOOP_HDFS_CLIENT_DATA_NODE_PROTOCOL

#include <string>

#include "ns3/hadoop-module.h"
#include "ns3/core-module.h"
#include "ns3/network-module.h"

using namespace std;

namespace ns3 {

class HdfsClientDataNodeProtocolHeader : public Header {
public:
    enum {
        HDFS_CLIENT_PIPELINE_CREATE_REQ = 0,
        HDFS_CLIENT_PIPELINE_CREATE_REP = 1,
        HDFS_CLIENT_PACKET = 2,
        HDFS_CLIENT_PACKET_ACK = 4,
        HDFS_CLIENT_PACKET_COMPLETE = 5
    };

    HdfsClientDataNodeProtocolHeader();
    virtual ~HdfsClientDataNodeProtocolHeader();
    static TypeId GetTypeId (void);

    void SetMsgType (uint32_t msgType);
    uint32_t GetMsgType (void);

    TypeId GetInstanceTypeId (void) const;
    virtual uint32_t GetSerializedSize (void) const;
    virtual void Serialize (Buffer::Iterator start) const;
    virtual uint32_t Deserialize (Buffer::Iterator start);
    virtual void Print (std::ostream &os) const; 

private:
    uint32_t m_msgType;
};

class HdfsClientPipelineCreateReqMsg: public Header {
public:
    HdfsClientPipelineCreateReqMsg();
    virtual ~HdfsClientPipelineCreateReqMsg();
    static TypeId GetTypeId (void);

    void SetBlockId (uint32_t blockId);
    uint32_t GetBlockId (void);

    void SetPipelineLen (uint32_t pipelineLen);
    uint32_t GetPipelineLen (void);

    void SetPipeline (Ipv4Address address, uint8_t order);
    Ipv4Address GetPipeline (uint8_t order);


    TypeId GetInstanceTypeId (void) const;
    virtual uint32_t GetSerializedSize (void) const;
    virtual void Serialize (Buffer::Iterator start) const;
    virtual uint32_t Deserialize (Buffer::Iterator start);
    virtual void Print (std::ostream &os) const; 

private:
    uint32_t m_blockId;
    uint32_t m_pipelineLen;
    Ipv4Address m_pipeline [MAX_PIPELINE_LEN];
};

class HdfsClientPipelineCreateRepMsg: public Header {
public:
    HdfsClientPipelineCreateRepMsg();
    virtual ~HdfsClientPipelineCreateRepMsg();
    static TypeId GetTypeId (void);

    void SetResultCode (uint32_t resultCode);
    uint32_t GetResultCode (void);

    void SetBlockId (uint32_t blockId);
    uint32_t GetBlockId (void);

    TypeId GetInstanceTypeId (void) const;
    virtual uint32_t GetSerializedSize (void) const;
    virtual void Serialize (Buffer::Iterator start) const;
    virtual uint32_t Deserialize (Buffer::Iterator start);
    virtual void Print (std::ostream &os) const; 

private:
    uint32_t m_resultCode;
    uint32_t m_blockId;
};

class HdfsClientPacketMsg: public Header {
public:
    HdfsClientPacketMsg();
    virtual ~HdfsClientPacketMsg();
    static TypeId GetTypeId (void);

    void SetBlockId (uint32_t blockId);
    uint32_t GetBlockId (void);

    void SetPacketId (uint32_t packetId);
    uint32_t GetPacketId (void);

    void SetSegmentId (uint32_t segmentId);
    uint32_t GetSegmentId (void);

    void SetLastSegment (bool lastSegment);
    bool GetLastSegment (void);

    void SetLastPacket (bool lastPacket);
    bool GetLastPacket (void);

    void SetPacketSize (uint32_t packetSize);
    uint32_t GetPacketSize (void);

    TypeId GetInstanceTypeId (void) const;
    virtual uint32_t GetSerializedSize (void) const;
    virtual void Serialize (Buffer::Iterator start) const;
    virtual uint32_t Deserialize (Buffer::Iterator start);
    virtual void Print (std::ostream &os) const; 

private:
    uint32_t m_blockId;
    uint32_t m_packetId;
    uint32_t m_segmentId;

    bool m_lastSegmentInPacket;
    bool m_lastPacketInBlock;

    uint32_t m_packetSize;
};


class HdfsClientPacketAckMsg: public Header {
public:
    HdfsClientPacketAckMsg();
    virtual ~HdfsClientPacketAckMsg();
    static TypeId GetTypeId (void);

    void SetResultCode (uint32_t resultCode);
    uint32_t GetResultCode (void);

    void SetBlockId (uint32_t blockId);
    uint32_t GetBlockId (void);

    void SetPacketId (uint32_t packetId);
    uint32_t GetPacketId (void);

    void SetLastPacket (bool lastPacket);
    bool GetLastPacket (void);

    void SetPacketSize (uint32_t packetSize);
    uint32_t GetPacketSize (void);

    TypeId GetInstanceTypeId (void) const;
    virtual uint32_t GetSerializedSize (void) const;
    virtual void Serialize (Buffer::Iterator start) const;
    virtual uint32_t Deserialize (Buffer::Iterator start);
    virtual void Print (std::ostream &os) const; 

private:
    uint32_t m_resultCode;
    uint32_t m_blockId;
    uint32_t m_packetId;
    bool m_lastPacketInBlock;
    uint32_t m_packetSize;
};

class HdfsClientPacketCompleteMsg: public Header {
public:
    HdfsClientPacketCompleteMsg();
    virtual ~HdfsClientPacketCompleteMsg();
    static TypeId GetTypeId (void);

    void SetResultCode (uint32_t resultCode);
    uint32_t GetResultCode (void);

    void SetBlockId (uint32_t blockId);
    uint32_t GetBlockId (void);

    void SetPacketId (uint32_t packetId);
    uint32_t GetPacketId (void);

    void SetLastPacket (bool lastPacket);
    bool GetLastPacket (void);

    void SetPacketSize (uint32_t packetSize);
    uint32_t GetPacketSize (void);

    TypeId GetInstanceTypeId (void) const;
    virtual uint32_t GetSerializedSize (void) const;
    virtual void Serialize (Buffer::Iterator start) const;
    virtual uint32_t Deserialize (Buffer::Iterator start);
    virtual void Print (std::ostream &os) const; 

private:
    uint32_t m_resultCode;
    uint32_t m_blockId;
    uint32_t m_packetId;
    bool m_lastPacketInBlock;
    uint32_t m_packetSize;
};


}; //namespace ns3

#endif /* HADOOP_HDFS_CLIENT_DATA_NODE_PROTOCOL */


