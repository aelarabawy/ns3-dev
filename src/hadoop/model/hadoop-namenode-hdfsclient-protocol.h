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

#ifndef HADOOP_NAME_NODE_HDFS_CLIENT_PROTOCOL
#define HADOOP_NAME_NODE_HDFS_CLIENT_PROTOCOL

#include "ns3/hadoop-module.h"
#include "ns3/core-module.h"
#include "ns3/network-module.h"

namespace ns3 {

class NameNodeHdfsClientProtocolHeader : public Header {
public:
    enum {
        HDFS_CLIENT_FILE_CREATE_REQ = 0,
        HDFS_CLIENT_FILE_CREATE_REP = 1,
        HDFS_CLIENT_FILE_BLOCK_ADD_REQ = 2,
        HDFS_CLIENT_FILE_BLOCK_ADD_REP = 3
    };

    NameNodeHdfsClientProtocolHeader();
    virtual ~NameNodeHdfsClientProtocolHeader();
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

class HdfsClientFileCreateReqMsg: public Header {
public:
    HdfsClientFileCreateReqMsg();
    virtual ~HdfsClientFileCreateReqMsg();
    static TypeId GetTypeId (void);

    void SetFileName (string fileName);
    string GetFileName (void);

    TypeId GetInstanceTypeId (void) const;
    virtual uint32_t GetSerializedSize (void) const;
    virtual void Serialize (Buffer::Iterator start) const;
    virtual uint32_t Deserialize (Buffer::Iterator start);
    virtual void Print (std::ostream &os) const; 

private:
    string m_fileName;
};

class HdfsClientFileCreateRepMsg: public Header {
public:
    HdfsClientFileCreateRepMsg();
    virtual ~HdfsClientFileCreateRepMsg();
    static TypeId GetTypeId (void);

    void SetResultCode (uint32_t resultCode);
    uint32_t GetResultCode (void);

    void SetFileId (uint32_t fileId);
    uint32_t GetFileId (void);

    void SetFileName (string fileName);
    string GetFileName (void);

    TypeId GetInstanceTypeId (void) const;
    virtual uint32_t GetSerializedSize (void) const;
    virtual void Serialize (Buffer::Iterator start) const;
    virtual uint32_t Deserialize (Buffer::Iterator start);
    virtual void Print (std::ostream &os) const; 

private:
    uint32_t m_resultCode;
    uint32_t m_fileId;
    string m_fileName;
};

class HdfsClientFileBlockAddReqMsg: public Header {
public:
    HdfsClientFileBlockAddReqMsg();
    virtual ~HdfsClientFileBlockAddReqMsg();
    static TypeId GetTypeId (void);

    void SetFileId (uint32_t fileId);
    uint32_t GetFileId (void);

    TypeId GetInstanceTypeId (void) const;
    virtual uint32_t GetSerializedSize (void) const;
    virtual void Serialize (Buffer::Iterator start) const;
    virtual uint32_t Deserialize (Buffer::Iterator start);
    virtual void Print (std::ostream &os) const; 

private:
    uint32_t m_fileId;
};

class HdfsClientFileBlockAddRepMsg: public Header {
public:
    HdfsClientFileBlockAddRepMsg();
    virtual ~HdfsClientFileBlockAddRepMsg();
    static TypeId GetTypeId (void);

    void SetResultCode (uint32_t resultCode);
    uint32_t GetResultCode (void);

    void SetFileId (uint32_t fileId);
    uint32_t GetFileId (void);

    void SetBlockId (uint32_t blockId);
    uint32_t GetBlockId (void);

    void SetBlockSize (uint32_t blockSize);
    uint32_t GetBlockSize (void);

    void SetPipeline (Ipv4Address address, uint8_t order);
    Ipv4Address GetPipeline (uint8_t order);

    TypeId GetInstanceTypeId (void) const;
    virtual uint32_t GetSerializedSize (void) const;
    virtual void Serialize (Buffer::Iterator start) const;
    virtual uint32_t Deserialize (Buffer::Iterator start);
    virtual void Print (std::ostream &os) const; 

private:
    uint32_t m_resultCode;
    uint32_t m_fileId;
    uint32_t m_blockId;
    uint32_t m_blockSize;
    Ipv4Address m_placement [3];
};



}; //namespace ns3

#endif /* HADOOP_NAME_NODE_HDFS_CLIENT_PROTOCOL */


