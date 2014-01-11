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

#ifndef HADOOP_NAME_NODE_DATA_NODE_PROTOCOL
#define HADOOP_NAME_NODE_DATA_NODE_PROTOCOL

#include "ns3/hadoop-module.h"
#include "ns3/core-module.h"
#include "ns3/network-module.h"

namespace ns3 {

class NameNodeDataNodeProtocolHeader : public Header {
public:
    enum {
        DATA_NODE_REGISTER_REQ = 0,
        DATA_NODE_REGISTER_REP = 1
    };

    NameNodeDataNodeProtocolHeader();
    virtual ~NameNodeDataNodeProtocolHeader();
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

class RegisterRequestMsg: public Header {
public:
    RegisterRequestMsg();
    virtual ~RegisterRequestMsg();
    static TypeId GetTypeId (void);

    void SetPodNum (uint32_t podNum);
    uint32_t GetPodNum (void);

    void SetRackNum (uint32_t rackNum);
    uint32_t GetRackNum (void);

    void SetHostNum (uint32_t hostNum);
    uint32_t GetHostNum (void);

    TypeId GetInstanceTypeId (void) const;
    virtual uint32_t GetSerializedSize (void) const;
    virtual void Serialize (Buffer::Iterator start) const;
    virtual uint32_t Deserialize (Buffer::Iterator start);
    virtual void Print (std::ostream &os) const; 

private:
    uint32_t m_podNum;
    uint32_t m_rackNum;
    uint32_t m_hostNum;
};

class RegisterReplyMsg: public Header {
public:
    RegisterReplyMsg();
    virtual ~RegisterReplyMsg();
    static TypeId GetTypeId (void);

    void SetResultCode (uint32_t resultCode);
    uint32_t GetResultCode (void);

    TypeId GetInstanceTypeId (void) const;
    virtual uint32_t GetSerializedSize (void) const;
    virtual void Serialize (Buffer::Iterator start) const;
    virtual uint32_t Deserialize (Buffer::Iterator start);
    virtual void Print (std::ostream &os) const; 

private:
    uint32_t m_resultCode;
};



}; //namespace ns3

#endif /* HADOOP_NAME_NODE_DATA_NODE_PROTOCOL */


