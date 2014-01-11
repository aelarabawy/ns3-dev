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

#include "hadoop-namenode-hdfsclient-protocol.h"

NS_LOG_COMPONENT_DEFINE ("HadoopNameNodeHdfsClientProtocol");

namespace ns3 {


/*************************************************************************
 *       Utility Functions 
 *************************************************************************/
    
    uint32_t GetStringSerializedSize (string str) {
        return (4 + str.length());
    }

    void StringSerialize (Buffer::Iterator start, string str) {
        
        start.WriteHtonU32(str.length());
        start.Write(reinterpret_cast<const uint8_t*> (str.data()),str.length());
    }

    uint32_t StringDeserialize (Buffer::Iterator start, string str) {
        uint32_t len = start.ReadNtohU32 ();
        uint8_t uintArray [50];
        start.Read (uintArray, len);
        uintArray[len] = 0;

        char * charArray = (char*) uintArray;
        str = string(charArray, len);

        return (4 + len);
    }



/*************************************************************************
 *       NameNodeHdfsClientProtocolHeader 
 *       (Used for all messages between Name node and Hdfs Clients)
 *************************************************************************/

    NS_OBJECT_ENSURE_REGISTERED (NameNodeHdfsClientProtocolHeader);

    NameNodeHdfsClientProtocolHeader::NameNodeHdfsClientProtocolHeader ()
        : m_msgType(0) {
        NS_LOG_FUNCTION (this);
    }

    NameNodeHdfsClientProtocolHeader::~NameNodeHdfsClientProtocolHeader () {
        NS_LOG_FUNCTION (this);
    }

    TypeId NameNodeHdfsClientProtocolHeader::GetTypeId (void) {
        static TypeId tid = TypeId ("ns3::NameNodeHdfsClientProtocolHeader")
            .SetParent<Header> ()
            .AddConstructor<NameNodeHdfsClientProtocolHeader> ()
        ;

        return tid;
    }

    TypeId NameNodeHdfsClientProtocolHeader::GetInstanceTypeId (void) const {
        NS_LOG_FUNCTION (this);
        return GetTypeId ();
    }

    void NameNodeHdfsClientProtocolHeader::SetMsgType (uint32_t msgType) {
        NS_LOG_FUNCTION (this);
        m_msgType = msgType;
    }

    uint32_t NameNodeHdfsClientProtocolHeader::GetMsgType (void) {
        NS_LOG_FUNCTION (this);
        return m_msgType;
    }

    uint32_t NameNodeHdfsClientProtocolHeader::GetSerializedSize (void) const {
        NS_LOG_FUNCTION (this);
        return 4;
    }

    void NameNodeHdfsClientProtocolHeader::Serialize (Buffer::Iterator start) const {
        NS_LOG_FUNCTION (this << &start);

        Buffer::Iterator i = start;
        i.WriteHtonU32(m_msgType);

    }

    uint32_t NameNodeHdfsClientProtocolHeader::Deserialize (Buffer::Iterator start) {
        NS_LOG_FUNCTION (this << &start);
        m_msgType = start.ReadNtohU32 ();
        return 4;
    }

    void NameNodeHdfsClientProtocolHeader::Print (std::ostream &os) const {
        NS_LOG_FUNCTION (this << &os);
        os << "Msg Type = " << m_msgType; 
    }

/*************************************************************************
 *        HdfsClientFileCreateReqMsg 
 *        (Used by the HDFS Client to add an HDFS file to the name node)
 *************************************************************************/

    NS_OBJECT_ENSURE_REGISTERED (HdfsClientFileCreateReqMsg);

    HdfsClientFileCreateReqMsg::HdfsClientFileCreateReqMsg ()
        : m_fileName("") {
        NS_LOG_FUNCTION (this);
    }

    HdfsClientFileCreateReqMsg::~HdfsClientFileCreateReqMsg() {
        NS_LOG_FUNCTION (this);
    }

    TypeId HdfsClientFileCreateReqMsg::GetTypeId (void) {
        static TypeId tid = TypeId ("ns3::HdfsClientFileCreateReqMsg")
            .SetParent<Header> ()
            .AddConstructor<HdfsClientFileCreateReqMsg> ()
        ;

        return tid;
    }

    TypeId HdfsClientFileCreateReqMsg::GetInstanceTypeId (void) const {
        NS_LOG_FUNCTION (this);
        return GetTypeId ();
    }

    void HdfsClientFileCreateReqMsg::SetFileName (string fileName) {
        NS_LOG_FUNCTION (this);
        m_fileName = fileName;
    }

    string HdfsClientFileCreateReqMsg::GetFileName (void) {
        NS_LOG_FUNCTION (this);
        return m_fileName;
    }


    uint32_t HdfsClientFileCreateReqMsg::GetSerializedSize (void) const {
        NS_LOG_FUNCTION (this);
        return (GetStringSerializedSize (m_fileName));
    }

    void HdfsClientFileCreateReqMsg::Serialize (Buffer::Iterator start) const {
        NS_LOG_FUNCTION (this << &start);

        Buffer::Iterator i = start;
        StringSerialize (i, m_fileName);
    }

    uint32_t HdfsClientFileCreateReqMsg::Deserialize (Buffer::Iterator start) {
        NS_LOG_FUNCTION (this << &start);

        return (StringDeserialize(start, m_fileName));
    }

    void HdfsClientFileCreateReqMsg::Print (std::ostream &os) const {
        NS_LOG_FUNCTION (this << &os);
        os << "Filename = " << m_fileName; 
    }

#if 0

/*************************************************************************
 *        RegisterReplyMsg 
 *        (Used by the Name node to respond to data node registration)
 *************************************************************************/

    NS_OBJECT_ENSURE_REGISTERED (RegisterReplyMsg);

    RegisterReplyMsg::RegisterReplyMsg ()
        : m_resultCode (0) {
        NS_LOG_FUNCTION (this);
    }

    RegisterReplyMsg::~RegisterReplyMsg() {
        NS_LOG_FUNCTION (this);
    }

    TypeId RegisterReplyMsg::GetTypeId (void) {
        static TypeId tid = TypeId ("ns3::RegisterReplyMsg")
            .SetParent<Header> ()
            .AddConstructor<RegisterReplyMsg> ()
        ;

        return tid;
    }

    TypeId RegisterReplyMsg::GetInstanceTypeId (void) const {
        NS_LOG_FUNCTION (this);
        return GetTypeId ();
    }

    void RegisterReplyMsg::SetResultCode (uint32_t resultCode) {
        NS_LOG_FUNCTION (this);
        m_resultCode = resultCode;
    }

    uint32_t RegisterReplyMsg::GetResultCode (void) {
        NS_LOG_FUNCTION (this);
        return m_resultCode;
    }

    uint32_t RegisterReplyMsg::GetSerializedSize (void) const {
        NS_LOG_FUNCTION (this);
        return 4;
    }

    void RegisterReplyMsg::Serialize (Buffer::Iterator start) const {
        NS_LOG_FUNCTION (this << &start);

        Buffer::Iterator i = start;
        i.WriteHtonU32(m_resultCode);
    }

    uint32_t RegisterReplyMsg::Deserialize (Buffer::Iterator start) {
        NS_LOG_FUNCTION (this << &start);
        m_resultCode = start.ReadNtohU32 ();
        return 4;
    }

    void RegisterReplyMsg::Print (std::ostream &os) const {
        NS_LOG_FUNCTION (this << &os);
        os << "Result Code = " << m_resultCode; 
    }
#endif
};

