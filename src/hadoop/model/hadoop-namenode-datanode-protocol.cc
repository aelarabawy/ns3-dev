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

#include "hadoop-namenode-datanode-protocol.h"

NS_LOG_COMPONENT_DEFINE ("HadoopNameNodeDataNodeProtocol");

namespace ns3 {
/*************************************************************************
 *        NameNodeDataNodeProtocolHeader
 *       (Used for all messages between Name node and Data nodes)
 *************************************************************************/

    NS_OBJECT_ENSURE_REGISTERED (NameNodeDataNodeProtocolHeader);

    NameNodeDataNodeProtocolHeader::NameNodeDataNodeProtocolHeader ()
        : m_msgType(0) {
        NS_LOG_FUNCTION (this);
    }

    NameNodeDataNodeProtocolHeader::~NameNodeDataNodeProtocolHeader () {
        NS_LOG_FUNCTION (this);
    }

    TypeId NameNodeDataNodeProtocolHeader::GetTypeId (void) {
        static TypeId tid = TypeId ("ns3::NameNodeDataNodeProtocolHeader")
            .SetParent<Header> ()
            .AddConstructor<NameNodeDataNodeProtocolHeader> ()
        ;

        return tid;
    }

    TypeId NameNodeDataNodeProtocolHeader::GetInstanceTypeId (void) const {
        NS_LOG_FUNCTION (this);

        return GetTypeId ();
    }

    void NameNodeDataNodeProtocolHeader::SetMsgType (uint32_t msgType) {
        NS_LOG_FUNCTION (this << msgType);

        m_msgType = msgType;
    }

    uint32_t NameNodeDataNodeProtocolHeader::GetMsgType (void) {
        NS_LOG_FUNCTION (this);

        return m_msgType;
    }

    uint32_t NameNodeDataNodeProtocolHeader::GetSerializedSize (void) const {
        NS_LOG_FUNCTION (this);

        return 4;
    }

    void NameNodeDataNodeProtocolHeader::Serialize (Buffer::Iterator start) const {
        NS_LOG_FUNCTION (this << &start);

        Buffer::Iterator i = start;
        i.WriteHtonU32(m_msgType);
    }

    uint32_t NameNodeDataNodeProtocolHeader::Deserialize (Buffer::Iterator start) {
        NS_LOG_FUNCTION (this << &start);

        m_msgType = start.ReadNtohU32 ();
        return 4;
    }

    void NameNodeDataNodeProtocolHeader::Print (std::ostream &os) const {
        NS_LOG_FUNCTION (this << &os);

        os << "Msg Type = " << m_msgType; 
    }


/*************************************************************************
 *        RegisterRequestMsg 
 *        (Used by the Data node to register with the name node)
 *************************************************************************/

    NS_OBJECT_ENSURE_REGISTERED (RegisterRequestMsg);

    RegisterRequestMsg::RegisterRequestMsg ()
        : m_podNum (0),
          m_rackNum (0),
          m_hostNum (0) {
        NS_LOG_FUNCTION (this);
    }

    RegisterRequestMsg::~RegisterRequestMsg() {
        NS_LOG_FUNCTION (this);
    }

    TypeId RegisterRequestMsg::GetTypeId (void) {
        static TypeId tid = TypeId ("ns3::RegisterRequestMsg")
            .SetParent<Header> ()
            .AddConstructor<RegisterRequestMsg> ()
        ;

        return tid;
    }

    TypeId RegisterRequestMsg::GetInstanceTypeId (void) const {
        NS_LOG_FUNCTION (this);

        return GetTypeId ();
    }

    void RegisterRequestMsg::SetPodNum (uint32_t podNum) {
        NS_LOG_FUNCTION (this << podNum);

        m_podNum = podNum;
    }

    uint32_t RegisterRequestMsg::GetPodNum (void) {
        NS_LOG_FUNCTION (this);

        return m_podNum;
    }

    void RegisterRequestMsg::SetRackNum (uint32_t rackNum) {
        NS_LOG_FUNCTION (this << rackNum);

        m_rackNum = rackNum;
    }

    uint32_t RegisterRequestMsg::GetRackNum (void) {
        NS_LOG_FUNCTION (this);

        return m_rackNum;
    }

   void RegisterRequestMsg::SetHostNum (uint32_t hostNum) {
        NS_LOG_FUNCTION (this << hostNum);

        m_hostNum = hostNum;
    }

    uint32_t RegisterRequestMsg::GetHostNum (void) {
        NS_LOG_FUNCTION (this);

        return m_hostNum;
    }

    
    void RegisterRequestMsg::SetIpAddress (Ipv4Address addr) {
        NS_LOG_FUNCTION (this << addr);

        uint32_t intAddr = addr.Get ();
        NS_LOG_LOGIC ("Address in Numeric Form " << intAddr);
        NS_LOG_LOGIC (((intAddr & 0xFF000000) >> 24) << "." <<  ((intAddr & 0x00FF0000) >> 16) << "." <<  ((intAddr & 0x0000FF00) >> 8) << "." << (intAddr & 0x000000FF));
        m_ipAddress = addr;
    }

    Ipv4Address RegisterRequestMsg::GetIpAddress (void) {
        NS_LOG_FUNCTION (this);

        return m_ipAddress;
    }


    uint32_t RegisterRequestMsg::GetSerializedSize (void) const {
        NS_LOG_FUNCTION (this);

        return 16;
    }

    void RegisterRequestMsg::Serialize (Buffer::Iterator start) const {
        NS_LOG_FUNCTION (this << &start);

        Buffer::Iterator i = start;
        i.WriteHtonU32(m_podNum);
        i.WriteHtonU32(m_rackNum);
        i.WriteHtonU32(m_hostNum);
        i.WriteHtonU32(m_ipAddress.Get());
    }

    uint32_t RegisterRequestMsg::Deserialize (Buffer::Iterator start) {
        NS_LOG_FUNCTION (this << &start);

        m_podNum = start.ReadNtohU32 ();
        m_rackNum = start.ReadNtohU32 ();
        m_hostNum = start.ReadNtohU32 ();
        m_ipAddress.Set (start.ReadNtohU32 ());
      
        return 16;
    }

    void RegisterRequestMsg::Print (std::ostream &os) const {
        NS_LOG_FUNCTION (this << &os);

        os << "Pod:Rack:Host = " << m_podNum << ":" << m_rackNum << ":" << m_hostNum << " IP Address = " << m_ipAddress; 
    }


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
        NS_LOG_FUNCTION (this << resultCode);

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

};


