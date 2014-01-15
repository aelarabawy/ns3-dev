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

#include "hadoop-hdfsclient-datanode-protocol.h"

NS_LOG_COMPONENT_DEFINE ("HadoopHdfsClientDataNodeProtocol");

namespace ns3 {


/*************************************************************************
 *        HdfsClientDataNodeProtocolHeader
 *       (Used for all messages between Name node and Hdfs Clients)
 *************************************************************************/

    NS_OBJECT_ENSURE_REGISTERED (HdfsClientDataNodeProtocolHeader);

    HdfsClientDataNodeProtocolHeader::HdfsClientDataNodeProtocolHeader ()
        : m_msgType(0) {
        NS_LOG_FUNCTION (this);
    }

    HdfsClientDataNodeProtocolHeader::~HdfsClientDataNodeProtocolHeader () {
        NS_LOG_FUNCTION (this);
    }

    TypeId HdfsClientDataNodeProtocolHeader::GetTypeId (void) {
        static TypeId tid = TypeId ("ns3::NameNodeHdfsClientProtocolHeader")
            .SetParent<Header> ()
            .AddConstructor<NameNodeHdfsClientProtocolHeader> ()
        ;

        return tid;
    }

    TypeId HdfsClientDataNodeProtocolHeader::GetInstanceTypeId (void) const {
        NS_LOG_FUNCTION (this);

        return GetTypeId ();
    }

    void HdfsClientDataNodeProtocolHeader::SetMsgType (uint32_t msgType) {
        NS_LOG_FUNCTION (this << msgType);

        m_msgType = msgType;
    }

    uint32_t HdfsClientDataNodeProtocolHeader::GetMsgType (void) {
        NS_LOG_FUNCTION (this);

        return m_msgType;
    }

    uint32_t HdfsClientDataNodeProtocolHeader::GetSerializedSize (void) const {
        NS_LOG_FUNCTION (this);

        return 4;
    }

    void HdfsClientDataNodeProtocolHeader::Serialize (Buffer::Iterator start) const {
        NS_LOG_FUNCTION (this << &start);

        Buffer::Iterator i = start;
        i.WriteHtonU32(m_msgType);
    }

    uint32_t HdfsClientDataNodeProtocolHeader::Deserialize (Buffer::Iterator start) {
        NS_LOG_FUNCTION (this << &start);

        m_msgType = start.ReadNtohU32 ();

        NS_LOG_LOGIC ("MsgType = " << m_msgType);
        return 4;
    }

    void HdfsClientDataNodeProtocolHeader::Print (std::ostream &os) const {
        NS_LOG_FUNCTION (this << &os);

        os << "Msg Type = " << m_msgType; 
    }

/*************************************************************************
 *        HdfsClientPipelineCreateReqMsg  
 *        (Used by the HDFS Client to create the Pipeline with data nodes)
 *************************************************************************/

    NS_OBJECT_ENSURE_REGISTERED (HdfsClientPipelineCreateReqMsg);

    HdfsClientPipelineCreateReqMsg::HdfsClientPipelineCreateReqMsg ()
        : m_blockId (0) {
        NS_LOG_FUNCTION (this);
    }

    HdfsClientPipelineCreateReqMsg::~HdfsClientPipelineCreateReqMsg() {
        NS_LOG_FUNCTION (this);
    }

    TypeId HdfsClientPipelineCreateReqMsg::GetTypeId (void) {
        static TypeId tid = TypeId ("ns3::HdfsClientPipelineCreateReqMsg")
            .SetParent<Header> ()
            .AddConstructor<HdfsClientPipelineCreateReqMsg> ()
        ;

        return tid;
    }

    TypeId HdfsClientPipelineCreateReqMsg::GetInstanceTypeId (void) const {
        NS_LOG_FUNCTION (this);

        return GetTypeId ();
    }

    void HdfsClientPipelineCreateReqMsg::SetBlockId (uint32_t blockId) {
        NS_LOG_FUNCTION (this << blockId);

        m_blockId = blockId;
    }

    uint32_t HdfsClientPipelineCreateReqMsg::GetBlockId (void) {
        NS_LOG_FUNCTION (this);

        return m_blockId;
    }

    void HdfsClientPipelineCreateReqMsg::SetPipelineLen (uint32_t pipelineLen) {
        NS_LOG_FUNCTION (this << pipelineLen);

        m_pipelineLen = pipelineLen;
    }

    uint32_t HdfsClientPipelineCreateReqMsg::GetPipelineLen (void) {
        NS_LOG_FUNCTION (this);
   
        return m_pipelineLen;
    }

    void HdfsClientPipelineCreateReqMsg::SetPipeline (Ipv4Address address, uint8_t order) {
        NS_LOG_FUNCTION (this << address << order);

        m_pipeline [order] = address;
    }

    Ipv4Address HdfsClientPipelineCreateReqMsg::GetPipeline (uint8_t order) {
        NS_LOG_FUNCTION (this << order);

        return m_pipeline [order];
    }

    uint32_t HdfsClientPipelineCreateReqMsg::GetSerializedSize (void) const {
        NS_LOG_FUNCTION (this);

        return (8 + 4 * m_pipelineLen);
    }

    void HdfsClientPipelineCreateReqMsg::Serialize (Buffer::Iterator start) const {
        NS_LOG_FUNCTION (this << &start);
      
        Buffer::Iterator i = start;

        i.WriteHtonU32(m_blockId);
        i.WriteHtonU32(m_pipelineLen);
        
        for (uint32_t index = 0; index < m_pipelineLen; ++index) {
            i.WriteHtonU32(m_pipeline[index].Get());
        }
    }

    uint32_t HdfsClientPipelineCreateReqMsg::Deserialize (Buffer::Iterator start) {
        NS_LOG_FUNCTION (this << &start);

        m_blockId = start.ReadNtohU32 ();
        m_pipelineLen = start.ReadNtohU32 ();

        for (uint32_t index = 0; index < m_pipelineLen; ++index) {
            m_pipeline[index].Set(start.ReadNtohU32 ());
        }

        return (8 + 4 * m_pipelineLen);
    }

    void HdfsClientPipelineCreateReqMsg::Print (std::ostream &os) const {
        NS_LOG_FUNCTION (this << &os);

        os << "BlockId = "  << m_blockId ;
        os << "Pipeline len = " << m_pipelineLen << "Pipeline = ";
       
        for (uint32_t index = 0; index < m_pipelineLen; ++index) {
            m_pipeline [index].Print(os);
        }
    }


/*************************************************************************
 *         HdfsClientPipelineCreateRepMsg
 *        (Used by the Data Node to respond to the Hdfs Client request)
 *************************************************************************/

    NS_OBJECT_ENSURE_REGISTERED (HdfsClientPipelineCreateRepMsg);

    HdfsClientPipelineCreateRepMsg::HdfsClientPipelineCreateRepMsg ()
       : m_resultCode (0),
         m_blockId (0) {
        NS_LOG_FUNCTION (this);
    }

    HdfsClientPipelineCreateRepMsg::~HdfsClientPipelineCreateRepMsg() {
        NS_LOG_FUNCTION (this);
    }

    TypeId HdfsClientPipelineCreateRepMsg::GetTypeId (void) {
        static TypeId tid = TypeId ("ns3::HdfsClientPipelineCreateRepMsg")
            .SetParent<Header> ()
            .AddConstructor<HdfsClientPipelineCreateRepMsg> ()
        ;

        return tid;
    }

    TypeId HdfsClientPipelineCreateRepMsg::GetInstanceTypeId (void) const {
        NS_LOG_FUNCTION (this);

        return GetTypeId ();
    }

    void HdfsClientPipelineCreateRepMsg::SetResultCode (uint32_t resultCode) {
        NS_LOG_FUNCTION (this << resultCode);

        m_resultCode = resultCode;
    }

    uint32_t HdfsClientPipelineCreateRepMsg::GetResultCode (void) {
        NS_LOG_FUNCTION (this);

        return m_resultCode;
    }

 
    void HdfsClientPipelineCreateRepMsg::SetBlockId (uint32_t blockId) {
        NS_LOG_FUNCTION (this << blockId);

        m_blockId = blockId;
    }

    uint32_t HdfsClientPipelineCreateRepMsg::GetBlockId (void) {
        NS_LOG_FUNCTION (this);

        return m_blockId;
    }

   uint32_t HdfsClientPipelineCreateRepMsg::GetSerializedSize (void) const {
        NS_LOG_FUNCTION (this);

        return 8;
    }

    void HdfsClientPipelineCreateRepMsg::Serialize (Buffer::Iterator start) const {
        NS_LOG_FUNCTION (this << &start);

        Buffer::Iterator i = start;
                
        i.WriteHtonU32(m_resultCode);
        i.WriteHtonU32(m_blockId);
    }

    uint32_t HdfsClientPipelineCreateRepMsg::Deserialize (Buffer::Iterator start) {
        NS_LOG_FUNCTION (this << &start);

        m_resultCode = start.ReadNtohU32 ();
        m_blockId = start.ReadNtohU32 ();

        NS_LOG_LOGIC ("Result Code = " << m_resultCode << " Block Id = " << m_blockId);
        return 8;
    }

    void HdfsClientPipelineCreateRepMsg::Print (std::ostream &os) const {
        NS_LOG_FUNCTION (this << &os);

        os <<  "ResultCode = " << m_resultCode << " Block Id = " << m_blockId ; 
    }



/*************************************************************************
 *         HdfsClientPacketAckMsg
 *        (Used by the Data Node to respond to the Hdfs Client request)
 *************************************************************************/

    NS_OBJECT_ENSURE_REGISTERED (HdfsClientPacketAckMsg);

    HdfsClientPacketAckMsg::HdfsClientPacketAckMsg ()
       : m_resultCode (0),
         m_blockId (0),
         m_packetId (0) {
        NS_LOG_FUNCTION (this);
    }

    HdfsClientPacketAckMsg::~HdfsClientPacketAckMsg() {
        NS_LOG_FUNCTION (this);
    }

    TypeId HdfsClientPacketAckMsg::GetTypeId (void) {
        static TypeId tid = TypeId ("ns3::HdfsClientPacketAckMsg")
            .SetParent<Header> ()
            .AddConstructor<HdfsClientPacketAckMsg> ()
        ;

        return tid;
    }

    TypeId HdfsClientPacketAckMsg::GetInstanceTypeId (void) const {
        NS_LOG_FUNCTION (this);

        return GetTypeId ();
    }

    void HdfsClientPacketAckMsg::SetResultCode (uint32_t resultCode) {
        NS_LOG_FUNCTION (this << resultCode);

        m_resultCode = resultCode;
    }

    uint32_t HdfsClientPacketAckMsg::GetResultCode (void) {
        NS_LOG_FUNCTION (this);

        return m_resultCode;
    }

 
    void HdfsClientPacketAckMsg::SetBlockId (uint32_t blockId) {
        NS_LOG_FUNCTION (this << blockId);

        m_blockId = blockId;
    }

    uint32_t HdfsClientPacketAckMsg::GetBlockId (void) {
        NS_LOG_FUNCTION (this);

        return m_blockId;
    }

    void HdfsClientPacketAckMsg::SetPacketId (uint32_t packetId) {
        NS_LOG_FUNCTION (this << packetId);

        m_packetId = packetId;
    }

    uint32_t HdfsClientPacketAckMsg::GetPacketId (void) {
        NS_LOG_FUNCTION (this);

        return m_packetId;
    }


   uint32_t HdfsClientPacketAckMsg::GetSerializedSize (void) const {
        NS_LOG_FUNCTION (this);

        return 12;
    }

    void HdfsClientPacketAckMsg::Serialize (Buffer::Iterator start) const {
        NS_LOG_FUNCTION (this << &start);

        Buffer::Iterator i = start;
                
        i.WriteHtonU32(m_resultCode);
        i.WriteHtonU32(m_blockId);
        i.WriteHtonU32(m_packetId);
    }

    uint32_t HdfsClientPacketAckMsg::Deserialize (Buffer::Iterator start) {
        NS_LOG_FUNCTION (this << &start);

        m_resultCode = start.ReadNtohU32 ();
        m_blockId = start.ReadNtohU32 ();
        m_packetId = start.ReadNtohU32 ();

        NS_LOG_LOGIC ("Result Code = " << m_resultCode << " Block Id = " << m_blockId << " Packet Id = " << m_packetId);
        return 12;
    }

    void HdfsClientPacketAckMsg::Print (std::ostream &os) const {
        NS_LOG_FUNCTION (this << &os);

        os <<  "ResultCode = " << m_resultCode << " Block Id = " << m_blockId << " PacketId = " << m_packetId; 
    }

};

