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

        NS_LOG_LOGIC ("GetStringSerializedSize: " << (4 + str.length()));
        return (4 + str.length());
    }

    void StringSerialize (Buffer::Iterator start, string str) {

        NS_LOG_LOGIC ("StringSerialize: " << str);

        start.WriteHtonU32(str.length());
        start.Write(reinterpret_cast<const uint8_t*> (str.data()),str.length());
    }

    uint32_t StringDeserialize (Buffer::Iterator start, string& str) {

        uint32_t len = start.ReadNtohU32 ();
        uint8_t uintArray [50];
        start.Read (uintArray, len);
        uintArray[len] = 0;

        char * charArray = (char*) uintArray;
        str = string(charArray, len);

        NS_LOG_LOGIC ("StringDeserialize: " << str);

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
        NS_LOG_FUNCTION (this << msgType);

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

        NS_LOG_LOGIC ("MsgType = " << m_msgType);
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
        NS_LOG_FUNCTION (this << fileName);

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

        StringDeserialize(start, m_fileName);
        NS_LOG_LOGIC ("File Name = " << m_fileName);
        return (GetStringSerializedSize (m_fileName));
    }

    void HdfsClientFileCreateReqMsg::Print (std::ostream &os) const {
        NS_LOG_FUNCTION (this << &os);
    
        os << "Filename = " << m_fileName; 
    }


/*************************************************************************
 *        HdfsClientFileCreateRepMsg 
 *        (Used by the Name Node to respond to the Hdfs Client request)
 *************************************************************************/

    NS_OBJECT_ENSURE_REGISTERED (HdfsClientFileCreateRepMsg);

    HdfsClientFileCreateRepMsg::HdfsClientFileCreateRepMsg ()
       : m_resultCode (0),
         m_fileId (0),
         m_fileName("") {
        NS_LOG_FUNCTION (this);
    }

    HdfsClientFileCreateRepMsg::~HdfsClientFileCreateRepMsg() {
        NS_LOG_FUNCTION (this);
    }

    TypeId HdfsClientFileCreateRepMsg::GetTypeId (void) {
        static TypeId tid = TypeId ("ns3::HdfsClientFileCreateRepMsg")
            .SetParent<Header> ()
            .AddConstructor<HdfsClientFileCreateRepMsg> ()
        ;

        return tid;
    }

    TypeId HdfsClientFileCreateRepMsg::GetInstanceTypeId (void) const {
        NS_LOG_FUNCTION (this);

        return GetTypeId ();
    }

    void HdfsClientFileCreateRepMsg::SetFileName (string fileName) {
        NS_LOG_FUNCTION (this << fileName);

        m_fileName = fileName;
    }

    string HdfsClientFileCreateRepMsg::GetFileName (void) {
        NS_LOG_FUNCTION (this);

        return m_fileName;
    }

    void HdfsClientFileCreateRepMsg::SetFileId (uint32_t fileId) {
        NS_LOG_FUNCTION (this << fileId);

        m_fileId = fileId;
    }

    uint32_t HdfsClientFileCreateRepMsg::GetFileId (void) {
        NS_LOG_FUNCTION (this);

        return m_fileId;
    }

    void HdfsClientFileCreateRepMsg::SetResultCode (uint32_t resultCode) {
        NS_LOG_FUNCTION (this << resultCode);

        m_resultCode = resultCode;
    }

    uint32_t HdfsClientFileCreateRepMsg::GetResultCode (void) {
        NS_LOG_FUNCTION (this);

        return m_resultCode;
    }

    uint32_t HdfsClientFileCreateRepMsg::GetSerializedSize (void) const {
        NS_LOG_FUNCTION (this);

        return (8 + GetStringSerializedSize (m_fileName));
    }

    void HdfsClientFileCreateRepMsg::Serialize (Buffer::Iterator start) const {
        NS_LOG_FUNCTION (this << &start);

        Buffer::Iterator i = start;
                
        i.WriteHtonU32(m_resultCode);
        i.WriteHtonU32(m_fileId);
        StringSerialize (i, m_fileName);
    }

    uint32_t HdfsClientFileCreateRepMsg::Deserialize (Buffer::Iterator start) {
        NS_LOG_FUNCTION (this << &start);

        m_resultCode = start.ReadNtohU32 ();
        m_fileId = start.ReadNtohU32 ();
        StringDeserialize(start, m_fileName);

        NS_LOG_LOGIC ("Result Code = " << m_resultCode << " File Id = " << m_fileId << " File Name = " << m_fileName);
        return (8 + GetStringSerializedSize (m_fileName));
    }

    void HdfsClientFileCreateRepMsg::Print (std::ostream &os) const {
        NS_LOG_FUNCTION (this << &os);
        os << "Filename = " << m_fileName << " FileId = " << m_fileId << " ResultCode = " << m_resultCode; 
    }

/*************************************************************************
 *        HdfsClientFileBlockAddReqMsg 
 *        (Used by the HDFS Client to add a Block for an HDFS file)
 *************************************************************************/

    NS_OBJECT_ENSURE_REGISTERED (HdfsClientFileBlockAddReqMsg);

    HdfsClientFileBlockAddReqMsg::HdfsClientFileBlockAddReqMsg ()
        : m_fileId(0) {
        NS_LOG_FUNCTION (this);
    }

    HdfsClientFileBlockAddReqMsg::~HdfsClientFileBlockAddReqMsg() {
        NS_LOG_FUNCTION (this);
    }

    TypeId HdfsClientFileBlockAddReqMsg::GetTypeId (void) {
        static TypeId tid = TypeId ("ns3::HdfsClientFileBlockAddReqMsg")
            .SetParent<Header> ()
            .AddConstructor<HdfsClientFileBlockAddReqMsg> ()
        ;

        return tid;
    }

    TypeId HdfsClientFileBlockAddReqMsg::GetInstanceTypeId (void) const {
        NS_LOG_FUNCTION (this);

        return GetTypeId ();
    }

    void HdfsClientFileBlockAddReqMsg::SetFileId (uint32_t fileId) {
        NS_LOG_FUNCTION (this << fileId);

        m_fileId = fileId;
    }

    uint32_t HdfsClientFileBlockAddReqMsg::GetFileId (void) {
        NS_LOG_FUNCTION (this);

        return m_fileId;
    }


    uint32_t HdfsClientFileBlockAddReqMsg::GetSerializedSize (void) const {
        NS_LOG_FUNCTION (this);

        return 4;
    }

    void HdfsClientFileBlockAddReqMsg::Serialize (Buffer::Iterator start) const {
        NS_LOG_FUNCTION (this << &start);

        Buffer::Iterator i = start;
        i.WriteHtonU32(m_fileId);
    }

    uint32_t HdfsClientFileBlockAddReqMsg::Deserialize (Buffer::Iterator start) {
        NS_LOG_FUNCTION (this << &start);

        m_fileId = start.ReadNtohU32 ();
        NS_LOG_LOGIC ("File Id = " << m_fileId);
        return 4;
    }

    void HdfsClientFileBlockAddReqMsg::Print (std::ostream &os) const {
        NS_LOG_FUNCTION (this << &os);

        os << "FileId = " << m_fileId; 
    }


/*************************************************************************
 *        HdfsClientFileBlockAddRepMsg 
 *        (Used by the Name node to respond to the HDFS Client)
 *************************************************************************/

    NS_OBJECT_ENSURE_REGISTERED (HdfsClientFileBlockAddRepMsg);

    HdfsClientFileBlockAddRepMsg::HdfsClientFileBlockAddRepMsg ()
        : m_resultCode (0),
          m_fileId (0),
          m_blockId (0),
          m_blockSize (0) {
        NS_LOG_FUNCTION (this);
    }

    HdfsClientFileBlockAddRepMsg::~HdfsClientFileBlockAddRepMsg() {
        NS_LOG_FUNCTION (this);
    }

    TypeId HdfsClientFileBlockAddRepMsg::GetTypeId (void) {
        static TypeId tid = TypeId ("ns3::HdfsClientFileBlockAddRepMsg")
            .SetParent<Header> ()
            .AddConstructor<HdfsClientFileBlockAddRepMsg> ()
        ;

        return tid;
    }

    TypeId HdfsClientFileBlockAddRepMsg::GetInstanceTypeId (void) const {
        NS_LOG_FUNCTION (this);

        return GetTypeId ();
    }

    void HdfsClientFileBlockAddRepMsg::SetResultCode (uint32_t resultCode) {
        NS_LOG_FUNCTION (this << resultCode);

        m_resultCode = resultCode;
    }

    uint32_t HdfsClientFileBlockAddRepMsg::GetResultCode (void) {
        NS_LOG_FUNCTION (this);

        return m_resultCode;
    }

   void HdfsClientFileBlockAddRepMsg::SetFileId (uint32_t fileId) {
        NS_LOG_FUNCTION (this << fileId);
        m_fileId = fileId;
    }

    uint32_t HdfsClientFileBlockAddRepMsg::GetFileId (void) {
        NS_LOG_FUNCTION (this);

        return m_fileId;
    }

   void HdfsClientFileBlockAddRepMsg::SetBlockId (uint32_t blockId) {
        NS_LOG_FUNCTION (this << blockId);

        m_blockId = blockId;
    }

    uint32_t HdfsClientFileBlockAddRepMsg::GetBlockId (void) {
        NS_LOG_FUNCTION (this);

        return m_blockId;
    }

   void HdfsClientFileBlockAddRepMsg::SetBlockSize (uint32_t blockSize) {
        NS_LOG_FUNCTION (this << blockSize);

        m_blockSize = blockSize;
    }

    uint32_t HdfsClientFileBlockAddRepMsg::GetBlockSize (void) {
        NS_LOG_FUNCTION (this);

        return m_blockSize;
    }

    void HdfsClientFileBlockAddRepMsg::SetPipelineLen (uint32_t pipelineLen) {
        NS_LOG_FUNCTION (this << pipelineLen);

        m_pipelineLen = pipelineLen;
    }

    uint32_t HdfsClientFileBlockAddRepMsg::GetPipelineLen (void) {
        NS_LOG_FUNCTION (this);
   
        return m_pipelineLen;
    }

    void HdfsClientFileBlockAddRepMsg::SetPipeline (Ipv4Address address, uint8_t order) {
        NS_LOG_FUNCTION (this << address << order);

        m_pipeline [order] = address;
    }

    Ipv4Address HdfsClientFileBlockAddRepMsg::GetPipeline (uint8_t order) {
        NS_LOG_FUNCTION (this << order);

        return m_pipeline [order];
    }

    uint32_t HdfsClientFileBlockAddRepMsg::GetSerializedSize (void) const {
        NS_LOG_FUNCTION (this);

        return (20 + 4 * m_pipelineLen);
    }

    void HdfsClientFileBlockAddRepMsg::Serialize (Buffer::Iterator start) const {
        NS_LOG_FUNCTION (this << &start);
      
        Buffer::Iterator i = start;

        i.WriteHtonU32(m_resultCode);
        i.WriteHtonU32(m_fileId);
        i.WriteHtonU32(m_blockId);
        i.WriteHtonU32(m_blockSize);
        i.WriteHtonU32(m_pipelineLen);
        
        for (uint32_t index = 0; index < m_pipelineLen; ++index) {
            i.WriteHtonU32(m_pipeline[index].Get());
        }
    }

    uint32_t HdfsClientFileBlockAddRepMsg::Deserialize (Buffer::Iterator start) {
        NS_LOG_FUNCTION (this << &start);

        m_resultCode = start.ReadNtohU32 ();
        m_fileId = start.ReadNtohU32 ();
        m_blockId = start.ReadNtohU32 ();
        m_blockSize = start.ReadNtohU32 ();
        m_pipelineLen = start.ReadNtohU32 ();

        for (uint32_t index = 0; index < m_pipelineLen; ++index) {
            m_pipeline[index].Set(start.ReadNtohU32 ());
        }

        NS_LOG_LOGIC ("File Id = " << m_fileId);

        return (20 + 4 * m_pipelineLen);
    }

    void HdfsClientFileBlockAddRepMsg::Print (std::ostream &os) const {
        NS_LOG_FUNCTION (this << &os);

        os << "FileId:BlockId = " << m_fileId << ":" << m_blockId << " BlockSize = " << m_blockSize << " Result Code = " << m_resultCode ;
        os << "Pipeline len = " << m_pipelineLen << "Pipeline = ";
       
        for (uint32_t index = 0; index < m_pipelineLen; ++index) {
            m_pipeline [index].Print(os);
        }
    }


/*************************************************************************
 *        HdfsClientBlockCompleteMsg 
 *        (Used by the HDFS Client to report completion of block transfer)
 *************************************************************************/

    NS_OBJECT_ENSURE_REGISTERED (HdfsClientBlockCompleteMsg);

    HdfsClientBlockCompleteMsg::HdfsClientBlockCompleteMsg ()
        : m_resultCode (0),
          m_blockId (0) {
        NS_LOG_FUNCTION (this);
    }

    HdfsClientBlockCompleteMsg::~HdfsClientBlockCompleteMsg() {
        NS_LOG_FUNCTION (this);
    }

    TypeId HdfsClientBlockCompleteMsg::GetTypeId (void) {
        static TypeId tid = TypeId ("ns3::HdfsClientBlockCompleteMsg")
            .SetParent<Header> ()
            .AddConstructor<HdfsClientBlockCompleteMsg> ()
        ;

        return tid;
    }

    TypeId HdfsClientBlockCompleteMsg::GetInstanceTypeId (void) const {
        NS_LOG_FUNCTION (this);

        return GetTypeId ();
    }

    void HdfsClientBlockCompleteMsg::SetResultCode (uint32_t resultCode) {
        NS_LOG_FUNCTION (this << resultCode);

        m_resultCode = resultCode;
    }

    uint32_t HdfsClientBlockCompleteMsg::GetResultCode (void) {
        NS_LOG_FUNCTION (this);

        return m_resultCode;
    }


   void HdfsClientBlockCompleteMsg::SetBlockId (uint32_t blockId) {
        NS_LOG_FUNCTION (this << blockId);

        m_blockId = blockId;
    }

    uint32_t HdfsClientBlockCompleteMsg::GetBlockId (void) {
        NS_LOG_FUNCTION (this);

        return m_blockId;
    }


    uint32_t HdfsClientBlockCompleteMsg::GetSerializedSize (void) const {
        NS_LOG_FUNCTION (this);

        return 8;
    }

    void HdfsClientBlockCompleteMsg::Serialize (Buffer::Iterator start) const {
        NS_LOG_FUNCTION (this << &start);
      
        Buffer::Iterator i = start;

        i.WriteHtonU32(m_resultCode);
        i.WriteHtonU32(m_blockId);
    }

    uint32_t HdfsClientBlockCompleteMsg::Deserialize (Buffer::Iterator start) {
        NS_LOG_FUNCTION (this << &start);

        m_resultCode = start.ReadNtohU32 ();
        m_blockId = start.ReadNtohU32 ();

        return 8;
    }

    void HdfsClientBlockCompleteMsg::Print (std::ostream &os) const {
        NS_LOG_FUNCTION (this << &os);

        os << "BlockId = " << m_blockId << " Result Code = " << m_resultCode ;
    }

};

