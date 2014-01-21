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
#ifndef HADOOP_NAME_NODE_H
#define HADOOP_NAME_NODE_H

#include "ns3/applications-module.h"
#include "ns3/core-module.h"
#include "ns3/internet-module.h"
#include "ns3/hadoop-module.h"

using namespace std;

namespace ns3 {

class HadoopNameNode : public Application {

public:
    static TypeId GetTypeId(void);
    HadoopNameNode();
    virtual ~HadoopNameNode();

private:
    Ptr<Socket> m_socket2DataNodes;  //Socket listening to Data Nodes
    Ptr<Socket> m_socket2HdfsClients; //Socket listening to HDFS Clients
    Ipv4Address m_ownIpAddress;

    uint32_t m_dataNodeCount;
    Ipv4Address m_dataNodeAddresses [16];

    void StartApplication (void);
    void StopApplication (void);

    bool AcceptDataNodeConnection (Ptr<Socket> socket, const Address& addr);
    void NewDataNodeConnectionCreated (Ptr<Socket> socket, const Address& addr);
    void RecvFromDataNode (Ptr<Socket> socket); 

    bool AcceptHdfsClientConnection (Ptr<Socket> socket, const Address& addr);
    void NewHdfsClientConnectionCreated (Ptr<Socket> socket, const Address& addr);
    void RecvFromHdfsClient (Ptr<Socket> socket); 
};

};

#endif //HADOOP_NAME_NODE_H



#if 0

public:

  /**
   * \param maxBytes the total number of bytes to send
   *
   * Set the total number of bytes to send. Once these bytes are sent, no packet 
   * is sent again, even in on state. The value zero means that there is no 
   * limit.
   */
  void SetMaxBytes (uint32_t maxBytes);

  /**
   * \return pointer to associated socket
   */
  Ptr<Socket> GetSocket (void) const;

 /**
  * Assign a fixed random variable stream number to the random variables
  * used by this model.  Return the number of streams (possibly zero) that
  * have been assigned.
  *
  * \param stream first stream index to use
  * \return the number of stream indices assigned by this model
  */
  int64_t AssignStreams (int64_t stream);

protected:
  virtual void DoDispose (void);
private:
  // inherited from Application base class.
  virtual void StartApplication (void);    // Called at time specified by Start
  virtual void StopApplication (void);     // Called at time specified by Stop

  //helpers
  void CancelEvents ();

  void Construct (Ptr<Node> n,
                  const Address &remote,
                  std::string tid,
                  const RandomVariable& ontime,
                  const RandomVariable& offtime,
                  uint32_t size);


  // Event handlers
  void StartSending ();
  void StopSending ();
  void SendPacket ();

  Ptr<Socket>     m_socket;       // Associated socket
  Address         m_peer;         // Peer address
  bool            m_connected;    // True if connected
  Ptr<RandomVariableStream>  m_onTime;       // rng for On Time
  Ptr<RandomVariableStream>  m_offTime;      // rng for Off Time
  DataRate        m_cbrRate;      // Rate that data is generated
  uint32_t        m_pktSize;      // Size of packets
  uint32_t        m_residualBits; // Number of generated, but not sent, bits
  Time            m_lastStartTime; // Time last packet sent
  uint32_t        m_maxBytes;     // Limit total number of bytes sent
  uint32_t        m_totBytes;     // Total bytes sent so far
  EventId         m_startStopEvent;     // Event id for next start or stop event
  EventId         m_sendEvent;    // Eventid of pending "send packet" event
  bool            m_sending;      // True if currently in sending state
  TypeId          m_tid;
  TracedCallback<Ptr<const Packet> > m_txTrace;

private:
  void ScheduleNextTx ();
  void ScheduleStartEvent ();
  void ScheduleStopEvent ();
  void ConnectionSucceeded (Ptr<Socket> socket);
  void ConnectionFailed (Ptr<Socket> socket);

#endif
