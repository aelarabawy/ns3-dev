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

#ifndef HADOOP_COMMON_H
#define HADOOP_COMMON_H

#include <string>


using namespace std;

namespace ns3 {

#define PORT_NUM_DATA_NODES 8000;
#define PORT_NUM_HDFS_CLIENTS 9000;
#define MAX_PIPELINE_LEN 3
#define DATA_NODE_TRAFFIC_LISTENING_PORT 9002

#define PACKET_SIZE 1000

}; //namespace ns3

#endif /* HADOOP_COMMON_H */

