/*
 * Copyright (c) 2019 R.C
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.aries;

import org.apache.aries.common.IntParameter;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.RETURN_CODE;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketReceiver;

import java.util.List;

public class RelatedSizeInHDFSWrite extends AbstractHDFSToy {

  private final Parameter<Integer> expected_packet_size = IntParameter.newBuilder("rsihw.expected_packet_size").setDescription("Expected packet size in flight, in bytes").opt();

  @Override protected String getParameterPrefix() {
    return "rsihw";
  }

  @Override protected void requisite(List<Parameter> requisites) {
    requisites.add(expected_packet_size);
  }

  @Override protected void exampleConfiguration() {
    example(expected_packet_size.key(), "512");
  }

  DfsClientConf client_conf;
  Configuration conf;

  @Override protected void buildToy(ToyConfiguration configuration) throws Exception {
    super.buildToy(configuration);
    conf = file_system.getConf();
    client_conf = file_system.getClient().getConf();
  }

  @Override protected int haveFun() throws Exception {
    Options.ChecksumOpt checksumOpt = client_conf.getDefaultChecksumOpt();
    LOG.info("Using checksum tpye: " + checksumOpt.getChecksumType().name());
    LOG.info("Checksum bytes: " + checksumOpt.getChecksumType().size);
    LOG.info("Bytes per checksum: " + checksumOpt.getBytesPerChecksum());
    LOG.info("Internal buffer before checksum: " + (checksumOpt.getBytesPerChecksum() * 9));
    LOG.info("Internal buffer for storing checksum: " + (checksumOpt.getChecksumType().size * 9));
    int write_packet_size = client_conf.getWritePacketSize();
    write_packet_size = Math.min(write_packet_size, PacketReceiver.MAX_PACKET_SIZE);
    LOG.info("Write packet size: " + write_packet_size);
    int bodySize = write_packet_size - PacketHeader.PKT_MAX_HEADER_LEN;
    int chunkSize = checksumOpt.getBytesPerChecksum() + checksumOpt.getChecksumType().size;
    int chunksPerPacket = Math.max(bodySize / chunkSize, 1);
    LOG.info("Body size: " + bodySize);
    LOG.info("Chunk size: " + chunkSize);
    LOG.info("Chunks per packet: " + chunksPerPacket);
    int packet_size = chunkSize * chunksPerPacket;
    LOG.info("Packet size: " + packet_size);
    int packet_with_header = PacketHeader.PKT_MAX_HEADER_LEN + packet_size;
    LOG.info("Buf in DFSPacket: " + packet_with_header);
    LOG.info("Max packets in flight: " + client_conf.getWriteMaxPackets());
    LOG.info("Max bytes in flight: " + (client_conf.getWriteMaxPackets() * packet_with_header));

    if (!expected_packet_size.empty()) {
      int expected_packet_sz = expected_packet_size.value() - PacketHeader.PKT_MAX_HEADER_LEN;
      LOG.info("Expected buf in DFSPacket: " + expected_packet_sz);
      int expected_chunks_per_packet = (expected_packet_sz / chunkSize);
      LOG.info("Expected chunks per packet: " + expected_chunks_per_packet);
      int expected_body_size = expected_chunks_per_packet * chunkSize;
      LOG.info("Expected body size: " + expected_body_size);
      LOG.info("Expected write packet size: " + (expected_body_size + PacketHeader.PKT_MAX_HEADER_LEN));
    }
    return RETURN_CODE.SUCCESS.code();
  }

}
