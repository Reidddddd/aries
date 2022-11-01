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

package org.apache.aries.workload.factory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public final class ConnectionFactory {

  private ConnectionFactory() {}

  public static Connection getSelfOwnedConnection(Configuration conf) throws IOException  {
    return org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(conf);
  }

  private static volatile Connection connection;
  public static Connection getSharedConnection(Configuration conf) throws IOException {
    if (connection == null) {
      synchronized (ConnectionFactory.class) {
        if (connection == null) {
          connection = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(conf);
        }
      }
    }
    return connection;
  }

}
