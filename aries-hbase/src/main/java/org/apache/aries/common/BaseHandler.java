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

package org.apache.aries.common;

import org.apache.aries.ConfigurationFactory;
import org.apache.aries.ToyConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Logger;

public abstract class BaseHandler implements Runnable {
  protected static final Logger LOG = Logger.getLogger(BaseHandler.class.getName());

  protected Connection connection;
  protected volatile MessageDigest digest;

  public BaseHandler(ToyConfiguration conf) throws IOException {
    connection = createConnection(conf);
    LOG.info("Connection created " + connection);
    digest = getDigest();
  }

  private MessageDigest getDigest() {
    if (digest == null) {
      synchronized (BaseHandler.class) {
        if (digest == null) {
          try {
            digest = MessageDigest.getInstance("MD5");
          } catch (NoSuchAlgorithmException nsae) {
            // rarely happen
            throw new RuntimeException(nsae.getCause());
          }
        }
      }
    }
    return digest;
  }

  Connection createConnection(ToyConfiguration conf) throws IOException {
    Configuration hbase_conf = ConfigurationFactory.createHBaseConfiguration(conf);
    return ConnectionFactory.createConnection(hbase_conf);
  }

  protected String getKey(KEY_PREFIX key_prefix, int key_length) {
    String key = ToyUtils.generateRandomString(key_length);
    switch (key_prefix) {
      case HEX: {
        byte[] digested = digest.digest(Bytes.toBytes(key));
        String md5 = new BigInteger(1, digested).toString(16).toLowerCase();
        key = md5.substring(0, 2) + ":" + key;
        break;
      }
      case DEC: {
        byte[] digested = digest.digest(Bytes.toBytes(key));
        String md5 = new BigInteger(1, digested).toString(10).toLowerCase();
        key = md5.substring(md5.length() - 2) + ":" + key;
        break;
      }
      case NONE: {
        break;
      }
      default:
        throw new IllegalStateException("Unexpected value: " + key_prefix);
    }
    return key;
  }

  protected byte[] getValue(VALUE_KIND kind, String key) {
    switch (kind) {
      case FIXED:  return ToyUtils.generateBase64Value(key);
      case RANDOM: return Bytes.toBytes(ToyUtils.generateRandomString(22));
      default:     return Bytes.toBytes(ToyUtils.generateRandomString(22));
    }
  }

  protected boolean verifiedResult(VALUE_KIND kind, String key, byte[] value) {
    if (kind == VALUE_KIND.RANDOM) {
      return false;
    }
    int res = Bytes.compareTo(value, ToyUtils.generateBase64Value(key));
    return res == 0;
  }

}
