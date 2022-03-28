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

package org.apache.aries.factory;

import org.apache.aries.ConfigurationFactory;
import org.apache.aries.ToyConfiguration;
import org.apache.aries.common.KEY_PREFIX;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.ToyUtils;
import org.apache.aries.common.VALUE_KIND;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.logging.Logger;

public abstract class HandlerFactory {

  protected final Configuration hbase_conf;

  public HandlerFactory(ToyConfiguration configuration, List<Parameter> parameters) {
    hbase_conf = ConfigurationFactory.createHBaseConfiguration(configuration);

    for (Parameter parameter : parameters) {
      if (parameter.key().contains(BaseHandler.FAMILY))     hbase_conf.set(BaseHandler.FAMILY, (String) parameter.value());
      if (parameter.key().contains(BaseHandler.KEY_KIND))   hbase_conf.setEnum(BaseHandler.KEY_KIND, (KEY_PREFIX) parameter.value());
      if (parameter.key().contains(BaseHandler.KEY_LENGTH)) hbase_conf.setInt(BaseHandler.KEY_LENGTH, (Integer) parameter.value());
      if (parameter.key().contains(BaseHandler.VALUE_KINE)) hbase_conf.setEnum(BaseHandler.VALUE_KINE, (VALUE_KIND) parameter.value());
    }
  }

  public abstract BaseHandler createHandler(TableName table) throws IOException;

  public static abstract class BaseHandler extends Thread {

    static final String FAMILY = "family";
    static final String KEY_KIND = "key_kind";
    static final String KEY_LENGTH = "key_length";
    static final String VALUE_KINE = "value_kind";

    protected static final Logger LOG = Logger.getLogger(BaseHandler.class.getName());

    protected volatile MessageDigest digest;

    protected final Connection connection;
    protected final TableName table;
    protected final String family;
    protected final Configuration hbase_conf;
    protected final KEY_PREFIX key_kind;
    protected final int key_length;
    protected final VALUE_KIND value_kind;

    public BaseHandler(Configuration conf, TableName table) throws IOException {
      hbase_conf = conf;
      connection = ConnectionFactory.createConnection(hbase_conf);
      LOG.info("Connection created " + connection + " for " + this.getClass().getSimpleName());
      this.table = table;
      family = hbase_conf.get(FAMILY);
      key_kind = hbase_conf.getEnum(KEY_KIND, KEY_PREFIX.NONE);
      key_length = hbase_conf.getInt(KEY_LENGTH, 0);
      value_kind = hbase_conf.getEnum(VALUE_KINE, VALUE_KIND.FIXED);
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

    protected TableName getTable() {
      return table;
    }
  }

}