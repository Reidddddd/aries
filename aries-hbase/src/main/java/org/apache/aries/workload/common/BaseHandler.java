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

package org.apache.aries.workload.common;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.aries.common.MetricRegistryInstance;
import org.apache.aries.common.ToyUtils;
import org.apache.aries.workload.factory.ConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public abstract class BaseHandler extends Thread {

  public static final String FAMILY = "target_family";
  public static final String KEY_KIND = "key_kind";
  public static final String KEY_LENGTH = "key_length";
  public static final String VALUE_KINE = "value_kind";
  public static final String RECORDS_NUM = "records_num";
  public static final String SHARED_CONNECTION = "shared_connection";

  protected static final Logger LOG = Logger.getLogger(BaseHandler.class.getName());
  protected static final MetricRegistry registry = MetricRegistryInstance.getMetricRegistry();
  protected static final Timer LATENCY = registry.timer("Latency");
  protected static final Meter READ_ROW = registry.meter("Read_Rows");

  protected volatile MessageDigest digest;

  protected final Connection connection;
  protected final TableName table;
  protected final String family;
  protected final Configuration hbase_conf;
  protected final KEY_PREFIX key_kind;
  protected final int key_length;
  protected final VALUE_KIND value_kind;
  protected final int records_num;
  protected BaseWorkload.Callback callback;

  private final int pad_length;

  public BaseHandler(Configuration conf, TableName table) throws IOException {
    hbase_conf = conf;

    boolean shared_connection = hbase_conf.getBoolean(SHARED_CONNECTION, false);
    connection = shared_connection ?
        ConnectionFactory.getSharedConnection(hbase_conf) :
        ConnectionFactory.getSelfOwnedConnection(hbase_conf);
    LOG.info("Connection created " + connection + " for " + this.getClass().getSimpleName());
    this.table = table;
    family = hbase_conf.get(FAMILY);
    key_kind = hbase_conf.getEnum(KEY_KIND, KEY_PREFIX.NONE);
    key_length = hbase_conf.getInt(KEY_LENGTH, 0);
    value_kind = hbase_conf.getEnum(VALUE_KINE, VALUE_KIND.FIXED);
    digest = getDigest();
    records_num = hbase_conf.getInt(RECORDS_NUM, 0);
    String records_num_in_string = Integer.toString(records_num);
    pad_length = records_num_in_string.length();
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

  protected static final AtomicInteger sequence = new AtomicInteger(1);
  private final Random random = new Random();

  protected String getKey(KEY_PREFIX key_prefix, int key_length, boolean random_key) {
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
      case SEQ: {
        int ran_key = random_key ? random.nextInt(records_num) + 1 : sequence.getAndIncrement();
        String ran_key_in_string = Integer.toString(ran_key);
        String key_without_prefix = ToyUtils.paddingWithZero(pad_length, ran_key_in_string);
        String prefix = key_without_prefix.substring(key_without_prefix.length() - 3);
        key = prefix + ":" + key_without_prefix;
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

  protected byte[] getValue(VALUE_KIND kind, String key, int size_in_bytes) {
    switch (kind) {
      case FIXED:  return ToyUtils.generateBase64Value(key);
      case RANDOM: return ToyUtils.generateRandomBytes(size_in_bytes);
      default:     return ToyUtils.generateRandomBytes(size_in_bytes);
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

  public void setCallback(BaseWorkload.Callback callback) {
    this.callback = callback;
  }

}
