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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public final class ToyUtils {

  public final static String PARAMETER_SEPARATOR = ".";

  public static void assertLengthValid(String[] res, int expected) {
    if (res.length != expected) {
      throw new IllegalArgumentException();
    }
  }

  public static String arrayToString(Object[] arr) {
    StringBuilder builder = new StringBuilder();
    for (Object a : arr) {
      builder.append(a.toString()).append(",");
    }
    String s = builder.toString();
    return s.substring(0, s.lastIndexOf(","));
  }

  private static String RANDOM_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789ABCEDEFGHIJKLMNOPQRSTUVWXYZ";
  public static String generateRandomString(int size) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < size; i++) {
      builder.append(RANDOM_CHARS.charAt((int)(Math.random() * RANDOM_CHARS.length())));
    }
    return builder.toString();
  }

  private static Random random = new Random();
  public static byte[] generateRandomBytes(int size_in_bytes) {
    byte[] res = new byte[size_in_bytes];
    random.nextBytes(res);
    return res;
  }

  public static byte[] generateBase64Value(String key) {
    return Base64.getEncoder().encode(key.getBytes(StandardCharsets.UTF_8));
  }

  public static String paddingWithZero(int size, String to_be_padded) {
    if (size == to_be_padded.length()) return to_be_padded;

    StringBuilder leadingZero = new StringBuilder();
    int pad = size - to_be_padded.length();
    while (pad-- > 0) leadingZero.append("0");
    return leadingZero.append(to_be_padded).toString();
  }

  public static String buildError(Throwable t) {
    StringBuilder errorBuilder = new StringBuilder(System.lineSeparator());
    errorBuilder.append("cause: " + t.getCause() + System.lineSeparator())
                .append("message: " + t.getMessage() + System.lineSeparator())
                .append("stacktrace:" + System.lineSeparator());
    for (StackTraceElement ste : t.getStackTrace()) {
      errorBuilder.append(ste + System.lineSeparator());
    }
    return errorBuilder.toString();
  }

  public static long getTimeoutInMilliSeconds(int timeout_in_seconds) {
    return TimeUnit.MILLISECONDS.convert(timeout_in_seconds, TimeUnit.SECONDS);
  }

  public static long getTimeoutInSeconds(long timeout_in_milli_seconds) {
    return TimeUnit.SECONDS.convert(timeout_in_milli_seconds, TimeUnit.MILLISECONDS);
  }
}

