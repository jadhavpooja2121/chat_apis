package com.fantasy.clash.chat_service.utils;

import java.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashUtils {
  private static final Logger logger = LoggerFactory.getLogger(HashUtils.class);

  public static String getSmallestString(String username, String otherUsername) {
    int result = username.compareTo(otherUsername);
    if (result < 0) {
      return username;
    } else {
      return otherUsername;
    }
  }

  public static final String getHash(String username, String otherUsername) {
    String smallestString = getSmallestString(username, otherUsername);
    String appendedString;
    if (smallestString.equals(username)) {
      StringBuilder s1 = new StringBuilder(username);
      StringBuilder s2 = new StringBuilder(otherUsername);
      StringBuilder s3 = s1.append(s2);
      appendedString = s3.toString();
    } else {
      StringBuilder s1 = new StringBuilder(otherUsername);
      StringBuilder s2 = new StringBuilder(username);
      StringBuilder s3 = s1.append(s2);
      appendedString = s3.toString();
    }

    Base64.Encoder encoder = Base64.getEncoder();
    String hash = encoder.encodeToString(appendedString.getBytes());
    return hash;
  }
}
