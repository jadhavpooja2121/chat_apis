package com.fantasy.clash.chat_service.utils;

import com.fantasy.clash.chat_service.constants.RedisConstants;
import com.fantasy.clash.framework.redis.utils.RedisUtils;

public class RedisServiceUtils {

  public static String contestGroupChatKey(Long groupChatId) {
    return RedisUtils.getKey(RedisConstants.CONTEST_GROUP_CHAT_KEY, groupChatId);
  }

  public static String userLastReadTimestampKey(Long groupChatId, String username) {
    return RedisUtils.getKey(RedisConstants.USER_LAST_READ_TIMESTAMP_KEY, groupChatId, username);
  }

  public static String userAccountKey(String username) {
    return RedisUtils.getKey(RedisConstants.USER_ACCOUNT_KEY, username);
  }
}
