package com.fantasy.clash.chat_service.utils;

import com.fantasy.clash.chat_service.constants.RedisConstants;
import com.fantasy.clash.framework.redis.utils.RedisUtils;

public class RedisServiceUtils {

  public static String contestGroupChatKey(Long contestId) {
    return RedisUtils.getKey(RedisConstants.CONTEST_GROUP_CHAT_KEY, contestId);
  }

  public static String userLastReadTimestampKey(Long contestId) {
    return RedisUtils.getKey(RedisConstants.USER_LAST_READ_HASH_KEY, contestId);
  }

}
