package com.fantasy.clash.chat_service.constants;

public class RedisConstants {
  private RedisConstants() {

  }

  public static final String REDIS_CLUSTER_NODES_CONFIG_KEY = "redis.cluster.nodes";
  public static final String REDIS_ALIAS = "chat_service";
  public static final String CONTEST_GROUP_CHAT_KEY = "r:chat_service:chat";
  public static final Long REDIS_24HRS_KEY_TTL = 24L * 60L * 60L * 1000;
  public static final String USER_LAST_READ_TIMESTAMP_KEY = "r:chat_service:chat:members";
}
