package com.fantasy.clash.chat_service.services;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import com.fantasy.clash.chat_service.constants.RedisConstants;
import com.fantasy.clash.chat_service.dos.SendMessageDO;
import com.fantasy.clash.chat_service.utils.RedisServiceUtils;
import com.fantasy.clash.chat_service.utils.TimeConversionUtils;
import com.fantasy.clash.framework.http.dos.OkResponseDO;
import com.fantasy.clash.framework.redis.cluster.ClusteredRedis;
import com.fantasy.clash.framework.utils.JacksonUtils;
import com.fantasy.clash.framework.utils.StringUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

@Service
public class GroupChatService {
  private static final Logger logger = LoggerFactory.getLogger(GroupChatService.class);

  @Autowired
  private ClusteredRedis redis;

  @Async("apiTaskExecutor")
  public void sendMessage(Long contestId, SendMessageDO sendMessageDO,
      CompletableFuture<ResponseEntity<?>> cf) {
    try {
      redis.zadd(RedisConstants.REDIS_ALIAS, RedisServiceUtils.contestGroupChatKey(contestId),
          TimeConversionUtils.getGMTTime(), JacksonUtils.toJson(sendMessageDO));
      redis.expire(RedisConstants.REDIS_ALIAS, RedisServiceUtils.contestGroupChatKey(contestId),
          RedisConstants.REDIS_24HRS_KEY_TTL);
      cf.complete(ResponseEntity.ok(new OkResponseDO<>(sendMessageDO)));
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
    }
  }

  @Async("apiTaskExecutor")
  public void getMessage(Long contestId, String username, CompletableFuture<ResponseEntity<?>> cf) {
    try {
      Set<String> messages = redis.zrange(RedisConstants.REDIS_ALIAS,
          RedisServiceUtils.contestGroupChatKey(contestId), 1668766871291L, 1668767718948L);
      logger.info("members {}", messages);
      Set<TypedTuple<String>> memberAndScore = redis.zrangeWithScores(RedisConstants.REDIS_ALIAS,
          RedisServiceUtils.contestGroupChatKey(contestId), 0, 10);
      logger.info("memeber, score {}", memberAndScore);

      Set<TypedTuple<String>> userMessages = memberAndScore.stream()
          .filter(tuple -> tuple.getValue().contains(username)).collect(Collectors.toSet());
      logger.info("usrMsgs:{}", userMessages);
      Map<Double, String> usernameMsg = new HashMap<>();
      for (TypedTuple<String> val : userMessages) {
        usernameMsg.put(val.getScore(), val.getValue());
      }
      logger.info("map <time, val> :{}", usernameMsg);

      List<Double> timestamplist = new ArrayList<>();
      for (Map.Entry<Double, String> valMap : usernameMsg.entrySet()) {
        logger.info("username1:{}",
            JacksonUtils.fromJson(valMap.getValue(), SendMessageDO.class).getUsername());
        logger.info("username2 {}", username.toString().replace("\"", ""));
        if (JacksonUtils.fromJson(valMap.getValue(), SendMessageDO.class).getUsername()
            .equals(username.toString().replace("\"", ""))) {
          Double latestTimestamp = valMap.getKey();
          logger.info("latestTimestamp:{}", latestTimestamp);
          timestamplist.add(latestTimestamp);
        } else {
          continue;
        }
      }
      logger.info("timestamplist:{}", timestamplist);
      Double lastreadtime = Collections.max(timestamplist);
      logger.info("lastRead:{}", lastreadtime);
      redis.hmset(RedisConstants.REDIS_ALIAS, RedisServiceUtils.userLastReadHashKey(contestId),
          username, lastreadtime.toString());
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
    }
  }
}
