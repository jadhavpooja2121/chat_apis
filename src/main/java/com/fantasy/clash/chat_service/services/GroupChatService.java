package com.fantasy.clash.chat_service.services;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.springframework.util.CollectionUtils;
import com.fantasy.clash.chat_service.constants.RedisConstants;
import com.fantasy.clash.chat_service.constants.ResponseErrorCodes;
import com.fantasy.clash.chat_service.constants.ResponseErrorMessages;
import com.fantasy.clash.chat_service.dos.GetMessageResponseDO;
import com.fantasy.clash.chat_service.dos.MessageNotificationDO;
import com.fantasy.clash.chat_service.dos.SendMessageDO;
import com.fantasy.clash.chat_service.dos.SendMessageResponseDO;
import com.fantasy.clash.chat_service.utils.RedisServiceUtils;
import com.fantasy.clash.chat_service.utils.TimeConversionUtils;
import com.fantasy.clash.framework.http.dos.ErrorResponseDO;
import com.fantasy.clash.framework.http.dos.OkResponseDO;
import com.fantasy.clash.framework.redis.cluster.ClusteredRedis;
import com.fantasy.clash.framework.utils.JacksonUtils;
import com.fantasy.clash.framework.utils.StringUtils;

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
  public void getMessage(Long contestId, String username, Long timestamp, boolean isNext,
      CompletableFuture<ResponseEntity<?>> cf) {
    try {
      String lastReadTimestamp = redis.hget(RedisConstants.REDIS_ALIAS,
          RedisServiceUtils.userLastReadTimestampKey(contestId), username);
      logger.info("user lastReadtime {}", lastReadTimestamp);
      Long min = timestamp;
      logger.info("conversion of sql timestamp to long {}", min);
      Long max = Long.MAX_VALUE;
      if (lastReadTimestamp == null) {
        // TODO: change count to 100
        Set<TypedTuple<String>> membersWithScores =
            redis.zrangeByScoreWithScores(RedisConstants.REDIS_ALIAS,
                RedisServiceUtils.contestGroupChatKey(contestId), min, max, 0, 3);
        logger.info("membersWithScores {}", membersWithScores);


        if (CollectionUtils.isEmpty(membersWithScores)) {
          ErrorResponseDO noMessagesResponseDO = new ErrorResponseDO(
              ResponseErrorCodes.EMPTY_GROUP_CHAT, ResponseErrorMessages.EMPTY_GROUP_CHAT);
          cf.complete(ResponseEntity.ok(noMessagesResponseDO));
        }

        Map<Double, String> usernameMsgTimestampMap = new HashMap<>();
        for (TypedTuple<String> val : membersWithScores) {
          usernameMsgTimestampMap.put(val.getScore(), val.getValue());
        }
        logger.info("map <time, val> :{}", usernameMsgTimestampMap);

        List<Double> userMsgsTimestampList = new ArrayList<>();
        for (Map.Entry<Double, String> valMap : usernameMsgTimestampMap.entrySet()) {
          Double latestTimestamp = valMap.getKey();
          logger.info("latestTimestamp:{}", latestTimestamp);
          userMsgsTimestampList.add(latestTimestamp);
        }
        logger.info("userMsgsTimestampList:{}", userMsgsTimestampList);
        Long lastreadtime = Collections.max(userMsgsTimestampList).longValue();
        logger.info("lastRead:{}", lastreadtime);

        List<SendMessageResponseDO> messageList = new ArrayList<>();
        for (Map.Entry<Double, String> valMap : usernameMsgTimestampMap.entrySet()) {
          SendMessageDO sendMessageDO =
              JacksonUtils.fromJson(valMap.getValue(), SendMessageDO.class);
          messageList.add(new SendMessageResponseDO(sendMessageDO.getUsername(),
              sendMessageDO.getMessage(), valMap.getKey().longValue(), true));
        }

        logger.info("messageList {}",JacksonUtils.toJson(messageList));
        redis.hmset(RedisConstants.REDIS_ALIAS,
            RedisServiceUtils.userLastReadTimestampKey(contestId), username,
            lastreadtime.toString());

        List<SendMessageDO> messages = usernameMsgTimestampMap.values().stream().map(str -> {
          try {
            return JacksonUtils.fromJson(str, SendMessageDO.class);
          } catch (Exception e) {
            logger.error("Exception due to {}", StringUtils.printStackTrace(e));
          }
          return null;
        }).collect(Collectors.toList());
        logger.info("all messages list:{}", messages);

        cf.complete(ResponseEntity.ok(new OkResponseDO<>(new GetMessageResponseDO(messages))));
        return;
      } else {

        String prevReadtime = redis.hget(RedisConstants.REDIS_ALIAS,
            RedisServiceUtils.userLastReadTimestampKey(contestId), username);
        logger.info("user lastReadtime {}", prevReadtime);

        min = Long.valueOf(prevReadtime).longValue();
        Set<TypedTuple<String>> membersWithScores1 =
            redis.zrangeByScoreWithScores(RedisConstants.REDIS_ALIAS,
                RedisServiceUtils.contestGroupChatKey(contestId), min, max, 1, 3);
        logger.info("membersWithScores {}", membersWithScores1);

        Map<Double, String> usernameMsgTimestampMap = new HashMap<>();
        for (TypedTuple<String> val : membersWithScores1) {
          usernameMsgTimestampMap.put(val.getScore(), val.getValue());
        }
        logger.info("map <time, val> :{}", usernameMsgTimestampMap);

        List<Double> userMsgsTimestampList = new ArrayList<>();
        for (Map.Entry<Double, String> valMap : usernameMsgTimestampMap.entrySet()) {
          Double latestTimestamp = valMap.getKey();
          logger.info("latestTimestamp:{}", latestTimestamp);
          userMsgsTimestampList.add(latestTimestamp);
        }
        logger.info("userMsgsTimestampList:{}", userMsgsTimestampList);
        Long lastreadtime = Collections.max(userMsgsTimestampList).longValue();
        logger.info("lastRead:{}", lastreadtime);
        redis.hmset(RedisConstants.REDIS_ALIAS,
            RedisServiceUtils.userLastReadTimestampKey(contestId), username,
            lastreadtime.toString());

        List<SendMessageDO> messages = usernameMsgTimestampMap.values().stream().map(str -> {
          try {
            return JacksonUtils.fromJson(str, SendMessageDO.class);
          } catch (Exception e) {
            logger.error("Exception due to {}", StringUtils.printStackTrace(e));
          }
          return null;
        }).collect(Collectors.toList());
        logger.info("all messages list:{}", messages);

        cf.complete(ResponseEntity.ok(new OkResponseDO<>(new GetMessageResponseDO(messages))));
      }
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
    }
  }

  @Async("apiTaskExecutor")
  public void notify(Long contestId, String username, CompletableFuture<ResponseEntity<?>> cf) {

    try {
      String prevReadtime = redis.hget(RedisConstants.REDIS_ALIAS,
          RedisServiceUtils.userLastReadTimestampKey(contestId), username);
      logger.info("user lastReadtime {}", prevReadtime);
      Long min = Long.valueOf(prevReadtime).longValue();
      Long max = 1669070339000L;

      long messageCnt = redis.zcount(RedisConstants.REDIS_ALIAS,
          RedisServiceUtils.contestGroupChatKey(contestId), min, max);
      logger.info("unread message count:{}", messageCnt);

      if (messageCnt == 0) {
        ErrorResponseDO noNewMessageREsponseDO = new ErrorResponseDO(
            ResponseErrorCodes.NO_NEW_MESSAGE, ResponseErrorMessages.NO_NEW_MESSAGE);
        cf.complete(ResponseEntity.ok(noNewMessageREsponseDO));
        return;
      }

      MessageNotificationDO messageNotificationDO = new MessageNotificationDO();
      messageNotificationDO.setMessageCnt(messageCnt);
      cf.complete(ResponseEntity.ok(new OkResponseDO<>(messageNotificationDO)));
    } catch (Exception e) {
      logger.error("Exception due to {}", StringUtils.printStackTrace(e));
    }
  }
}
