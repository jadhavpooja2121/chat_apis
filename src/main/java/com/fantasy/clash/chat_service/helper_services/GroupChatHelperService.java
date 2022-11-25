package com.fantasy.clash.chat_service.helper_services;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import com.fantasy.clash.chat_service.constants.RedisConstants;
import com.fantasy.clash.chat_service.constants.ResponseErrorCodes;
import com.fantasy.clash.chat_service.constants.ResponseErrorMessages;
import com.fantasy.clash.chat_service.dos.GetMessageResponseDO;
import com.fantasy.clash.chat_service.dos.SendMessageDO;
import com.fantasy.clash.chat_service.dos.SendMessageResponseDO;
import com.fantasy.clash.chat_service.utils.RedisServiceUtils;
import com.fantasy.clash.framework.http.dos.ErrorResponseDO;
import com.fantasy.clash.framework.http.dos.OkResponseDO;
import com.fantasy.clash.framework.redis.cluster.ClusteredRedis;
import com.fantasy.clash.framework.utils.JacksonUtils;
import com.fantasy.clash.framework.utils.StringUtils;

@Service
public class GroupChatHelperService {
  private static final Logger logger = LoggerFactory.getLogger(GroupChatHelperService.class);

  @Autowired
  private ClusteredRedis redis;

  public void getNextMessages(Long contestId, String username, Long min, Long max,
      CompletableFuture<ResponseEntity<?>> cf) {
    try {
      Set<TypedTuple<String>> nextMessagesWithScores =
          redis.zrangeByScoreWithScores(RedisConstants.REDIS_ALIAS,
              RedisServiceUtils.contestGroupChatKey(contestId), min, max, 1, 3);
      logger.info("membersWithScores {}", nextMessagesWithScores);

      if (CollectionUtils.isEmpty(nextMessagesWithScores)) {
        ErrorResponseDO noMessagesResponseDO = new ErrorResponseDO(
            ResponseErrorCodes.NO_NEW_MESSAGES, ResponseErrorMessages.NO_NEW_MESSAGES);
        cf.complete(ResponseEntity.ok(noMessagesResponseDO));
        return;
      }
      Map<Double, String> usernameMsgTimestampMap = new TreeMap<Double, String>();
      for (TypedTuple<String> val : nextMessagesWithScores) {
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
          RedisServiceUtils.userLastReadTimestampKey(contestId, username), username,
          lastreadtime.toString());

      List<SendMessageResponseDO> messageList = new ArrayList<>();
      for (Map.Entry<Double, String> valMap : usernameMsgTimestampMap.entrySet()) {
        SendMessageDO sendMessageDO = JacksonUtils.fromJson(valMap.getValue(), SendMessageDO.class);
        messageList.add(new SendMessageResponseDO(sendMessageDO.getUsername(),
            sendMessageDO.getMessage(), valMap.getKey().longValue(), false));
      }
      logger.info("messageList {}", JacksonUtils.toJson(messageList));
      cf.complete(ResponseEntity.ok(new OkResponseDO<>(new GetMessageResponseDO(messageList))));
      return;
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
    }
  }

  public void getPreviousMessages(Long contestId, String username, Long min, Long max,
      CompletableFuture<ResponseEntity<?>> cf) {
    try {
      Set<TypedTuple<String>> prevMessagesWithScores =
          redis.zrevrangeByScoreWithScores(RedisConstants.REDIS_ALIAS,
              RedisServiceUtils.contestGroupChatKey(contestId), max, min, 1, 3);
      logger.info("membersWithScores {}", prevMessagesWithScores);

      if (CollectionUtils.isEmpty(prevMessagesWithScores)) {
        ErrorResponseDO noMessagesResponseDO = new ErrorResponseDO(
            ResponseErrorCodes.NO_LAST_MESSAGES, ResponseErrorMessages.NO_LAST_MESSAGES);
        cf.complete(ResponseEntity.ok(noMessagesResponseDO));
        return;
      }

      Map<Double, String> usernameMsgTimestampMap = new TreeMap<Double, String>();
      for (TypedTuple<String> val : prevMessagesWithScores) {
        usernameMsgTimestampMap.put(val.getScore(), val.getValue());
      }
      logger.info("map <time, val> :{}", usernameMsgTimestampMap);

      List<SendMessageResponseDO> messageList = new ArrayList<>();
      for (Map.Entry<Double, String> valMap : usernameMsgTimestampMap.entrySet()) {
        SendMessageDO sendMessageDO = JacksonUtils.fromJson(valMap.getValue(), SendMessageDO.class);
        messageList.add(new SendMessageResponseDO(sendMessageDO.getUsername(),
            sendMessageDO.getMessage(), valMap.getKey().longValue(), true));
      }
      logger.info("previous messages List {}", JacksonUtils.toJson(messageList));
      cf.complete(ResponseEntity.ok(new OkResponseDO<>(new GetMessageResponseDO(messageList))));
      return;
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
    }
  }

}
