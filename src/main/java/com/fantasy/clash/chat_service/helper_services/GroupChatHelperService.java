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
import com.fantasy.clash.chat_service.dos.GetGroupChatMessagesResponseDO;
import com.fantasy.clash.chat_service.dos.SaveGroupChatMessageDO;
import com.fantasy.clash.chat_service.dos.SendGroupChatMessageDO;
import com.fantasy.clash.chat_service.dos.GetGroupChatMessageResponseDO;
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

  public void getNextMessages(Long groupChatId, String username, Long min, Long max, Long lastRead,
      Integer pageSize, CompletableFuture<ResponseEntity<?>> cf) {
    try {
      Set<TypedTuple<String>> nextMessagesWithScores =
          redis.zrangeByScoreWithScores(RedisConstants.REDIS_ALIAS,
              RedisServiceUtils.contestGroupChatKey(groupChatId), min, max, 1, pageSize);
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
      List<Double> userMsgsTimestampList = new ArrayList<>();
      for (Map.Entry<Double, String> valMap : usernameMsgTimestampMap.entrySet()) {
        Double latestTimestamp = valMap.getKey();
        userMsgsTimestampList.add(latestTimestamp);
      }
      Long latestReadTime = Collections.max(userMsgsTimestampList).longValue();
      String userLastReadTime = redis.hget(RedisConstants.REDIS_ALIAS,
          RedisServiceUtils.userLastReadTimestampKey(groupChatId, username), username);
      if (StringUtils.isEmpty(userLastReadTime)) {
        redis.hmset(RedisConstants.REDIS_ALIAS,
            RedisServiceUtils.userLastReadTimestampKey(groupChatId, username), username,
            latestReadTime.toString());
      } else {
        Long lastReadTimestamp = StringUtils.convertToLong(userLastReadTime);
        if (lastReadTimestamp < latestReadTime) {
          redis.hmset(RedisConstants.REDIS_ALIAS,
              RedisServiceUtils.userLastReadTimestampKey(groupChatId, username), username,
              latestReadTime.toString());
        }
      }
      List<GetGroupChatMessageResponseDO> messageList = new ArrayList<>();
      for (Map.Entry<Double, String> valMap : usernameMsgTimestampMap.entrySet()) {
        SaveGroupChatMessageDO saveGroupChatMessageDO =
            JacksonUtils.fromJson(valMap.getValue(), SaveGroupChatMessageDO.class);
        Boolean isRead = valMap.getKey().longValue() <= lastRead ? true : false;
        messageList.add(new GetGroupChatMessageResponseDO(saveGroupChatMessageDO.getUsername(),
            saveGroupChatMessageDO.getMessage(), valMap.getKey().longValue(), isRead));
      }
      cf.complete(
          ResponseEntity.ok(new OkResponseDO<>(new GetGroupChatMessagesResponseDO(messageList))));
      return;
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
    }
  }

  public void getPreviousMessages(Long groupChatId, String username, Long min, Long max,
      Long lastRead, Integer pageSize, CompletableFuture<ResponseEntity<?>> cf) {
    try {
      Set<TypedTuple<String>> prevMessagesWithScores =
          redis.zrevrangeByScoreWithScores(RedisConstants.REDIS_ALIAS,
              RedisServiceUtils.contestGroupChatKey(groupChatId), max, min, 1, pageSize);
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
      List<GetGroupChatMessageResponseDO> messageList = new ArrayList<>();
      for (Map.Entry<Double, String> valMap : usernameMsgTimestampMap.entrySet()) {
        SaveGroupChatMessageDO saveGroupChatMessageDO =
            JacksonUtils.fromJson(valMap.getValue(), SaveGroupChatMessageDO.class);
        Boolean isRead = valMap.getKey().longValue() <= lastRead ? true : false;
        messageList.add(new GetGroupChatMessageResponseDO(saveGroupChatMessageDO.getUsername(),
            saveGroupChatMessageDO.getMessage(), valMap.getKey().longValue(), isRead));
      }
      cf.complete(
          ResponseEntity.ok(new OkResponseDO<>(new GetGroupChatMessagesResponseDO(messageList))));
      return;
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
    }
  }

}
