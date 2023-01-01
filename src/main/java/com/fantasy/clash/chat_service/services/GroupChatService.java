
package com.fantasy.clash.chat_service.services;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
import com.fantasy.clash.chat_service.constants.PropertyConstants;
import com.fantasy.clash.chat_service.constants.RedisConstants;
import com.fantasy.clash.chat_service.constants.ResponseErrorCodes;
import com.fantasy.clash.chat_service.constants.ResponseErrorMessages;
import com.fantasy.clash.chat_service.dos.GetGroupChatMessagesResponseDO;
import com.fantasy.clash.chat_service.dos.GroupChatMessageNotificationDO;
import com.fantasy.clash.chat_service.dos.SaveGroupChatMessageDO;
import com.fantasy.clash.chat_service.dos.SendGroupChatMessageResponseDO;
import com.fantasy.clash.chat_service.dos.GetGroupChatMessageResponseDO;
import com.fantasy.clash.chat_service.helper_services.GroupChatHelperService;
import com.fantasy.clash.chat_service.utils.RedisServiceUtils;
import com.fantasy.clash.chat_service.utils.TimeConversionUtils;
import com.fantasy.clash.framework.configuration.Configurator;
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

  @Autowired
  private GroupChatHelperService groupChatHelperService;

  @Autowired
  private Configurator configurator;

  @Async("apiTaskExecutor")
  public void sendMessage(Long groupChatId,
      SendGroupChatMessageResponseDO sendGroupChatMessageResponseDO,
      CompletableFuture<ResponseEntity<?>> cf) {
    try {
      Set<String> members = redis.zrange(RedisConstants.REDIS_ALIAS,
          RedisServiceUtils.contestGroupChatKey(groupChatId), 0D, Long.MAX_VALUE);
      if (CollectionUtils.isEmpty(members)) {
        redis.zadd(RedisConstants.REDIS_ALIAS, RedisServiceUtils.contestGroupChatKey(groupChatId),
            TimeConversionUtils.getGMTTime(), JacksonUtils.toJson(sendGroupChatMessageResponseDO));
        redis.expire(RedisConstants.REDIS_ALIAS, RedisServiceUtils.contestGroupChatKey(groupChatId),
            RedisConstants.REDIS_24HRS_KEY_TTL);
      } else {
        redis.zadd(RedisConstants.REDIS_ALIAS, RedisServiceUtils.contestGroupChatKey(groupChatId),
            TimeConversionUtils.getGMTTime(), JacksonUtils.toJson(sendGroupChatMessageResponseDO));
      }
      cf.complete(ResponseEntity.ok(new OkResponseDO<>(sendGroupChatMessageResponseDO)));
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
    }
  }

  @Async("apiTaskExecutor")
  public void getMessage(Long groupChatId, String username, Long timestamp, boolean isNext,
      CompletableFuture<ResponseEntity<?>> cf) {
    try {
      Long min = timestamp;
      Long max = Long.MAX_VALUE;
      Integer pageSize = configurator.getInt(PropertyConstants.PAGE_SIZE);
      if (timestamp == 0) {
        // TODO: change count to 100
        Set<TypedTuple<String>> membersWithScores =
            redis.zrangeByScoreWithScores(RedisConstants.REDIS_ALIAS,
                RedisServiceUtils.contestGroupChatKey(groupChatId), min, max, 0, max);
        if (CollectionUtils.isEmpty(membersWithScores)) {
          ErrorResponseDO noMessagesResponseDO = new ErrorResponseDO(
              ResponseErrorCodes.EMPTY_GROUP_CHAT, ResponseErrorMessages.EMPTY_GROUP_CHAT);
          cf.complete(ResponseEntity.ok(noMessagesResponseDO));
          return;
        }
        Map<Double, String> usernameMsgTimestampMap = new TreeMap<Double, String>();
        for (TypedTuple<String> val : membersWithScores) {
          usernameMsgTimestampMap.put(val.getScore(), val.getValue());
        }
        List<Double> userMsgsTimestampList = new ArrayList<>();
        for (Map.Entry<Double, String> valMap : usernameMsgTimestampMap.entrySet()) {
          Double latestTimestamp = valMap.getKey();
          userMsgsTimestampList.add(latestTimestamp);
        }
        Long latestReadTime = Collections.max(userMsgsTimestampList).longValue();

        List<GetGroupChatMessageResponseDO> messageList = new ArrayList<>();

        try {
          String lastReadTimestamp = redis.hget(RedisConstants.REDIS_ALIAS,
              RedisServiceUtils.userLastReadTimestampKey(groupChatId, username), username);
          Long userLastReadTimestamp = StringUtils.convertToLong(lastReadTimestamp);
          if (userLastReadTimestamp < latestReadTime) {
            redis.hmset(RedisConstants.REDIS_ALIAS,
                RedisServiceUtils.userLastReadTimestampKey(groupChatId, username), username,
                latestReadTime.toString());
          }
          for (Map.Entry<Double, String> valMap : usernameMsgTimestampMap.entrySet()) {
            SaveGroupChatMessageDO saveGroupChatMessageDO =
                JacksonUtils.fromJson(valMap.getValue(), SaveGroupChatMessageDO.class);
            Boolean isRead = valMap.getKey().longValue() <= userLastReadTimestamp ? true : false;

            messageList.add(new GetGroupChatMessageResponseDO(saveGroupChatMessageDO.getUsername(),
                saveGroupChatMessageDO.getMessage(), valMap.getKey().longValue(), isRead));
          }

        } catch (Exception e) {
          redis.hmset(RedisConstants.REDIS_ALIAS,
              RedisServiceUtils.userLastReadTimestampKey(groupChatId, username), username,
              latestReadTime.toString());
          redis.expire(RedisConstants.REDIS_ALIAS,
              RedisServiceUtils.userLastReadTimestampKey(groupChatId, username),
              RedisConstants.REDIS_24HRS_KEY_TTL);

          for (Map.Entry<Double, String> valMap : usernameMsgTimestampMap.entrySet()) {
            SaveGroupChatMessageDO saveGroupChatMessageDO =
                JacksonUtils.fromJson(valMap.getValue(), SaveGroupChatMessageDO.class);
            messageList.add(new GetGroupChatMessageResponseDO(saveGroupChatMessageDO.getUsername(),
                saveGroupChatMessageDO.getMessage(), valMap.getKey().longValue(), false));
          }
        }

        Integer toIndex = messageList.size();
        Integer fromIndex = toIndex - pageSize;
        List<GetGroupChatMessageResponseDO> recentMessages = new ArrayList<>();
        try {
          recentMessages = messageList.subList(fromIndex, toIndex);
        } catch (Exception e) {
          recentMessages = messageList;
        }

        cf.complete(ResponseEntity
            .ok(new OkResponseDO<>(new GetGroupChatMessagesResponseDO(recentMessages))));
        return;
      } else {
        String prevReadtime = redis.hget(RedisConstants.REDIS_ALIAS,
            RedisServiceUtils.userLastReadTimestampKey(groupChatId, username), username);
        Long lastRead = StringUtils.convertToLong(prevReadtime);
        if (isNext == true) {
          min = timestamp;
          groupChatHelperService.getNextMessages(groupChatId, username, min, max, lastRead,
              pageSize, cf);
        } else {
          min = timestamp;
          max = Long.MIN_VALUE;
          groupChatHelperService.getPreviousMessages(groupChatId, username, min, max, lastRead,
              pageSize, cf);
        }
      }
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
    }
  }

  @Async("apiTaskExecutor")
  public void notify(Long groupChatId, String username, CompletableFuture<ResponseEntity<?>> cf) {
    try {
      String prevReadtime = redis.hget(RedisConstants.REDIS_ALIAS,
          RedisServiceUtils.userLastReadTimestampKey(groupChatId, username), username);
      Long min = StringUtils.convertToLong(prevReadtime);
      Long max = Long.MAX_VALUE;

      Set<String> members = redis.zrange(RedisConstants.REDIS_ALIAS,
          RedisServiceUtils.contestGroupChatKey(groupChatId), min, Long.MAX_VALUE);

      List<SaveGroupChatMessageDO> allMessages =
          members.stream().map(t -> JacksonUtils.fromJson(t, SaveGroupChatMessageDO.class))
              .collect(Collectors.toList());

      Long messageCnt = 0L;
      for (SaveGroupChatMessageDO member : allMessages) {
        if (!member.getUsername().equals(username)) {
          messageCnt++;
        }
      }

      if (messageCnt == 0) {
        ErrorResponseDO noNewMessageREsponseDO = new ErrorResponseDO(
            ResponseErrorCodes.NO_NEW_MESSAGES, ResponseErrorMessages.NO_NEW_MESSAGES);
        cf.complete(ResponseEntity.ok(noNewMessageREsponseDO));
        return;
      }
      GroupChatMessageNotificationDO messageNotificationDO = new GroupChatMessageNotificationDO();
      messageNotificationDO.setMessageCnt(messageCnt);
      cf.complete(ResponseEntity.ok(new OkResponseDO<>(messageNotificationDO)));
    } catch (Exception e) {
      logger.error("Exception due to {}", StringUtils.printStackTrace(e));
    }
  }
}
