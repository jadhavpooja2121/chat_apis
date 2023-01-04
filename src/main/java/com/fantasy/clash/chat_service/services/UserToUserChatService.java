package com.fantasy.clash.chat_service.services;

import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.fantasy.clash.chat_service.constants.DatabaseConstants;
import com.fantasy.clash.chat_service.constants.RedisConstants;
import com.fantasy.clash.chat_service.constants.ResponseErrorCodes;
import com.fantasy.clash.chat_service.constants.ResponseErrorMessages;
import com.fantasy.clash.chat_service.helper_services.UserToUserChatHelperService;
import com.fantasy.clash.chat_service.rest_clients.UserServiceAccountsRestClient;
import com.fantasy.clash.chat_service.utils.HashUtils;
import com.fantasy.clash.chat_service.utils.RedisServiceUtils;
import com.fantasy.clash.framework.http.dos.BaseResponseDO;
import com.fantasy.clash.framework.http.dos.ErrorResponseDO;
import com.fantasy.clash.framework.http.dos.OkResponseDO;
import com.fantasy.clash.framework.http.error.responses.ErrorResponse;
import com.fantasy.clash.framework.object_collection.chat_service.dos.GetUserChatsResponseDO;
import com.fantasy.clash.framework.object_collection.chat_service.dos.GetUserToUserMessagesResponseDO;
import com.fantasy.clash.framework.object_collection.chat_service.dos.SendUserToUserMessageDO;
import com.fantasy.clash.framework.object_collection.chat_service.dos.SendUserToUserMessageResponseDO;
import com.fantasy.clash.framework.object_collection.user_service.dos.UserDO;
import com.fantasy.clash.framework.object_collection.user_service.dos.UsersDO;
import com.fantasy.clash.framework.redis.cluster.ClusteredRedis;
import com.fantasy.clash.framework.utils.JacksonUtils;
import com.fantasy.clash.framework.utils.StringUtils;
import com.fantasy.clash.framework.utils.TimeUtils;
import com.fasterxml.jackson.core.type.TypeReference;

@Service
public class UserToUserChatService {
  private static final Logger logger = LoggerFactory.getLogger(UserToUserChatService.class);

  @Autowired
  private UserToUserChatHelperService userToUserChatHelperService;

  @Autowired
  private UserServiceAccountsRestClient userServiceAccountsRestClient;

  @Autowired
  private ClusteredRedis redis;

  @Async("apiTaskExecutor")
  public void sendMessage(String username, SendUserToUserMessageDO sendUserToUserMessageDO,
      CompletableFuture<ResponseEntity<?>> cf) {
    try {
      String userAccount = redis.get(RedisConstants.REDIS_ALIAS,
          RedisServiceUtils.userAccountKey(sendUserToUserMessageDO.getRecipient()));

      if (StringUtils.isNullOrEmpty(userAccount)) {
        UsersDO user = new UsersDO();
        user.setUsernames(List.of(sendUserToUserMessageDO.getRecipient()));

        BaseResponseDO baseResponse = userServiceAccountsRestClient.getUsersAccounts(user);
        if (baseResponse == null) {
          cf.complete(ResponseEntity.ok(new ErrorResponseDO(ResponseErrorCodes.UNABLE_TO_FETCH_DATA,
              ResponseErrorMessages.UNABLE_TO_FETCH_DATA)));
          return;
        } else if (baseResponse.code != HttpStatus.OK.value()) {
          ErrorResponseDO errorResponse = (ErrorResponseDO) baseResponse;
          cf.complete(ResponseEntity.ok(errorResponse));
          return;
        }

        OkResponseDO<UsersDO> usersAccounts =
            (OkResponseDO<UsersDO>) userServiceAccountsRestClient.getUsersAccounts(user);
        UsersDO users =
            JacksonUtils.fromJson(JacksonUtils.toJson(usersAccounts.result), UsersDO.class);
        List<UserDO> usersList = users.getUsers();

        Predicate<UserDO> receiver =
            userDO -> (userDO.getUsername().equals(sendUserToUserMessageDO.getRecipient()));
        Boolean isUserExists = usersList.stream().anyMatch(receiver);

        UserDO userInfo = usersList.stream().filter(receiver).findAny().get();

        if (!isUserExists) {
          cf.complete(ResponseEntity.ok(new ErrorResponseDO(ResponseErrorCodes.USER_DOES_NOT_EXIST,
              ResponseErrorMessages.USER_DOES_NOT_EXIST)));
          return;
        }

        redis.setex(RedisConstants.REDIS_ALIAS,
            RedisServiceUtils.userAccountKey(sendUserToUserMessageDO.getRecipient()),
            JacksonUtils.toJson(userInfo), RedisConstants.REDIS_30_DAYS_KEY_TTL);
      }

      String groupChatId = HashUtils.getHash(username, sendUserToUserMessageDO.getRecipient());
      SendUserToUserMessageResponseDO sendUserToUserMessageResponseDO = userToUserChatHelperService
          .saveMessage(groupChatId, username, sendUserToUserMessageDO.getRecipient(),
              sendUserToUserMessageDO.getMessage(), TimeUtils.getGMTTime());
      if (sendUserToUserMessageResponseDO != null) {
        cf.complete(ResponseEntity.ok(new OkResponseDO<>(sendUserToUserMessageResponseDO)));
        return;
      }
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
    }
  }

  public void getMessage(String username, String sender, Long timestamp, boolean isNext,
      CompletableFuture<ResponseEntity<?>> cf) {
    logger.info("request from {} to get messages by {}", username, sender);
    try {
      String groupChatId = HashUtils.getHash(username, sender);
      GetUserToUserMessagesResponseDO messagesList = userToUserChatHelperService
          .getUserMessages(groupChatId, username, sender, timestamp, isNext);
      if (messagesList == null) {
        cf.complete(ResponseEntity.ok(new ErrorResponseDO(ResponseErrorCodes.NO_MESSAGES_IN_CHAT,
            ResponseErrorMessages.NO_MESSAGES_IN_CHAT)));
        return;
      }
      cf.complete(ResponseEntity.ok(new OkResponseDO<>(messagesList)));
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
    }
  }

  public void getChats(String username, CompletableFuture<ResponseEntity<?>> cf) {
    try {

      GetUserChatsResponseDO userActiveChatsDO =
          userToUserChatHelperService.getActiveChats(username);
      if (userActiveChatsDO == null) {
        cf.complete(ResponseEntity.ok(new ErrorResponseDO(ResponseErrorCodes.NO_ACTIVE_USER_CHATS,
            ResponseErrorMessages.NO_ACTIVE_USER_CHATS)));
        return;
      }
      cf.complete(ResponseEntity.ok(new OkResponseDO<>(userActiveChatsDO)));
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
    }

  }
}
