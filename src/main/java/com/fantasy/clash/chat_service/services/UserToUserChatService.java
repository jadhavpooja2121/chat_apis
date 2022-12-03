package com.fantasy.clash.chat_service.services;

import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import com.fantasy.clash.chat_service.constants.ResponseErrorCodes;
import com.fantasy.clash.chat_service.constants.ResponseErrorMessages;
import com.fantasy.clash.chat_service.dos.GetUserToUserMessagesResponseDO;
import com.fantasy.clash.chat_service.dos.SendUserToUserMessageDO;
import com.fantasy.clash.chat_service.dos.SendUserToUserMessageResponseDO;
import com.fantasy.clash.chat_service.helper_services.UserToUserChatHelperService;
import com.fantasy.clash.chat_service.utils.HashUtils;
import com.fantasy.clash.chat_service.utils.TimeConversionUtils;
import com.fantasy.clash.framework.http.dos.ErrorResponseDO;
import com.fantasy.clash.framework.http.dos.OkResponseDO;
import com.fantasy.clash.framework.utils.StringUtils;

@Service
public class UserToUserChatService {
  private static final Logger logger = LoggerFactory.getLogger(UserToUserChatService.class);

  @Autowired
  private UserToUserChatHelperService userToUserChatHelperService;

  @Async("apiTaskExecutor")
  public void sendMessage(String username, SendUserToUserMessageDO sendUserToUserMessageDO,
      CompletableFuture<ResponseEntity<?>> cf) {
    try {
      String groupChatId = HashUtils.getHash(username, sendUserToUserMessageDO.getRecipient());
      SendUserToUserMessageResponseDO sendUserToUserMessageResponseDO = userToUserChatHelperService
          .saveMessage(groupChatId, username, sendUserToUserMessageDO.getRecipient(),
              sendUserToUserMessageDO.getMessage(), TimeConversionUtils.getGMTTime());
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
      logger.info("get hash {}", groupChatId);
      GetUserToUserMessagesResponseDO messagesList = userToUserChatHelperService
          .getUserMessages(groupChatId, username, sender, timestamp, isNext);
      if (messagesList == null) {
        cf.complete(ResponseEntity.ok(new ErrorResponseDO(ResponseErrorCodes.NO_MESSAGES_IN_CHAT,
            ResponseErrorMessages.NO_MESSAGES_IN_CHAT)));
        return;
      } else {
        cf.complete(ResponseEntity.ok(new OkResponseDO<>(messagesList)));
      }
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
    }
  }
}
