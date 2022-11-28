package com.fantasy.clash.chat_service.services;

import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import com.fantasy.clash.chat_service.dos.SendUserToUserMessageDO;
import com.fantasy.clash.chat_service.dos.SendUserToUserMessageResponseDO;
import com.fantasy.clash.chat_service.helper_services.UserToUserChatHelperService;
import com.fantasy.clash.chat_service.utils.HashUtils;
import com.fantasy.clash.chat_service.utils.TimeConversionUtils;
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
      String hash = HashUtils.getHash(username, sendUserToUserMessageDO.getOtherUsername());
      SendUserToUserMessageResponseDO sendUserToUserMessageResponseDO = userToUserChatHelperService
          .saveMessage(hash, username, sendUserToUserMessageDO.getOtherUsername(),
              sendUserToUserMessageDO.getMessage(), TimeConversionUtils.getGMTTime());
      if (sendUserToUserMessageResponseDO != null) {
        cf.complete(ResponseEntity.ok(new OkResponseDO<>(sendUserToUserMessageResponseDO)));
        return;
      }
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
    }
  }
}
