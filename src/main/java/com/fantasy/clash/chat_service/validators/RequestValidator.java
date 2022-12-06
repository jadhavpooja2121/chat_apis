package com.fantasy.clash.chat_service.validators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.fantasy.clash.chat_service.constants.PropertyConstants;
import com.fantasy.clash.chat_service.constants.ResponseErrorCodes;
import com.fantasy.clash.chat_service.constants.ResponseErrorMessages;
import com.fantasy.clash.chat_service.dos.SendGroupChatMessageDO;
import com.fantasy.clash.chat_service.dos.SendUserToUserMessageDO;
import com.fantasy.clash.framework.configuration.Configurator;
import com.fantasy.clash.framework.http.dos.ErrorResponseDO;
import com.fantasy.clash.framework.utils.CollectionUtils;
import com.fantasy.clash.framework.utils.StringUtils;

@Service
public class RequestValidator {
  private static final Logger logger = LoggerFactory.getLogger(RequestValidator.class);

  @Autowired
  private Configurator configurator;

  public static ErrorResponseDO validateSendMessageRequest(SendGroupChatMessageDO sendMessageDO) {
    if (sendMessageDO == null) {
      return new ErrorResponseDO(ResponseErrorCodes.SEND_MESSSAGE_REQUEST_INVALID,
          ResponseErrorMessages.SEND_MESSSAGE_REQUEST_INVALID);
    }
    return null;
  }

  public static ErrorResponseDO validateGroupChatId(Long groupChatId) {
    if (groupChatId == null || groupChatId < 0) {
      return new ErrorResponseDO(ResponseErrorCodes.GROUP_CHAT_ID_INVALID,
          ResponseErrorMessages.GROUP_CHAT_ID_INVALID);
    }
    return null;
  }

  public static ErrorResponseDO validateUsername(String username) {
    if (StringUtils.isNullOrEmpty(username)) {
      return new ErrorResponseDO(ResponseErrorCodes.INVALID_USERNAME,
          ResponseErrorMessages.INVALID_USERNAME);
    }
    return null;
  }

  public static ErrorResponseDO validateTimestamp(Long timestamp) {
    if (timestamp == null) {
      return new ErrorResponseDO(ResponseErrorCodes.INVALID_TIMESTAMP,
          ResponseErrorMessages.INVALID_TIMESTAMP);
    }
    return null;
  }

  public static ErrorResponseDO validateSendUserToUserMessageRequest(
      SendUserToUserMessageDO sendUserToUserMessageDO) {
    if (sendUserToUserMessageDO == null) {
      return new ErrorResponseDO(ResponseErrorCodes.SEND_MESSSAGE_REQUEST_INVALID,
          ResponseErrorMessages.SEND_MESSSAGE_REQUEST_INVALID);
    }
    return null;
  }

  public ErrorResponseDO validateMessageLength(String message) {
    try {
      if (message.length() < configurator.getInt(PropertyConstants.MESSAGE_MIN_LENGTH)
          || message.length() > configurator.getInt(PropertyConstants.MESSAGE_MAX_LENGTH)) {
        return new ErrorResponseDO(ResponseErrorCodes.INVALID_MESSAGE_LENGTH,
            ResponseErrorMessages.INVALID_MESSAGE_LENGTH);
      }
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
    }
    return null;
  }

  public static ErrorResponseDO validateIsSenderAndReceiverSame(String username, String recipient) {
    if (username.equals(recipient)) {
      return new ErrorResponseDO(ResponseErrorCodes.SENDER_AND_RECIPIENT_SAME,
          ResponseErrorMessages.SENDER_AND_RECIPIENT_SAME);
    }
    return null;
  }
}
