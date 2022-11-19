package com.fantasy.clash.chat_service.validators;

import org.springframework.stereotype.Service;
import com.fantasy.clash.chat_service.constants.ResponseErrorCodes;
import com.fantasy.clash.chat_service.constants.ResponseErrorMessages;
import com.fantasy.clash.chat_service.dos.SendMessageDO;
import com.fantasy.clash.framework.http.dos.ErrorResponseDO;

@Service
public class RequestValidator {

  public static ErrorResponseDO validateSendMessageRequest(SendMessageDO sendMessageDO) {
    if (sendMessageDO == null) {
      return new ErrorResponseDO(ResponseErrorCodes.SEND_MESSSAGE_REQUEST_INVALID,
          ResponseErrorMessages.SEND_MESSSAGE_REQUEST_INVALID);
    }
    return null;
  }

  public static ErrorResponseDO validateContestId(Long contestId) {
    if (contestId == null || contestId < 0) {
      return new ErrorResponseDO(ResponseErrorCodes.CONTEST_ID_INVALID,
          ResponseErrorMessages.CONTEST_ID_INVALID);
    }
    return null;
  }
}
