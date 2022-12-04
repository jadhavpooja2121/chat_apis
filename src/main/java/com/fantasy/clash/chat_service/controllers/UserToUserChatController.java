package com.fantasy.clash.chat_service.controllers;

import java.util.concurrent.CompletableFuture;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;
import com.fantasy.clash.chat_service.dos.SendUserToUserMessageDO;
import com.fantasy.clash.chat_service.services.UserToUserChatService;
import com.fantasy.clash.chat_service.validators.RequestValidator;
import com.fantasy.clash.framework.http.constants.ErrorConstants;
import com.fantasy.clash.framework.http.constants.ErrorMessages;
import com.fantasy.clash.framework.http.controller.BaseController;
import com.fantasy.clash.framework.http.dos.ErrorResponseDO;
import com.fantasy.clash.framework.http.error.responses.ErrorResponse;
import com.fantasy.clash.framework.http.exceptions.InvalidLoginContextHeaderException;
import com.fantasy.clash.framework.http.header.dos.LoginContext;
import com.fantasy.clash.framework.utils.StringUtils;

@RestController
@RequestMapping("v1/chat_service/user_to_user_chat")
public class UserToUserChatController extends BaseController {
  private static final Logger logger = LoggerFactory.getLogger(UserToUserChatController.class);

  @Autowired
  private UserToUserChatService userToUserChatService;

  @Autowired
  private RequestValidator requestValidator;

  @PostMapping(value = "/send", produces = MediaType.APPLICATION_JSON_VALUE,
      consumes = MediaType.APPLICATION_JSON_VALUE)
  public DeferredResult<ResponseEntity<?>> sendMessage(
      @RequestBody SendUserToUserMessageDO sendUserToUserMessageDO, HttpServletRequest request) {
    Long startTime = System.currentTimeMillis();
    String apiEndPoint = "/chat_service/user_to_user_chat/send";
    DeferredResult<ResponseEntity<?>> df = new DeferredResult<ResponseEntity<?>>();
    try {
      LoginContext loginContext = getLoginContext(request);
      String username = loginContext.getUsername();
      logger.info("Received send message request from user {} ", username);
      CompletableFuture<ResponseEntity<?>> cf = new CompletableFuture<ResponseEntity<?>>();
      ErrorResponseDO usernameValidationDO = RequestValidator.validateUsername(username);
      if (usernameValidationDO != null) {
        cf.complete(ResponseEntity.ok(usernameValidationDO));
        this.processDeferredResult(df, cf, apiEndPoint, startTime, loginContext.getReqId());
        return df;
      }
      ErrorResponseDO validateIfSenderAndRecipientSameDO = RequestValidator
          .validateIsSenderAndReceiverSame(username, sendUserToUserMessageDO.getRecipient());
      if (validateIfSenderAndRecipientSameDO != null) {
        cf.complete(ResponseEntity.ok(validateIfSenderAndRecipientSameDO));
        this.processDeferredResult(df, cf, apiEndPoint, startTime, loginContext.getReqId());
        return df;
      }
      ErrorResponseDO sendMessageReqValidationDO =
          RequestValidator.validateSendUserToUserMessageRequest(sendUserToUserMessageDO);
      if (sendMessageReqValidationDO != null) {
        cf.complete(ResponseEntity.ok(sendMessageReqValidationDO));
        this.processDeferredResult(df, cf, apiEndPoint, startTime, loginContext.getReqId());
        return df;
      }
      ErrorResponseDO messageLengthValidationDO =
          requestValidator.validateMessageLength(sendUserToUserMessageDO.getMessage());
      if (messageLengthValidationDO != null) {
        cf.complete(ResponseEntity.ok(messageLengthValidationDO));
        this.processDeferredResult(df, cf, apiEndPoint, startTime, loginContext.getReqId());
        return df;
      }
      userToUserChatService.sendMessage(username, sendUserToUserMessageDO, cf);
      this.processDeferredResult(df, cf, apiEndPoint, startTime, loginContext.getReqId());
    } catch (Exception e) {
      logger.error("Send message request failed due to {}", StringUtils.printStackTrace(e));
      ErrorResponse errorResponse = new ErrorResponse();
      if (e instanceof InvalidLoginContextHeaderException) {
        errorResponse.setCode(ErrorConstants.LOGIN_CONTEXT_HEADER_ERROR_CODE);
        errorResponse.setMessage(ErrorMessages.UNKNOWN_ERROR_MESSAGE);
      } else {
        errorResponse.setCode(ErrorConstants.UNKNOWN_ERROR_CODE);
        errorResponse.setMessage(ErrorMessages.UNKNOWN_ERROR_MESSAGE);
      }
      df.setResult(ResponseEntity.ok(errorResponse));
    }
    return df;
  }

  @GetMapping(value = "/get", produces = MediaType.APPLICATION_JSON_VALUE)
  public DeferredResult<ResponseEntity<?>> getMessage(@RequestParam(required = true) String sender,
      @RequestParam(required = true, defaultValue = "0") Long timestamp,
      @RequestParam(required = true, defaultValue = "true") boolean isNext,
      HttpServletRequest request) {
    Long startTime = System.currentTimeMillis();
    String apiEndPoint = "/chat_service/user_to_user_chat/get";
    DeferredResult<ResponseEntity<?>> df = new DeferredResult<ResponseEntity<?>>();
    try {
      LoginContext loginContext = getLoginContext(request);
      String username = loginContext.getUsername();
      logger.info("Received get messages from {} by {}", sender, username);
      CompletableFuture<ResponseEntity<?>> cf = new CompletableFuture<ResponseEntity<?>>();
      ErrorResponseDO usernameValidationDO = RequestValidator.validateUsername(username);
      if (usernameValidationDO != null) {
        cf.complete(ResponseEntity.ok(usernameValidationDO));
        this.processDeferredResult(df, cf, apiEndPoint, startTime, loginContext.getReqId());
        return df;
      }
      ErrorResponseDO recipientNameValidationDO = RequestValidator.validateUsername(sender);
      if (recipientNameValidationDO != null) {
        cf.complete(ResponseEntity.ok(recipientNameValidationDO));
        this.processDeferredResult(df, cf, apiEndPoint, startTime, loginContext.getReqId());
        return df;
      }
      userToUserChatService.getMessage(username, sender, timestamp, isNext, cf);
      this.processDeferredResult(df, cf, apiEndPoint, startTime, loginContext.getReqId());
    } catch (Exception e) {
      logger.error("Get message request failed due to {}", StringUtils.printStackTrace(e));
      ErrorResponse errorResponse = new ErrorResponse();
      if (e instanceof InvalidLoginContextHeaderException) {
        errorResponse.setCode(ErrorConstants.LOGIN_CONTEXT_HEADER_ERROR_CODE);
        errorResponse.setMessage(ErrorMessages.UNKNOWN_ERROR_MESSAGE);
      } else {
        errorResponse.setCode(ErrorConstants.UNKNOWN_ERROR_CODE);
        errorResponse.setMessage(ErrorMessages.UNKNOWN_ERROR_MESSAGE);
      }
      df.setResult(ResponseEntity.ok(errorResponse));
    }
    return df;
  }
  
  @GetMapping(value = "/chats", produces = MediaType.APPLICATION_JSON_VALUE)
  public DeferredResult<ResponseEntity<?>> getUserChats(
      @RequestParam(required = true, defaultValue = "true") boolean isNext,
      HttpServletRequest request) {
    Long startTime = System.currentTimeMillis();
    String apiEndPoint = "/chat_service/user_to_user_chat/chats";
    DeferredResult<ResponseEntity<?>> df = new DeferredResult<ResponseEntity<?>>();
    try {
      LoginContext loginContext = getLoginContext(request);
      String username = loginContext.getUsername();
      logger.info("Received get all chats request from {}", username);
      CompletableFuture<ResponseEntity<?>> cf = new CompletableFuture<ResponseEntity<?>>();
      ErrorResponseDO usernameValidationDO = RequestValidator.validateUsername(username);
      if (usernameValidationDO != null) {
        cf.complete(ResponseEntity.ok(usernameValidationDO));
        this.processDeferredResult(df, cf, apiEndPoint, startTime, loginContext.getReqId());
        return df;
      }
      userToUserChatService.getChats(username, isNext, cf);
      this.processDeferredResult(df, cf, apiEndPoint, startTime, loginContext.getReqId());
    } catch (Exception e) {
      logger.error("Get chats request failed due to {}", StringUtils.printStackTrace(e));
      ErrorResponse errorResponse = new ErrorResponse();
      if (e instanceof InvalidLoginContextHeaderException) {
        errorResponse.setCode(ErrorConstants.LOGIN_CONTEXT_HEADER_ERROR_CODE);
        errorResponse.setMessage(ErrorMessages.UNKNOWN_ERROR_MESSAGE);
      } else {
        errorResponse.setCode(ErrorConstants.UNKNOWN_ERROR_CODE);
        errorResponse.setMessage(ErrorMessages.UNKNOWN_ERROR_MESSAGE);
      }
      df.setResult(ResponseEntity.ok(errorResponse));
    }
    return df;
  }

}
