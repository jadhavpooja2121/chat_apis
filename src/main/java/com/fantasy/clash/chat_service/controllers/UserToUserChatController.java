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
@RequestMapping("v1/chat_service/")
public class UserToUserChatController extends BaseController {
  private static final Logger logger = LoggerFactory.getLogger(UserToUserChatController.class);

  @Autowired
  private UserToUserChatService userToUserChatService;

  @PostMapping(value = "/send", produces = MediaType.APPLICATION_JSON_VALUE,
      consumes = MediaType.APPLICATION_JSON_VALUE)
  public DeferredResult<ResponseEntity<?>> sendMessage(
      @RequestBody SendUserToUserMessageDO sendUserToUserMessageDO,
      @RequestParam(required = true) String username, HttpServletRequest request) {
    Long startTime = System.currentTimeMillis();
    String apiEndPoint = "/chat_service/send";
    DeferredResult<ResponseEntity<?>> df = new DeferredResult<ResponseEntity<?>>();
    try {
      LoginContext loginContext = getLoginContext(request);
      logger.debug("Received send messge request from {} ", loginContext.getUserId());
      CompletableFuture<ResponseEntity<?>> cf = new CompletableFuture<ResponseEntity<?>>();
      ErrorResponseDO usernameValidationDO = RequestValidator.validateUsername(username);
      if (usernameValidationDO != null) {
        cf.complete(ResponseEntity.ok(usernameValidationDO));
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
  public DeferredResult<ResponseEntity<?>> getMessage(
      @RequestParam(required = true) String username,
      @RequestParam(required = true) String username2, HttpServletRequest request) {
    Long startTime = System.currentTimeMillis();
    String apiEndPoint = "/chat_service/get";
    DeferredResult<ResponseEntity<?>> df = new DeferredResult<ResponseEntity<?>>();
    try {
      LoginContext loginContext = getLoginContext(request);
      logger.debug("Received send messge request from {} ", loginContext.getUserId());
      CompletableFuture<ResponseEntity<?>> cf = new CompletableFuture<ResponseEntity<?>>();
      ErrorResponseDO usernameValidationDO = RequestValidator.validateUsername(username);
      if (usernameValidationDO != null) {
        cf.complete(ResponseEntity.ok(usernameValidationDO));
        this.processDeferredResult(df, cf, apiEndPoint, startTime, loginContext.getReqId());
        return df;
      }
      ErrorResponseDO OtherUserNameValidationDO = RequestValidator.validateUsername(username2);
      if (OtherUserNameValidationDO != null) {
        cf.complete(ResponseEntity.ok(OtherUserNameValidationDO));
        this.processDeferredResult(df, cf, apiEndPoint, startTime, loginContext.getReqId());
        return df;
      }
      userToUserChatService.getMessage(username, username2, cf);   
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

}
