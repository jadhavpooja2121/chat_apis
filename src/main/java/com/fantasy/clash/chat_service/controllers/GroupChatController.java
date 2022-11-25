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
import com.fantasy.clash.chat_service.dos.SendMessageDO;
import com.fantasy.clash.chat_service.services.GroupChatService;
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
@RequestMapping("/v1/chat_service/{contestId}")
public class GroupChatController extends BaseController {
  private static final Logger logger = LoggerFactory.getLogger(GroupChatController.class);

  @Autowired
  private GroupChatService groupChatService;

  @PostMapping(value = "/send", produces = MediaType.APPLICATION_JSON_VALUE,
      consumes = MediaType.APPLICATION_JSON_VALUE)
  public DeferredResult<ResponseEntity<?>> sendMessage(@PathVariable Long contestId,
      @RequestBody SendMessageDO sendMessageDO, HttpServletRequest request) {
    Long startTime = System.currentTimeMillis();
    String apiEndPoint = "/chat_service/{contestId}/send";
    DeferredResult<ResponseEntity<?>> df = new DeferredResult<ResponseEntity<?>>();
    try {
      LoginContext loginContext = getLoginContext(request);
      logger.debug("Received send messge request from {} ", loginContext.getUserId());
      CompletableFuture<ResponseEntity<?>> cf = new CompletableFuture<ResponseEntity<?>>();
      ErrorResponseDO contestIdValidationDO = RequestValidator.validateContestId(contestId);
      if (contestIdValidationDO != null) {
        cf.complete(ResponseEntity.ok(contestIdValidationDO));
        this.processDeferredResult(df, cf, apiEndPoint, startTime, loginContext.getReqId());
        return df;
      }
      ErrorResponseDO sendMessageReqValidationDO =
          RequestValidator.validateSendMessageRequest(sendMessageDO);
      if (sendMessageReqValidationDO != null) {
        cf.complete(ResponseEntity.ok(sendMessageReqValidationDO));
        this.processDeferredResult(df, cf, apiEndPoint, startTime, loginContext.getReqId());
        return df;
      }
      groupChatService.sendMessage(contestId, sendMessageDO, cf);
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
  public DeferredResult<ResponseEntity<?>> getMessage(@PathVariable Long contestId,
      @RequestParam(required = true) String username,
      @RequestParam(required = true, defaultValue = "0") Long timestamp,
      @RequestParam(required = true, defaultValue = "true") boolean isNext,
      HttpServletRequest request) {
    Long startTime = System.currentTimeMillis();
    String apiEndPoint = "/chat_service/{contestId}/get";
    DeferredResult<ResponseEntity<?>> df = new DeferredResult<ResponseEntity<?>>();
    try {
      LoginContext loginContext = getLoginContext(request);
      logger.debug("Received read messge request from {} ", loginContext.getUserId());
      CompletableFuture<ResponseEntity<?>> cf = new CompletableFuture<ResponseEntity<?>>();
      ErrorResponseDO contestIdValidationDO = RequestValidator.validateContestId(contestId);
      if (contestIdValidationDO != null) {
        cf.complete(ResponseEntity.ok(contestIdValidationDO));
        this.processDeferredResult(df, cf, apiEndPoint, startTime, loginContext.getReqId());
        return df;
      }
      ErrorResponseDO usernameValidationDO = RequestValidator.validateUsername(username);
      if (usernameValidationDO != null) {
        cf.complete(ResponseEntity.ok(usernameValidationDO));
        this.processDeferredResult(df, cf, apiEndPoint, startTime, loginContext.getReqId());
        return df;
      }
      ErrorResponseDO timestampValidationDO = RequestValidator.validateTimestamp(timestamp);
      if (timestampValidationDO != null) {
        cf.complete(ResponseEntity.ok(timestampValidationDO));
        this.processDeferredResult(df, cf, apiEndPoint, startTime, loginContext.getReqId());
        return df;
      }
      groupChatService.getMessage(contestId, username, timestamp, isNext, cf);
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

  @GetMapping(value = "/message-notify", produces = MediaType.APPLICATION_JSON_VALUE)
  public DeferredResult<ResponseEntity<?>> notify(@PathVariable Long contestId,
      @RequestParam String username, HttpServletRequest request) {
    Long startTime = System.currentTimeMillis();
    String apiEndPoint = "/chat_service/{contestId}/message-notify";
    DeferredResult<ResponseEntity<?>> df = new DeferredResult<ResponseEntity<?>>();
    try {
      LoginContext loginContext = getLoginContext(request);
      logger.debug("Received read messge request from {} ", loginContext.getUserId());
      CompletableFuture<ResponseEntity<?>> cf = new CompletableFuture<ResponseEntity<?>>();
      ErrorResponseDO contestIdValidationDO = RequestValidator.validateContestId(contestId);
      if (contestIdValidationDO != null) {
        cf.complete(ResponseEntity.ok(contestIdValidationDO));
        this.processDeferredResult(df, cf, apiEndPoint, startTime, loginContext.getReqId());
        return df;
      }
      ErrorResponseDO usernameValidationDO = RequestValidator.validateUsername(username);
      if (usernameValidationDO != null) {
        cf.complete(ResponseEntity.ok(usernameValidationDO));
        this.processDeferredResult(df, cf, apiEndPoint, startTime, loginContext.getReqId());
        return df;
      }
      groupChatService.notify(contestId, username, cf);
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
}
