package com.fantasy.clash.chat_service.controllers;


import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;
import com.fantasy.clash.framework.http.controller.BaseController;

@RestController
@RequestMapping("/v1/chat_service")
public class GroupChatController extends BaseController{
  
  @PostMapping(value = "/send", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
  public DeferredResult<ResponseEntity<?>> sendMessage(@RequestBody)
  
}
