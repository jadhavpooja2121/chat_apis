package com.fantasy.clash.chat_service.dos;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class GetUserToUserMessageResponseDO {
  private String sender;
  private String message;
  private Long sentAt;
}
