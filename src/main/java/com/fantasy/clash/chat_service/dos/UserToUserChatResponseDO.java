package com.fantasy.clash.chat_service.dos;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class UserToUserChatResponseDO {
  private String sender;
  private Long lastSeenAt;
  private String notificationText;
}
