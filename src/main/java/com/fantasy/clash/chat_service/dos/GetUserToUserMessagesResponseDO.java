package com.fantasy.clash.chat_service.dos;

import java.sql.Timestamp;
import java.util.NavigableMap;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class GetUserToUserMessagesResponseDO {
  private NavigableMap<Long, String> messagesList;
}
