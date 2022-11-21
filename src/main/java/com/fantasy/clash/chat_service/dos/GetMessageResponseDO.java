package com.fantasy.clash.chat_service.dos;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class GetMessageResponseDO {
  private List<SendMessageDO> messages;
}
