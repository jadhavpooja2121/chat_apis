package com.fantasy.clash.chat_service.dbos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MessageDBO {
  private Long id;
  private String messageText;
  private Long createdAt;
  private Long updatedAt;
}
