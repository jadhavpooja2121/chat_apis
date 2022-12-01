package com.fantasy.clash.chat_service.constants;

public class DatabaseConstants {
  private DatabaseConstants() {

  }

  public static final String DEFAULT_KEYSPACE = "chat_service";
  public static final String USER_CHATS_TABLE = "chats";
  public static final String CHAT_IDENTIFIER_COLUMN = "group_chat_id";
  public static final String MESSAGE_SENDER_COLUMN = "sender";
  public static final String MESSAGE_SENT_AT_TIMESTAMP_COLUMN = "sent_at";
  public static final String MESSAGE_TEXT_COLUMN = "message";
  public static final String ACTIVE_CHATS_TABLE = "active_chats";
  public static final String USERNAME1_COLUMN = "username";
  public static final String USERNAME2_COLUMN = "username2";
  public static final String LAST_ACTIVE_TIMESTAMP_COLUMN = "last_active_at";
}
