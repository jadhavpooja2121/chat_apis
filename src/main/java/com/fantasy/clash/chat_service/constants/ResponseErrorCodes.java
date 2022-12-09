package com.fantasy.clash.chat_service.constants;

public class ResponseErrorCodes {
  private ResponseErrorCodes() {

  }

  public static final Integer SEND_MESSSAGE_REQUEST_INVALID = 1101;
  public static final Integer GROUP_CHAT_ID_INVALID = 1104;
  public static final Integer EMPTY_GROUP_CHAT = 1111;
  public static final Integer NO_NEW_MESSAGES = 1113;
  public static final Integer NO_LAST_MESSAGES = 1114;
  public static final Integer INVALID_USERNAME = 1115;
  public static final Integer INVALID_MESSAGE_LENGTH = 1117;
  public static final Integer INVALID_TIMESTAMP = 1119;
  public static final Integer NO_MESSAGES_IN_CHAT = 1121;
  public static final Integer SENDER_AND_RECIPIENT_SAME = 1123;
  public static final Integer NO_ACTIVE_USER_CHATS = 1124;
  public static final Integer USER_DOES_NOT_EXIST = 1130;
  public static final Integer UNABLE_TO_FETCH_DATA = 1131;
}
