package com.fantasy.clash.chat_service.constants;

public class ResponseErrorMessages {
  private ResponseErrorMessages() {

  }

  public static final String SEND_MESSSAGE_REQUEST_INVALID = "Send message request is not valid";
  public static final String CONTEST_ID_INVALID = "Contest Id is not valid";
  public static final String EMPTY_GROUP_CHAT = "No messages in chat";
  public static final String NO_NEW_MESSAGES = "No new messages since your last read";
  public static final String NO_LAST_MESSAGES = "Already reached to the last message of this chat";
  public static final String INVALID_USERNAME = "Username is not valid";
  public static final String INVALID_MESSAGE_LENGTH = "Message Length is not valid";
  public static final String INVALID_TIMESTAMP = "Timestamp is not valid";
  public static final String NO_MESSAGES_IN_CHAT = "No messages in the chat";
  public static final String SENDER_AND_RECIPIENT_SAME = "Sender and recipient cannot be the same";
}
