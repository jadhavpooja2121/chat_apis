package com.fantasy.clash.chat_service.helper_services;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.cassandra.mapping.Column;
import org.springframework.stereotype.Service;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.fantasy.clash.chat_service.constants.CassandraConstants;
import com.fantasy.clash.chat_service.constants.DatabaseConstants;
import com.fantasy.clash.chat_service.dos.GetUserToUserMessageResponseDO;
import com.fantasy.clash.chat_service.dos.GetUserToUserMessagesResponseDO;
import com.fantasy.clash.chat_service.dos.SendUserToUserMessageResponseDO;
import com.fantasy.clash.framework.utils.JacksonUtils;
import com.fantasy.clash.framework.utils.StringUtils;

@Service
public class UserToUserChatHelperService {
  private static final Logger logger = LoggerFactory.getLogger(UserToUserChatHelperService.class);

  @Autowired
  @Qualifier("chatSession")
  private Session chatSession;

  public SendUserToUserMessageResponseDO saveMessage(String groupChatId, String username,
      String username2, String message, Long timestamp) {
    try {

      Statement statement = QueryBuilder
          .insertInto(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.USER_CHATS_TABLE)
          .values(
              List.of(DatabaseConstants.CHAT_IDENTIFIER_COLUMN,
                  DatabaseConstants.MESSAGE_SENDER_COLUMN,
                  DatabaseConstants.MESSAGE_SENT_AT_TIMESTAMP_COLUMN,
                  DatabaseConstants.MESSAGE_TEXT_COLUMN),
              List.of(groupChatId, username, timestamp, message))
          .using(QueryBuilder.ttl(CassandraConstants.DEFAULT_CHAT_DATA_EXPIRY_TTL))
          .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
      chatSession.execute(statement);

      Statement st = QueryBuilder
          .insertInto(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.ACTIVE_CHATS_TABLE)
          .values(
              List.of(DatabaseConstants.USERNAME1_COLUMN, DatabaseConstants.USERNAME2_COLUMN,
                  DatabaseConstants.LAST_ACTIVE_TIMESTAMP_COLUMN),
              List.of(username, username2, timestamp))
          .using(QueryBuilder.ttl(CassandraConstants.DEFAULT_CHAT_DATA_EXPIRY_TTL))
          .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
      chatSession.execute(st);

      SendUserToUserMessageResponseDO sendUserToUserMessageResponseDO =
          new SendUserToUserMessageResponseDO(username, username2, message, timestamp);
      return sendUserToUserMessageResponseDO;
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
      return null;
    }
  }

  public GetUserToUserMessagesResponseDO getUserMessages(String groupChatId, String username,
      String username2) {
    try {
      Statement st = QueryBuilder
          .select(DatabaseConstants.MESSAGE_SENDER_COLUMN, DatabaseConstants.MESSAGE_TEXT_COLUMN,
              DatabaseConstants.MESSAGE_SENT_AT_TIMESTAMP_COLUMN)
          .from(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.USER_CHATS_TABLE)
          .where(QueryBuilder.eq(DatabaseConstants.CHAT_IDENTIFIER_COLUMN, groupChatId))
          .and(QueryBuilder.in("sender", List.of(username, username2)));

      ResultSet result = chatSession.execute(st);

      List<GetUserToUserMessageResponseDO> allMessages =
          new ArrayList<GetUserToUserMessageResponseDO>();
      for (Row msg : result.all()) {
        logger.info("msg:{}", msg.getString(0));
        GetUserToUserMessageResponseDO message =
            new GetUserToUserMessageResponseDO(msg.getString(0), msg.getString(1), msg.getLong(2));
        allMessages.add(message);
      }

      List<GetUserToUserMessageResponseDO> sortedMessages =
          allMessages.stream().sorted((t1, t2) -> t1.getSentAt().compareTo(t2.getSentAt()))
              .collect(Collectors.toList());
      new ArrayList<GetUserToUserMessageResponseDO>();

      for (GetUserToUserMessageResponseDO msgDO : sortedMessages) {
        logger.info("do {}", JacksonUtils.toJson(msgDO));
      }

      GetUserToUserMessagesResponseDO userMessagesDO =
          new GetUserToUserMessagesResponseDO(sortedMessages);
      return userMessagesDO;
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
    }
    return null;
  }
}
