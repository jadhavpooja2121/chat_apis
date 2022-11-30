package com.fantasy.clash.chat_service.helper_services;

import java.sql.Timestamp;
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

      Statement statement = QueryBuilder.insertInto("chat_service", "chats")
          .values(List.of("group_chat_id", "sender", "sent_at", "message"),
              List.of(groupChatId, username, timestamp, message))
          .using(QueryBuilder.ttl(CassandraConstants.DEFAULT_CHAT_DATA_EXPIRY_TTL))
          .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
      chatSession.execute(statement);

      Statement st = QueryBuilder.insertInto("chat_service", "active_chats")
          .values(List.of("username", "username2", "last_active_at"),
              List.of(username, username2, timestamp))
          .using(QueryBuilder.ttl(CassandraConstants.DEFAULT_CHAT_DATA_EXPIRY_TTL))
          .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
      chatSession.execute(st);

      SendUserToUserMessageResponseDO sendUserToUserMessageResponseDO =
          new SendUserToUserMessageResponseDO(username, username2, timestamp);
      return sendUserToUserMessageResponseDO;
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
      return null;
    }
  }

  public GetUserToUserMessagesResponseDO getUserMessages(String groupChatId, String username,
      String username2) {
    try {
      Statement st = QueryBuilder.select("message", "sent_at").from("chat_service", "chats")
          .where(QueryBuilder.eq("group_chat_id", groupChatId))
          .and(QueryBuilder.in("sender", List.of(username, username2)));
      // .orderBy(QueryBuilder.desc("sent_at"));

      ResultSet result = chatSession.execute(st);

      // logger.info("resultset {}", result.all());
      // for (Row msg : result.all()) {
      // logger.info("get message result:{}", msg);
      // }
      //
      // List<String> chatMessages =
      // result.all().stream().map(row -> row.toString()).collect(Collectors.toList());
      // logger.info(JacksonUtils.toJson(chatMessages));

      Map<Long, String> messages = new TreeMap<>();
      for (Row msg : result.all()) {
        messages.put( msg.getLong(1), msg.getString(0));
      }
      logger.info("map:{}", JacksonUtils.toJson(messages));
      // TODO: update activechat for username

      NavigableMap<Long, String> allMessages = new TreeMap<>(messages);
      NavigableMap<Long, String> messagesInDescOrder = allMessages.descendingMap();
      logger.info("all messages in desc order {}", JacksonUtils.toJson(messagesInDescOrder));

      GetUserToUserMessagesResponseDO userMessagesDO =
          new GetUserToUserMessagesResponseDO(messagesInDescOrder);
      return userMessagesDO;
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
    }
    return null;
  }
}
