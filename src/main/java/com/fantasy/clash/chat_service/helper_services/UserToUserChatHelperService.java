package com.fantasy.clash.chat_service.helper_services;

import java.sql.Timestamp;
import java.util.List;
import java.util.NavigableMap;
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
import com.fantasy.clash.chat_service.dos.SendUserToUserMessageResponseDO;
import com.fantasy.clash.framework.utils.StringUtils;

@Service
public class UserToUserChatHelperService {
  private static final Logger logger = LoggerFactory.getLogger(UserToUserChatHelperService.class);

  @Autowired
  @Qualifier("chatSession")
  private Session chatSession;

  public SendUserToUserMessageResponseDO saveMessage(String hash, String username,
      String otherUserName, String message, Long timestamp) {
    try {
      // Select s = QueryBuilder.select().from("userchats");
      Statement statement = QueryBuilder.insertInto("chat_service", "userchats")
          .values(List.of("hash", "sender", "chattime", "text"),
              List.of(hash, username, timestamp, message))
          .using(QueryBuilder.ttl(CassandraConstants.DEFAULT_CHAT_DATA_EXPIRY_TTL))
          .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
      chatSession.execute(statement);

      Statement st = QueryBuilder.insertInto("chat_service", "activechats")
          .values(List.of("username", "otherusername", "lastchattime"),
              List.of(username, otherUserName, timestamp))
          .using(QueryBuilder.ttl(CassandraConstants.DEFAULT_CHAT_DATA_EXPIRY_TTL))
          .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
      chatSession.execute(st);

      SendUserToUserMessageResponseDO sendUserToUserMessageResponseDO =
          new SendUserToUserMessageResponseDO(username, otherUserName, timestamp);
      return sendUserToUserMessageResponseDO;
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
      return null;
    }
  }

  public NavigableMap<String, Timestamp> getUserMessages(String hash, String username,
      String otherUserName) {
    Statement st = QueryBuilder.select("text", "chattime").from("chat_service", "userchats")
        .where(QueryBuilder.eq("hash", hash)).and(QueryBuilder.eq("sender", otherUserName))
        .orderBy(QueryBuilder.desc("chattime"));
    ResultSet result = chatSession.execute(st);
    logger.info("resultset {}", result.all());
    for (Row msg : result.all()) {
      logger.info("get message result:{}", msg);
    }
    // TODO: update activechat for username

    return null;
  }
}
