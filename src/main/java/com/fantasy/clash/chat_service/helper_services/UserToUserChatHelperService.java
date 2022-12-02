package com.fantasy.clash.chat_service.helper_services;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.cassandra.mapping.Column;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.fantasy.clash.chat_service.constants.CassandraConstants;
import com.fantasy.clash.chat_service.constants.DatabaseConstants;
import com.fantasy.clash.chat_service.constants.ResponseErrorCodes;
import com.fantasy.clash.chat_service.constants.ResponseErrorMessages;
import com.fantasy.clash.chat_service.dos.GetUserToUserMessageResponseDO;
import com.fantasy.clash.chat_service.dos.GetUserToUserMessagesResponseDO;
import com.fantasy.clash.chat_service.dos.SendUserToUserMessageResponseDO;
import com.fantasy.clash.chat_service.utils.TimeConversionUtils;
import com.fantasy.clash.framework.http.dos.ErrorResponseDO;
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

      Statement insertChat = QueryBuilder
          .insertInto(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.USER_CHATS_TABLE)
          .values(
              List.of(DatabaseConstants.CHAT_IDENTIFIER_COLUMN,
                  DatabaseConstants.MESSAGE_SENDER_COLUMN,
                  DatabaseConstants.MESSAGE_SENT_AT_TIMESTAMP_COLUMN,
                  DatabaseConstants.MESSAGE_TEXT_COLUMN),
              List.of(groupChatId, username, timestamp, message))
          .using(QueryBuilder.ttl(CassandraConstants.DEFAULT_CHAT_DATA_EXPIRY_TTL))
          .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
      chatSession.execute(insertChat);

      Statement insertUserLastReadTime = QueryBuilder
          .insertInto(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.ACTIVE_CHATS_TABLE)
          .values(
              List.of(DatabaseConstants.USERNAME1_COLUMN, DatabaseConstants.USERNAME2_COLUMN,
                  DatabaseConstants.LAST_ACTIVE_TIMESTAMP_COLUMN),
              List.of(username, username2, timestamp))
          .using(QueryBuilder.ttl(CassandraConstants.DEFAULT_CHAT_DATA_EXPIRY_TTL))
          .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
      chatSession.execute(insertUserLastReadTime);

      // Add row for another user with timestamp less than 1st user's last read
      Statement getUserLastReadTime =
          QueryBuilder.select(DatabaseConstants.LAST_ACTIVE_TIMESTAMP_COLUMN)
              .from(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.ACTIVE_CHATS_TABLE)
              .where(QueryBuilder.eq(DatabaseConstants.USERNAME1_COLUMN, username));

      ResultSet result = chatSession.execute(getUserLastReadTime);
      Long user1LastReadtime = null;
      for (Row lastRead : result.all()) {
        user1LastReadtime = lastRead.getLong(0);
        logger.info("user1LastReadtime:{}", user1LastReadtime);
      }

      Long defaultUser2LastRead = user1LastReadtime - 100;
      logger.info("user1 last read:{}", defaultUser2LastRead);

      Statement insertUser2LastReadTime = QueryBuilder
          .insertInto(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.ACTIVE_CHATS_TABLE)
          .values(
              List.of(DatabaseConstants.USERNAME1_COLUMN, DatabaseConstants.USERNAME2_COLUMN,
                  DatabaseConstants.LAST_ACTIVE_TIMESTAMP_COLUMN),
              List.of(username2, username, defaultUser2LastRead))
          .using(QueryBuilder.ttl(CassandraConstants.DEFAULT_CHAT_DATA_EXPIRY_TTL))
          .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
      chatSession.execute(insertUser2LastReadTime);


      SendUserToUserMessageResponseDO sendUserToUserMessageResponseDO =
          new SendUserToUserMessageResponseDO(username, username2, message, timestamp);
      return sendUserToUserMessageResponseDO;
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
      return null;
    }
  }

  public GetUserToUserMessagesResponseDO getUserMessages(String groupChatId, String username,
      String username2, Long timestamp, boolean isNext) {
    try {
      if (timestamp == 0) {
        Statement readMessages = QueryBuilder
            .select(DatabaseConstants.MESSAGE_SENDER_COLUMN, DatabaseConstants.MESSAGE_TEXT_COLUMN,
                DatabaseConstants.MESSAGE_SENT_AT_TIMESTAMP_COLUMN)
            .from(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.USER_CHATS_TABLE)
            .where(QueryBuilder.eq(DatabaseConstants.CHAT_IDENTIFIER_COLUMN, groupChatId))
            .and(QueryBuilder.in("sender", List.of(username, username2)));

        ResultSet result = chatSession.execute(readMessages);

        Statement getUserLastReadTime =
            QueryBuilder.select(DatabaseConstants.LAST_ACTIVE_TIMESTAMP_COLUMN)
                .from(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.ACTIVE_CHATS_TABLE)
                .where(QueryBuilder.eq(DatabaseConstants.USERNAME1_COLUMN, username));
        ResultSet getUserLastReadTimeResult = chatSession.execute(getUserLastReadTime);
        Long user1LastReadtime = null;
        for (Row lastRead : getUserLastReadTimeResult.all()) {
          user1LastReadtime = lastRead.getLong(0);
          logger.info("user1LastReadtime:{}", user1LastReadtime);
        }

        List<GetUserToUserMessageResponseDO> allMessages =
            new ArrayList<GetUserToUserMessageResponseDO>();
        Boolean isRead = null;
        for (Row msg : result.all()) {
          isRead = msg.getLong(2) <= user1LastReadtime ? true : false;
          GetUserToUserMessageResponseDO message = new GetUserToUserMessageResponseDO(
              msg.getString(0), msg.getString(1), msg.getLong(2), isRead);
          allMessages.add(message);
        }

        logger.info("raw message list {}", JacksonUtils.toJson(allMessages));

        if (CollectionUtils.isEmpty(allMessages)) {
          return null;
        }

        List<GetUserToUserMessageResponseDO> sortedMessages =
            allMessages.stream().sorted((t1, t2) -> t1.getSentAt().compareTo(t2.getSentAt()))
                .collect(Collectors.toList());
        logger.info("sorted message list {}", JacksonUtils.toJson(sortedMessages));

        // Get 100 messages only
        // TODO: change limit to 100

        List<GetUserToUserMessageResponseDO> page1 =
            sortedMessages.stream().limit(5).collect(Collectors.toList());

        GetUserToUserMessagesResponseDO userMessagesDO = new GetUserToUserMessagesResponseDO(page1);

        Long lastRead = page1.get(page1.size() - 1).getSentAt();
        logger.info("last read in the list:{}", lastRead);

        Statement updateActiveChat = QueryBuilder
            .insertInto(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.ACTIVE_CHATS_TABLE)
            .values(
                List.of(DatabaseConstants.USERNAME1_COLUMN, DatabaseConstants.USERNAME2_COLUMN,
                    DatabaseConstants.LAST_ACTIVE_TIMESTAMP_COLUMN),
                List.of(username, username2, lastRead))
            .using(QueryBuilder.ttl(CassandraConstants.DEFAULT_CHAT_DATA_EXPIRY_TTL))
            .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
        chatSession.execute(updateActiveChat);

        return userMessagesDO;
      } else if (timestamp != 0 && isNext == true) {
        Long fromTimestamp = timestamp;
        GetUserToUserMessagesResponseDO nextMessages =
            getNextMessages(groupChatId, username, username2, fromTimestamp);
        return nextMessages;

      }
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
    }
    return null;
  }

  public GetUserToUserMessagesResponseDO getNextMessages(String groupChatId, String username,
      String username2, Long fromTimestamp) {
    logger.info("Inside get next");
    try {
      Statement getMessages = QueryBuilder
          .select(DatabaseConstants.MESSAGE_SENDER_COLUMN, DatabaseConstants.MESSAGE_TEXT_COLUMN,
              DatabaseConstants.MESSAGE_SENT_AT_TIMESTAMP_COLUMN)
          .from(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.USER_CHATS_TABLE)
          .where(QueryBuilder.eq(DatabaseConstants.CHAT_IDENTIFIER_COLUMN, groupChatId))
          .and(QueryBuilder.gt(DatabaseConstants.MESSAGE_SENT_AT_TIMESTAMP_COLUMN, fromTimestamp))
          .and(QueryBuilder.in("sender", List.of(username, username2)));
      ResultSet result = chatSession.execute(getMessages);

      Statement getUserLastReadTime =
          QueryBuilder.select(DatabaseConstants.LAST_ACTIVE_TIMESTAMP_COLUMN)
              .from(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.ACTIVE_CHATS_TABLE)
              .where(QueryBuilder.eq(DatabaseConstants.USERNAME1_COLUMN, username));
      ResultSet getUserLastReadTimeResult = chatSession.execute(getUserLastReadTime);
      Long userLastReadtime = null;
      for (Row lastRead : getUserLastReadTimeResult.all()) {
        userLastReadtime = lastRead.getLong(0);
        logger.info("user1LastReadtime:{}", userLastReadtime);
      }

      List<GetUserToUserMessageResponseDO> allMessages =
          new ArrayList<GetUserToUserMessageResponseDO>();
      Boolean isRead = null;
      for (Row msg : result.all()) {
        isRead = msg.getLong(2) <= userLastReadtime ? true : false;
        GetUserToUserMessageResponseDO message = new GetUserToUserMessageResponseDO(
            msg.getString(0), msg.getString(1), msg.getLong(2), isRead);
        allMessages.add(message);
      }

      logger.info("raw message list {}", JacksonUtils.toJson(allMessages));

      if (CollectionUtils.isEmpty(allMessages)) {
        return null;
      }

      List<GetUserToUserMessageResponseDO> sortedMessages =
          allMessages.stream().sorted((t1, t2) -> t1.getSentAt().compareTo(t2.getSentAt()))
              .collect(Collectors.toList());
      logger.info("sorted message list {}", JacksonUtils.toJson(sortedMessages));

      // Get 100 messages only
      // TODO: change limit to 100

      List<GetUserToUserMessageResponseDO> pageN =
          sortedMessages.stream().limit(5).collect(Collectors.toList());
      logger.info("page1 {}", JacksonUtils.toJson(pageN));

      GetUserToUserMessagesResponseDO userMessagesDO = new GetUserToUserMessagesResponseDO(pageN);

      Long lastReadTime = pageN.get(pageN.size() - 1).getSentAt();
      logger.info("last read in the list:{}", lastReadTime);

      Statement updateActiveChat = QueryBuilder
          .insertInto(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.ACTIVE_CHATS_TABLE)
          .values(
              List.of(DatabaseConstants.USERNAME1_COLUMN, DatabaseConstants.USERNAME2_COLUMN,
                  DatabaseConstants.LAST_ACTIVE_TIMESTAMP_COLUMN),
              List.of(username, username2, lastReadTime))
          .using(QueryBuilder.ttl(CassandraConstants.DEFAULT_CHAT_DATA_EXPIRY_TTL))
          .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
      chatSession.execute(updateActiveChat);

      return userMessagesDO;
    } catch (Exception e) {
      logger.info(StringUtils.printStackTrace(e));
    }
    return null;
  }
}
