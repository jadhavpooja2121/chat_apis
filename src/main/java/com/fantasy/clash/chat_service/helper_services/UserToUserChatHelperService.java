package com.fantasy.clash.chat_service.helper_services;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.cassandra.mapping.Column;
import org.springframework.http.HttpStatus;
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
import com.fantasy.clash.chat_service.constants.PropertyConstants;
import com.fantasy.clash.chat_service.constants.RedisConstants;
import com.fantasy.clash.chat_service.constants.ResponseErrorCodes;
import com.fantasy.clash.chat_service.constants.ResponseErrorMessages;
import com.fantasy.clash.chat_service.dos.GetUserChatsResponseDO;
import com.fantasy.clash.chat_service.dos.GetUserToUserMessageResponseDO;
import com.fantasy.clash.chat_service.dos.GetUserToUserMessagesResponseDO;
import com.fantasy.clash.chat_service.dos.SendUserToUserMessageResponseDO;
import com.fantasy.clash.chat_service.dos.UserActiveChatsDO;
import com.fantasy.clash.chat_service.dos.UserToUserChatDO;
import com.fantasy.clash.chat_service.dos.UserToUserChatResponseDO;
import com.fantasy.clash.chat_service.rest_clients.UserServiceAccountsRestClient;
import com.fantasy.clash.chat_service.utils.HashUtils;
import com.fantasy.clash.chat_service.utils.RedisServiceUtils;
import com.fantasy.clash.chat_service.utils.TimeConversionUtils;
import com.fantasy.clash.framework.configuration.Configurator;
import com.fantasy.clash.framework.http.dos.BaseResponseDO;
import com.fantasy.clash.framework.http.dos.ErrorResponseDO;
import com.fantasy.clash.framework.http.dos.OkResponseDO;
import com.fantasy.clash.framework.object_collection.user_service.dos.UserDO;
import com.fantasy.clash.framework.object_collection.user_service.dos.UsersDO;
import com.fantasy.clash.framework.redis.cluster.ClusteredRedis;
import com.fantasy.clash.framework.utils.JacksonUtils;
import com.fantasy.clash.framework.utils.StringUtils;

@Service
public class UserToUserChatHelperService {
  private static final Logger logger = LoggerFactory.getLogger(UserToUserChatHelperService.class);

  @Autowired
  @Qualifier("chatSession")
  private Session chatSession;

  @Autowired
  private Configurator configurator;

  @Autowired
  private ClusteredRedis redis;

  @Autowired
  private UserServiceAccountsRestClient userServiceAccountsRestClient;

  public SendUserToUserMessageResponseDO saveMessage(String groupChatId, String username,
      String recipient, String message, Long timestamp) {
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
              List.of(username, recipient, timestamp))
          .using(QueryBuilder.ttl(CassandraConstants.DEFAULT_CHAT_DATA_EXPIRY_TTL))
          .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
      chatSession.execute(insertUserLastReadTime);

      Statement getUserLastReadTime =
          QueryBuilder.select(DatabaseConstants.LAST_ACTIVE_TIMESTAMP_COLUMN)
              .from(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.ACTIVE_CHATS_TABLE)
              .where(QueryBuilder.eq(DatabaseConstants.USERNAME1_COLUMN, username))
              .and(QueryBuilder.eq(DatabaseConstants.USERNAME2_COLUMN, recipient));

      ResultSet result = chatSession.execute(getUserLastReadTime);
      Long user1LastReadtime = null;
      for (Row lastRead : result.all()) {
        user1LastReadtime = lastRead.getLong(0);
      }

      Statement getUser2LastReadTime =
          QueryBuilder.select(DatabaseConstants.LAST_ACTIVE_TIMESTAMP_COLUMN)
              .from(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.ACTIVE_CHATS_TABLE)
              .where(QueryBuilder.eq(DatabaseConstants.USERNAME1_COLUMN, recipient))
              .and(QueryBuilder.eq(DatabaseConstants.USERNAME2_COLUMN, username));

      ResultSet user2LastReadResult = chatSession.execute(getUser2LastReadTime);
      Long user2LastReadtime = null;
      for (Row lastRead : user2LastReadResult.all()) {
        user2LastReadtime = lastRead.getLong(0);
      }

      if (user2LastReadtime == null) {
        Long defaultUser2LastRead = user1LastReadtime - 100;
        Statement insertUser2LastReadTime = QueryBuilder
            .insertInto(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.ACTIVE_CHATS_TABLE)
            .values(
                List.of(DatabaseConstants.USERNAME1_COLUMN, DatabaseConstants.USERNAME2_COLUMN,
                    DatabaseConstants.LAST_ACTIVE_TIMESTAMP_COLUMN),
                List.of(recipient, username, defaultUser2LastRead))
            .using(QueryBuilder.ttl(CassandraConstants.DEFAULT_CHAT_DATA_EXPIRY_TTL))
            .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
        chatSession.execute(insertUser2LastReadTime);
      }
      SendUserToUserMessageResponseDO sendUserToUserMessageResponseDO =
          new SendUserToUserMessageResponseDO(username, recipient, message, timestamp);
      return sendUserToUserMessageResponseDO;
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
    }
    return null;
  }

  public GetUserToUserMessagesResponseDO getUserMessages(String groupChatId, String username,
      String sender, Long timestamp, boolean isNext) {
    try {
      if (timestamp == 0) {
        Statement readMessages = QueryBuilder
            .select(DatabaseConstants.MESSAGE_SENDER_COLUMN, DatabaseConstants.MESSAGE_TEXT_COLUMN,
                DatabaseConstants.MESSAGE_SENT_AT_TIMESTAMP_COLUMN)
            .from(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.USER_CHATS_TABLE)
            .where(QueryBuilder.eq(DatabaseConstants.CHAT_IDENTIFIER_COLUMN, groupChatId))
            .and(QueryBuilder.in("sender", List.of(username, sender)));

        ResultSet result = chatSession.execute(readMessages);

        Statement getUserLastReadTime =
            QueryBuilder.select(DatabaseConstants.LAST_ACTIVE_TIMESTAMP_COLUMN)
                .from(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.ACTIVE_CHATS_TABLE)
                .where(QueryBuilder.eq(DatabaseConstants.USERNAME1_COLUMN, username))
                .and(QueryBuilder.eq(DatabaseConstants.USERNAME2_COLUMN, sender));
        ResultSet getUserLastReadTimeResult = chatSession.execute(getUserLastReadTime);
        Long userLastReadtime = null;
        for (Row lastRead : getUserLastReadTimeResult.all()) {
          userLastReadtime = lastRead.getLong(0);
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

        if (CollectionUtils.isEmpty(allMessages)) {
          return null;
        }

        List<GetUserToUserMessageResponseDO> sortedMessages =
            allMessages.stream().sorted((t1, t2) -> t1.getSentAt().compareTo(t2.getSentAt()))
                .collect(Collectors.toList());

        logger.info("sorted message list {}", JacksonUtils.toJson(sortedMessages));

        // Get 100 messages only
        // TODO: change limit to 100

        List<GetUserToUserMessageResponseDO> pageN = sortedMessages.stream()
            .limit(configurator.getInt(PropertyConstants.PAGE_SIZE)).collect(Collectors.toList());

        GetUserToUserMessagesResponseDO userMessagesDO = new GetUserToUserMessagesResponseDO(pageN);

        Long lastRead = pageN.get(pageN.size() - 1).getSentAt();

        if (userLastReadtime < lastRead) {
          Statement updateActiveChat = QueryBuilder
              .insertInto(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.ACTIVE_CHATS_TABLE)
              .values(
                  List.of(DatabaseConstants.USERNAME1_COLUMN, DatabaseConstants.USERNAME2_COLUMN,
                      DatabaseConstants.LAST_ACTIVE_TIMESTAMP_COLUMN),
                  List.of(username, sender, lastRead))
              .using(QueryBuilder.ttl(CassandraConstants.DEFAULT_CHAT_DATA_EXPIRY_TTL))
              .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
          chatSession.execute(updateActiveChat);
        }
        return userMessagesDO;
      } else if (timestamp != 0 && isNext == true) {
        Long fromTimestamp = timestamp;
        GetUserToUserMessagesResponseDO nextMessages =
            getNextMessages(groupChatId, username, sender, fromTimestamp);
        return nextMessages;
      } else {
        Long fromTimestamp = timestamp;
        GetUserToUserMessagesResponseDO prevMessages =
            getPrevMessages(groupChatId, username, sender, fromTimestamp);
        return prevMessages;
      }
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
    }
    return null;
  }

  public GetUserToUserMessagesResponseDO getNextMessages(String groupChatId, String username,
      String sender, Long fromTimestamp) {
    try {
      Statement getMessages = QueryBuilder
          .select(DatabaseConstants.MESSAGE_SENDER_COLUMN, DatabaseConstants.MESSAGE_TEXT_COLUMN,
              DatabaseConstants.MESSAGE_SENT_AT_TIMESTAMP_COLUMN)
          .from(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.USER_CHATS_TABLE)
          .where(QueryBuilder.eq(DatabaseConstants.CHAT_IDENTIFIER_COLUMN, groupChatId))
          .and(QueryBuilder.gt(DatabaseConstants.MESSAGE_SENT_AT_TIMESTAMP_COLUMN, fromTimestamp))
          .and(QueryBuilder.in("sender", List.of(username, sender)));
      ResultSet result = chatSession.execute(getMessages);

      Statement getUserLastReadTime =
          QueryBuilder.select(DatabaseConstants.LAST_ACTIVE_TIMESTAMP_COLUMN)
              .from(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.ACTIVE_CHATS_TABLE)
              .where(QueryBuilder.eq(DatabaseConstants.USERNAME1_COLUMN, username))
              .and(QueryBuilder.eq(DatabaseConstants.USERNAME2_COLUMN, sender));
      ResultSet getUserLastReadTimeResult = chatSession.execute(getUserLastReadTime);
      Long userLastReadtime = null;
      for (Row lastRead : getUserLastReadTimeResult.all()) {
        userLastReadtime = lastRead.getLong(0);
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

      if (CollectionUtils.isEmpty(allMessages)) {
        return null;
      }

      List<GetUserToUserMessageResponseDO> sortedMessages =
          allMessages.stream().sorted((t1, t2) -> t1.getSentAt().compareTo(t2.getSentAt()))
              .collect(Collectors.toList());

      // Get 100 messages only
      // TODO: change limit to 100

      List<GetUserToUserMessageResponseDO> pageN = sortedMessages.stream()
          .limit(configurator.getInt(PropertyConstants.PAGE_SIZE)).collect(Collectors.toList());

      GetUserToUserMessagesResponseDO userMessagesDO = new GetUserToUserMessagesResponseDO(pageN);

      Long lastReadTime = pageN.get(pageN.size() - 1).getSentAt();

      if (userLastReadtime < lastReadTime) {
        Statement updateActiveChat = QueryBuilder
            .insertInto(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.ACTIVE_CHATS_TABLE)
            .values(
                List.of(DatabaseConstants.USERNAME1_COLUMN, DatabaseConstants.USERNAME2_COLUMN,
                    DatabaseConstants.LAST_ACTIVE_TIMESTAMP_COLUMN),
                List.of(username, sender, lastReadTime))
            .using(QueryBuilder.ttl(CassandraConstants.DEFAULT_CHAT_DATA_EXPIRY_TTL))
            .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
        chatSession.execute(updateActiveChat);
      }
      return userMessagesDO;
    } catch (Exception e) {
      logger.info(StringUtils.printStackTrace(e));
    }
    return null;
  }

  public GetUserToUserMessagesResponseDO getPrevMessages(String groupChatId, String username,
      String sender, Long fromTimestamp) {
    logger.info("Inside get prev");
    try {
      Statement getMessages = QueryBuilder
          .select(DatabaseConstants.MESSAGE_SENDER_COLUMN, DatabaseConstants.MESSAGE_TEXT_COLUMN,
              DatabaseConstants.MESSAGE_SENT_AT_TIMESTAMP_COLUMN)
          .from(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.USER_CHATS_TABLE)
          .where(QueryBuilder.eq(DatabaseConstants.CHAT_IDENTIFIER_COLUMN, groupChatId))
          .and(QueryBuilder.lt(DatabaseConstants.MESSAGE_SENT_AT_TIMESTAMP_COLUMN, fromTimestamp))
          .and(QueryBuilder.in("sender", List.of(username, sender)));
      ResultSet result = chatSession.execute(getMessages);

      Statement getUserLastReadTime =
          QueryBuilder.select(DatabaseConstants.LAST_ACTIVE_TIMESTAMP_COLUMN)
              .from(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.ACTIVE_CHATS_TABLE)
              .where(QueryBuilder.eq(DatabaseConstants.USERNAME1_COLUMN, username));
      ResultSet getUserLastReadTimeResult = chatSession.execute(getUserLastReadTime);
      Long userLastReadtime = null;
      for (Row lastRead : getUserLastReadTimeResult.all()) {
        userLastReadtime = lastRead.getLong(0);
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

      if (CollectionUtils.isEmpty(allMessages)) {
        return null;
      }

      List<GetUserToUserMessageResponseDO> sortedMessages =
          allMessages.stream().sorted((t1, t2) -> t1.getSentAt().compareTo(t2.getSentAt()))
              .collect(Collectors.toList());

      // Get 100 messages only
      // TODO: change limit to 100
      List<GetUserToUserMessageResponseDO> pageN = new ArrayList<>();
      try {
        pageN = sortedMessages.subList(
            sortedMessages.size() - configurator.getInt(PropertyConstants.PAGE_SIZE),
            sortedMessages.size());

        GetUserToUserMessagesResponseDO userMessagesDO = new GetUserToUserMessagesResponseDO(pageN);
        return userMessagesDO;
      } catch (Exception e) {
        pageN = new ArrayList<>(sortedMessages);
        GetUserToUserMessagesResponseDO userMessagesDO = new GetUserToUserMessagesResponseDO(pageN);
        return userMessagesDO;
      }

    } catch (Exception e) {
      logger.info(StringUtils.printStackTrace(e));
    }
    return null;
  }

  public GetUserChatsResponseDO getActiveChats(String username) {
    try {
      Statement selectActiveUsersAndLastActiveAt = QueryBuilder
          .select(DatabaseConstants.USERNAME2_COLUMN,
              DatabaseConstants.LAST_ACTIVE_TIMESTAMP_COLUMN)
          .from(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.ACTIVE_CHATS_TABLE)
          .where(QueryBuilder.eq(DatabaseConstants.USERNAME1_COLUMN, username));

      ResultSet activeUsersAndLastActiveTimestamp =
          chatSession.execute(selectActiveUsersAndLastActiveAt);

      List<UserActiveChatsDO> activeChatUsersAndLastActiveAtList = new ArrayList<>();
      for (Row user : activeUsersAndLastActiveTimestamp.all()) {
        UserActiveChatsDO userAndLastSeenAt =
            new UserActiveChatsDO(user.getString(0), user.getLong(1));
        activeChatUsersAndLastActiveAtList.add(userAndLastSeenAt);
      }

      List<UserToUserChatResponseDO> userChats = new ArrayList<>();

      for (int i = 0; i < activeChatUsersAndLastActiveAtList.size(); i++) {
        UserActiveChatsDO userActiveChatsDO = activeChatUsersAndLastActiveAtList.get(i);
        String sender = userActiveChatsDO.getAttender();
        Long lastRead = userActiveChatsDO.getLastMessageAt();

        String groupChatId = HashUtils.getHash(username, sender);

        Statement getUserLatestMessages = QueryBuilder.select(DatabaseConstants.MESSAGE_TEXT_COLUMN)
            .from(DatabaseConstants.DEFAULT_KEYSPACE, DatabaseConstants.USER_CHATS_TABLE)
            .allowFiltering()
            .where(QueryBuilder.eq(DatabaseConstants.MESSAGE_SENDER_COLUMN, sender))
            .and(QueryBuilder.eq(DatabaseConstants.CHAT_IDENTIFIER_COLUMN, groupChatId))
            .and(QueryBuilder.gt(DatabaseConstants.MESSAGE_SENT_AT_TIMESTAMP_COLUMN, lastRead));
        ResultSet userLatestMessages = chatSession.execute(getUserLatestMessages);
        int count = 0;
        for (Row result : userLatestMessages.all()) {
          count++;
        }

        UserToUserChatResponseDO userChatResponseDO = new UserToUserChatResponseDO();
        userChatResponseDO.setSender(sender);
        Long lastMessageTime = TimeConversionUtils.getGMTTime() - lastRead;
        userChatResponseDO.setLastMessageTime(lastMessageTime);
        userChatResponseDO.setNotificationText(String.format("%s new message(s)", count));
        userChats.add(userChatResponseDO);
      }

      List<UserToUserChatResponseDO> sortedUserChats = userChats.stream()
          .sorted((t1, t2) -> t1.getLastMessageTime().compareTo(t2.getLastMessageTime()))
          .collect(Collectors.toList());

      List<UserToUserChatDO> userLatestChats = new ArrayList<>();
      for (UserToUserChatResponseDO userChat : sortedUserChats) {
        String lastMessagedAt =
            TimeConversionUtils.convertTimeIntoString(userChat.getLastMessageTime());
        UserToUserChatDO chat = new UserToUserChatDO();
        chat.setSender(userChat.getSender());
        chat.setLastMessageTime(lastMessagedAt);
        chat.setNotificationText(userChat.getNotificationText());
        userLatestChats.add(chat);
      }

      getResponseWithUserImage(userLatestChats);

      GetUserChatsResponseDO getUserChatsResponseDO = new GetUserChatsResponseDO(userLatestChats);
      return getUserChatsResponseDO;

    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
    }
    return null;
  }

  private List<UserToUserChatDO> getResponseWithUserImage(List<UserToUserChatDO> userLatestChats) {
    List<String> listOfSenders = new ArrayList<>();
    for (UserToUserChatDO sender : userLatestChats) {
      listOfSenders.add(sender.getSender());
    }

    logger.info("senders list {}", JacksonUtils.toJson(listOfSenders));
    String imageUrl = "";
    for (UserToUserChatDO userChatMetaData : userLatestChats) {
      String userAccount = redis.get(RedisConstants.REDIS_ALIAS,
          RedisServiceUtils.userAccountKey(userChatMetaData.getSender()));
      if (StringUtils.isNullOrEmpty(userAccount)) {
        UsersDO user = new UsersDO();
        user.setUsernames(listOfSenders);
        BaseResponseDO baseResponse = userServiceAccountsRestClient.getUsersAccounts(user);
        if (baseResponse == null || baseResponse.code != HttpStatus.OK.value()) {
          // cannot throw error response here as it will affect the next processing inside the
          // loop
          imageUrl = "http://noImage.png";
        } else {
          OkResponseDO<UsersDO> usersAccounts =
              (OkResponseDO<UsersDO>) userServiceAccountsRestClient.getUsersAccounts(user);
          UsersDO users =
              JacksonUtils.fromJson(JacksonUtils.toJson(usersAccounts.result), UsersDO.class);
          List<UserDO> usersList = users.getUsers();

          Predicate<UserDO> sender =
              userDO -> (userDO.getUsername().equals(userChatMetaData.getSender()));
          // Boolean isUserExists = usersList.stream().anyMatch(sender);

          UserDO userInfo = usersList.stream().filter(sender).findAny().get();

          imageUrl = userInfo.getProfile().getImageUrl();

          if (StringUtils.isNotNullAndEmpty(imageUrl)) {
            // set a default image
            imageUrl = "http://noImage.png";
          }
          redis.setex(RedisConstants.REDIS_ALIAS,
              RedisServiceUtils.userAccountKey(userChatMetaData.getSender()),
              JacksonUtils.toJson(userInfo), RedisConstants.REDIS_30_DAYS_KEY_TTL);
        }
      } else {
        UserDO userData = JacksonUtils.fromJson(userAccount, UserDO.class);
        imageUrl = userData.getProfile().getImageUrl();
      }
      userChatMetaData.setImageUrl(imageUrl);
    }
    return userLatestChats;
  }
}
