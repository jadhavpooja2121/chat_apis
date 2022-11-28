package com.fantasy.clash.chat_service.helper_services;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.fantasy.clash.chat_service.constants.CassandraConstants;
import com.fantasy.clash.chat_service.dos.SendUserToUserMessageResponseDO;
import com.fantasy.clash.framework.utils.StringUtils;

@Service
public class UserToUserChatHelperService {
  private static final Logger logger = LoggerFactory.getLogger(UserToUserChatHelperService.class);

  @Qualifier("chatSession")
  private Session chatSession;

  public SendUserToUserMessageResponseDO saveMessage(String hash, String username,
      String otherUserName, String message, Long timestamp) {
    try {
      Statement statement = QueryBuilder.insertInto("chat_service", "userchats")
          .values(List.of("hash", "sender", "chattime", "text"),
              List.of(hash, username, timestamp, message))
          .using(QueryBuilder.ttl(CassandraConstants.DEFAULT_CHAT_DATA_EXPIRY_TTL))
          .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
      chatSession.execute(statement);
      SendUserToUserMessageResponseDO sendUserToUserMessageResponseDO =
          new SendUserToUserMessageResponseDO(username, otherUserName, timestamp);
      return sendUserToUserMessageResponseDO;
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
      return null;
    }
  }
}
