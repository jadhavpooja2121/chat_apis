package com.fantasy.clash.chat_service.rest_clients;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import com.datastax.oss.driver.shaded.guava.common.net.HttpHeaders;
import com.fantasy.clash.chat_service.utils.internal_services.UserServiceUtils;
import com.fantasy.clash.framework.http.client.RestClient;
import com.fantasy.clash.framework.http.dos.OkResponseDO;
import com.fantasy.clash.framework.object_collection.user_service.dos.UsersDO;
import com.fantasy.clash.framework.utils.JacksonUtils;
import com.fantasy.clash.framework.utils.StringUtils;
import com.fasterxml.jackson.core.type.TypeReference;

@Service
public class UserServiceAccountsRestClient {
  private static final Logger logger = LoggerFactory.getLogger(UserServiceAccountsRestClient.class);

  @Autowired
  private UserServiceUtils userServiceUtils;

  @Autowired
  private RestClient restClient;

  public UsersDO getUsersAccounts(UsersDO usersDO) {
    try {

      String usersAccountsUrl = userServiceUtils.getUsersAccountsUrl();
      logger.info("usersAccountsUrl {}", usersAccountsUrl);

      Map<String, String> headers = new HashMap<String, String>();
      headers.put(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
      headers.put("x-mock-match-request-body", "true");

      String body = JacksonUtils.toJson(usersDO);

      ResponseEntity<?> rawResponse = restClient.post(usersAccountsUrl, headers, body);
      if (rawResponse.getStatusCode().value() == HttpStatus.OK.value()
          && StringUtils.isNotNullAndEmpty(rawResponse.getBody().toString())) {
        OkResponseDO<UsersDO> userAccounts = JacksonUtils.fromJson(rawResponse.getBody().toString(),
            new TypeReference<OkResponseDO<UsersDO>>() {});
        logger.info("result {}", userAccounts.result);
        logger.info("accounts {}", userAccounts.result.getUsers());
        return userAccounts.result;
      }
    } catch (Exception e) {
      logger.error("Exception in get user accounts due to {}", StringUtils.printStackTrace(e));
    }
    return null;
  }
}
