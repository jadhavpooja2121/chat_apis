package com.fantasy.clash.chat_service.utils.internal_services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.fantasy.clash.chat_service.constants.RestClientServiceConstants;
import com.fantasy.clash.framework.configuration.Configurator;

@Service
public class UserServiceUtils {
  private static final Logger logger = LoggerFactory.getLogger(UserServiceUtils.class);
  @Autowired
  private Configurator configurator;

  public String getUsersAccountsUrl() {
    String userServiceGetAccountsUrl =
        configurator.getString(RestClientServiceConstants.USERS_ACCOUNTS);
    return userServiceGetAccountsUrl;
  }

}
