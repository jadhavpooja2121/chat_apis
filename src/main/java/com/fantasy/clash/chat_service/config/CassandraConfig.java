package com.fantasy.clash.chat_service.config;

import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.datastax.driver.core.Session;
import com.fantasy.clash.chat_service.constants.CassandraConstants;
import com.fantasy.clash.framework.cassandra.CassandraConfiguration;

@Configuration
public class CassandraConfig {
  private static final Logger logger = LoggerFactory.getLogger(CassandraConfig.class);

  @Autowired
  private CassandraConfiguration cassandraConfiguration;

  private Session session;

  @PostConstruct
  public void init() throws Exception {
    logger.info("Creating session...");
    session = cassandraConfiguration.getSession(CassandraConstants.CASSANDRA_ALIAS);
  }

  @Bean("chatSession")
  public Session getSession() {
    return session;
  }

}
