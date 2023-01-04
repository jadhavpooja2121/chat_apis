package com.fantasy.clash.chat_service.config;

import java.io.IOException;
import java.util.List;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import com.fantasy.clash.chat_service.constants.RedisConstants;
import com.fantasy.clash.framework.configuration.Configurator;
import com.fantasy.clash.framework.redis.cluster.ClusteredRedis;
import com.fantasy.clash.framework.utils.CollectionUtils;

@Configuration
public class RedisConfig {

  @Autowired
  private ClusteredRedis redis;

  @Autowired
  private Configurator configurator;

  @PostConstruct
  private void init() throws IOException {
    List<String> nodes = CollectionUtils
        .convertCsvToList(configurator.getString(RedisConstants.REDIS_CLUSTER_NODES_CONFIG_KEY));
    redis.addAlias(RedisConstants.REDIS_ALIAS, nodes, null, null);
  }
}
