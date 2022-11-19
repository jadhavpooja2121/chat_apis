package com.fantasy.clash.chat_service.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import com.fantasy.clash.chat_service.constants.PropertyConstants;
import com.fantasy.clash.framework.configuration.Configurator;

@Configuration
public class AsyncConfig {
  
  @Autowired
  private Configurator configurator;

  @Bean(name = "apiTaskExecutor")
  public ThreadPoolTaskExecutor asyncExecutor() {
    ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
    threadPoolTaskExecutor
        .setMaxPoolSize(configurator.getInt(PropertyConstants.ASYNC_THREAD_POOL_MAX_POOL_SIZE,
            PropertyConstants.DEFAULT_MAX_POOL_SIZE));
    threadPoolTaskExecutor
        .setCorePoolSize(configurator.getInt(PropertyConstants.ASYNC_THREAD_POOL_CORE_POOL_SIZE,
            PropertyConstants.DEFAULT_CORE_POOL_SIZE));
    threadPoolTaskExecutor
        .setQueueCapacity(configurator.getInt(PropertyConstants.ASYNC_THREAD_POOL_QUEUE_CAPACITY,
            PropertyConstants.DEFAULT_QUEUE_CAPACITY));
    threadPoolTaskExecutor.setAwaitTerminationSeconds(
        configurator.getInt(PropertyConstants.ASYNC_THREAD_POOL_AWAIT_TERMINATION_SECONDS,
            PropertyConstants.DEFAULT_AWAIT_TERMINATION_SECONDS));
    return threadPoolTaskExecutor;
  }
}
