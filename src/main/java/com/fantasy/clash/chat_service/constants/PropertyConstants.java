package com.fantasy.clash.chat_service.constants;

public class PropertyConstants {
  private PropertyConstants() {

  }

  public static final int DEFAULT_MAX_POOL_SIZE = 10;
  public static final int DEFAULT_CORE_POOL_SIZE = 5;
  public static final int DEFAULT_QUEUE_CAPACITY = 5000;
  public static final int DEFAULT_AWAIT_TERMINATION_SECONDS = 5;
  public static final String ASYNC_THREAD_POOL_MAX_POOL_SIZE = "async.threadpool.maxpoolsize";
  public static final String ASYNC_THREAD_POOL_CORE_POOL_SIZE = "async.threadpool.corepoolsize";
  public static final String ASYNC_THREAD_POOL_QUEUE_CAPACITY = "async.threadpool.queuecapacity";
  public static final String ASYNC_THREAD_POOL_AWAIT_TERMINATION_SECONDS =
      "async.threadpool.await.termination.seconds";
}
