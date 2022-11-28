package com.fantasy.clash.chat_service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration;
import org.springframework.boot.autoconfigure.data.cassandra.CassandraDataAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@ComponentScan("com.fantasy.clash")
@EnableAsync
@EnableAutoConfiguration(exclude={CassandraAutoConfiguration.class})
public class ChatServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(ChatServiceApplication.class, args);
  }
}
