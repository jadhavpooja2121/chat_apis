package com.fantasy.clash.chat_service.constants;

public class CassandraConstants {
  private CassandraConstants() {

  }

  public static final String CASSANDRA_ALIAS = "fantasy_clash";
  public static final String KEYSPACENAME = "cassandra.fantasy_clash.keyspacename";
  public static final String CONTACTPOINTS = "cassandra.fantasy_clash.contactpoints";
  public static final String PORT = "cassandra.fantasy_clash.port";
  public static final Integer DEFAULT_CHAT_DATA_EXPIRY_TTL = 30 * 24 * 60 * 60;

}
