CREATE KEYSPACE IF NOT EXISTS events_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TABLE IF NOT EXISTS events_keyspace.events (
  user_id int,
  username text,
  action text,
  value double,
  timestamp bigint,
  PRIMARY KEY (user_id, timestamp)
);
