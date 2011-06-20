% configuration values
% will at some point be replaced by a dynamically loaded configuration file

-define(TELEX_MAX_BYTES, 1400).
-define(DEFAULT_PORT, 42424).
-define(END_BITS, 160).
-define(REPLICATION, 20).
-define(DIALER_PARALLEL_REQUESTS, 3).
-define(DIALER_PING_TIMEOUT, 1000).
-define(ROUTER_REFRESH_TIME, 60*60*1000). % one hour
-define(ROUTER_PING_TIMEOUT, 1000).
-define(ROUTER_DIAL_TIMEOUT, 10000).
-define(TELEHASH_ORG, #address{host={208,68,163,247}, port=42424}).
