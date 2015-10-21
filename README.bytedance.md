# twemproxy in Bytedance

We're using twemproxy in Bytedance since 2014. It's simple to use, but we implemented more features to meet our needs, including:

+ Features
  + Failover to slave: support master/slaves in server list, and support read_prefer strategy(none/slave/master). 
  + Reuse port(SO_REUSEPORT) to let multiple instances to listen on the same port.
  + Support lpm(loggest prefix match) IP matching to prefer local backends in the same IDC/rack. 
+ Performance
  + Try to fix mget performance issue.
  + Bounded free pool of mbuf/connection.
  + Introduce new option no-async to disable async request handling.
+ Robust
  + Maintain a safe number for max client connections to solve ENFILE issue
+ Util
  + Support access log by sampling.
  + Support log limit by time.
  + Add more stats.
  + Log rotation daily.
