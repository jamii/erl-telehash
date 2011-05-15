% common types

-type ip_address() :: {integer(), integer(), integer(), integer()}.

-record(address, {
	  host :: ip_address(), 
	  port :: integer()
	}).
-type address() :: #address{}.

-type 'end'() :: {'end', binary()}. 

-record(tap, {subtaps}).
-record(subtap, {is, has}).
