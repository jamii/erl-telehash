% common types

-type now() :: {integer(), integer(), integer()}.

-type ip_address() :: {integer(), integer(), integer(), integer()}.

-record(address, {
	  host :: ip_address(), 
	  port :: integer()
	}).
-type address() :: #address{}.

-type 'end'() :: {'end', binary()}.
-type bits() :: list(boolean()). 

-record(tap, {subtaps}).
-record(subtap, {is, has}).
