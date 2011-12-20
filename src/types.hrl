% common types

-type now() :: {integer(), integer(), integer()}.

-type ip_address() :: {integer(), integer(), integer(), integer()}.

-record(address, {host, port}).
-type address() :: #address{
	       host :: ip_address(),
	       port :: integer()
		       }.

-type 'end'() :: {'end', binary()}.
-type bits() :: list(boolean()).

-record(tap, {subtaps}).
-type tap() :: #tap{
	   subtaps :: list(subtap())
		      }.

-record(subtap, {is, has}).
-type subtap() :: #subtap{
	      is :: list({Key :: th_telex:json_string(), Value :: th_telex:json()}),
	      has :: list(Key :: th_telex:json_string())
		 }.
