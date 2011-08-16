% test utils

-module(th_test).

-include("conf.hrl").
-include("types.hrl").

-include_lib("proper/include/proper.hrl").

-export([ip_address/0, address/0, hex/0, 'end'/0]).

-define(MAX_PORT, 65535).

ip_address() ->
    {integer(0, 255), integer(0, 255), integer(0, 255), integer(0, 255)}.

address() ->
    #address{host=ip_address(), port=integer(0, ?MAX_PORT)}.

hex() ->
    ?LET(Chars, vector(round(?END_BITS / 4), oneof("0123456789ABCDEFabcdef")),
	 iolist_to_binary(Chars)
	).

'end'() ->
    {'end', binary(round(?END_BITS / 8))}.
