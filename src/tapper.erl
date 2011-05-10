% tapper handles .tap commands received from other nodes
% and sends replies whenever matching signals are received

-module(tapper).

-include("types.hrl").
-include("conf.hrl").
-include("log.hrl").

-define(K, ?REPLICATION)

-export([send_tap/2])

% --- api ---

send_tap(End, Tap, Timeout) ->
    Addresses = router:nearest(?K, End, Timeout),
    Addresses2 = dialer:dial_sync(End, Addresses, Timeoutr),
    Telex = telex:tap_command(Tap),
    lists:foreach(
      fun (Address) ->
	      switch:send(Address, Telex)
      end,
      Addresses2
     ). % maybe wait for confirmation?

% --- internal functions ---

all(List, Fun) -> lists:all(Fun, List).
any(List, Fun) -> lists:any(Fun, List).

% if the tap matches the telex return the matching signals, otherwise return none
matches(Tap, Telex) ->
    try
	{ok, matches_tap(Tap, Telex)}
    catch
	error:{badmatch,_} ->
	    false
    end.

matches(#tap{subtaps=Subtaps}, Telex) -> 
    lists:any(fun (Subtap) -> matches_subtap(Subtap, Telex), Subtaps).

matches_subtap(#subtap{is=Is, has=Has}, Telex) ->
    matches_is(Is, Telex) and matches_has(Has, Telex).

matches_is(Is, Telex) ->
    lists:all(
      fun ({Key, Value}) ->
	      {ok, Value} = telex:get(Telex, Key)
      end,
      Is
     ).

matches_has(Has, Telex) ->
    lists:all(
      fun (Arg) ->
	      {ok, _} = telex:get(Telex, Arg)
      end
     ).

% --- end ---



