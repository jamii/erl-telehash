% tapper handles .tap commands received from other nodes
% and sends replies whenever matching signals are received

-module(th_tapper).

-include("types.hrl").
-include("conf.hrl").
-include("log.hrl").

-export([start_link/0, tap/1, send_tap/3, match/2]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(K, ?REPLICATION).

-record(state, {
	  taps :: dict() % dict mapping addresses to taps
	 }).

% --- api ---

tap(List) ->
    Subtaps =
        lists:map(
          fun (Sublist) ->
                  #subtap{
                    is = [{Key,Val} || {Key,Val} <- Sublist],
                    has = [Key || Key <- Sublist, is_list(Key) or is_binary(Key)]
                  }
          end,
          List),
    #tap{subtaps=Subtaps}.

-spec send_tap('end'(), tap(), timeout()) -> ok.
send_tap(End, Tap, Timeout) ->
    {ok, Addresses} = th_dialer:dial_sync(End, th_router:nearest(?K, End, Timeout), Timeout),
    Telex = th_telex:tap_command(Tap),
    lists:foreach(
      fun (Address) ->
	      th_udp:send(Address, Telex)
      end,
      Addresses
     ),
    {ok, Addresses}.

-spec start_link() -> {ok, pid()}.
start_link() ->
    ?INFO([starting]),
    State = #state{taps=dict:new()},
    {ok, _Pid} = gen_server:start_link({local, ?MODULE}, ?MODULE, State, []).

% --- gen_server callbacks ---

init(State) ->
    th_event:listen(),
    {ok, State}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Cast, State) ->
    {ok, State}.

handle_info({event, {recv, From, Telex}}, #state{taps=Taps}=State) ->
    Taps2 =
	case th_telex:get(Telex, '.tap') of
	    {ok, Json} ->
		try th_telex:json_to_tap(Json) of
		    Tap ->
			?INFO([new_tap, {from, From}, {tap, Tap}]),
			dict:store(From, Tap, Taps)
		catch
		    _ ->
			?WARN([bad_tap, {from, From}, {json, Json}]),
			Taps
		end;
	    _ ->
		Taps
    end,
    case th_telex:get(Telex, '_hop') of
	{ok, Hop} when is_integer(Hop) ->
	    if
		Hop < 4 ->
		    lists:foreach(
		      fun ({Address, Match}) ->
			      Match2 = th_telex:set(Match, '_hop', Hop+1),
			      ?INFO([forward, {address, Address}, {match, Match2}]),
			      th_udp:send(Address, Match2)
		      end,
		      matches(Telex, dict:to_list(Taps2))
		     );
		true ->
		    ok
	    end;
	_ ->
	    ok
    end,
    {noreply, State#state{taps=Taps2}};
handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    th_event:deafen(),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% --- internal functions ---

-spec matches(th_telex:telex(), list({address(), tap()})) -> list({address(), th_telex:telex()}).
matches(Telex, Taps) ->
    Matches =
	lists:map(
	  fun ({Address, Tap}) ->
		  {Address, match(Tap, Telex)}
	  end,
	  Taps
	 ),
    lists:filter(
      fun ({_Address, {struct, Signals}}) ->
	      Signals /= []
      end,
      Matches
     ).

-spec match(tap(), th_telex:telex()) -> th_telex:json_object().
match(#tap{subtaps=Subtaps}, Telex) ->
    {struct,
     lists:concat(
       lists:map(
	 fun (Subtap) ->
		 {struct, Signals} = match_subtap(Subtap, Telex),
		 Signals
	 end,
	 Subtaps
	)
      )
    }.

-spec match_subtap(subtap(), th_telex:telex()) -> th_telex:json_object().
match_subtap(#subtap{is=Is, has=Has}, Telex) ->
    try
	{struct, match_is(Is, Telex) ++ match_has(Has, Telex)}
    catch
	error:'th_tapper.bad_match' ->
	    {struct, []}
    end.

match_is(Is, Telex) ->
    lists:map(
      fun ({Key, Value}) ->
	      case th_telex:get(Telex, Key) of
		  {ok, Value} -> {Key, Value}; % !!! have to sort keys to compare structs
		  {error, _} -> erlang:error('th_tapper.bad_match')
	      end
      end,
      Is
     ).

match_has(Has, Telex) ->
    lists:map(
      fun (Key) ->
	      case th_telex:get(Telex, Key) of
		  {ok, Value} -> {Key, Value};
		  {error, _} -> erlang:error('th_tapper.bad_match')
	      end
      end,
      Has
     ).

% --- end ---
