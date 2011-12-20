% a dialer locates the ?K nodes closest to a given end
% each dialer is a gen_server subscribed to th_event

-module(th_dialer).

-include("conf.hrl").
-include("types.hrl").
-include("log.hrl").

-export([dial/3, dial_sync/3]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

% corresponds to k and alpha in kademlia paper
-define(K, ?REPLICATION).
-define(A, ?DIALER_PARALLEL_REQUESTS).

-type peer() :: {Distance :: non_neg_integer(), address()}.

-record(conf, {
	  target :: 'end'(), % the end to dial
	  timeout :: timeout(), % the timeout for the entire dialing process
	  ref :: reference(), caller :: pid() % reply details
	 }).
-type conf() :: #conf{}.

-record(state, {
	  fresh :: pq_sets:pq(), % peers which have not yet been contacted
	  pinged :: set(),  % peers which have been contacted and have not replied
	  waiting :: pq_sets:pq(), % peers in pinged which were contacted less than ?DIALER_PING_TIMEOUT ago (used for throttling outgoing requests)
	  ponged :: pq_sets:pq(), % peers which have been contacted and have replied
	  seen :: set() % all peers which have been seen
	 }). % invariant: pq_sets:size(waiting) = ?A or pq_sets:empty(fresh)
-type state() :: #state{}.

% --- api ---

-spec dial('end'(), list(address()), timeout()) -> reference().
dial(To, From, Timeout) ->
    ?INFO([dialing, {to, To}, {from, From}, {timeout, Timeout}]),
    Ref = erlang:make_ref(),
    Conf = #conf{
      target = To,
      timeout = Timeout,
      ref = Ref,
      caller = self()
     },
    Peers = [{th_util:distance(Address, To), Address}
	     || Address <- From],
    State = #state{
      fresh=pq_sets:from_list(Peers),
      pinged=sets:new(),
      waiting=pq_sets:empty(),
      ponged=pq_sets:empty(),
      seen=sets:new()
     },
    {ok, _Pid} = gen_server:start(?MODULE, {Conf, State}, []),
    Ref.

-spec dial_sync('end'(), list(address()), timeout()) -> {ok, list(address())} | {error, timeout}.
dial_sync(To, From, Timeout) ->
    Ref = dial(To, From, Timeout),
    receive
	{dialed, Ref, Result} ->
	    {ok, Result}
    after Timeout ->
	    {error, timeout}
    end.

% --- gen_server callbacks ---

init({#conf{timeout=Timeout}=Conf, State}) ->
    th_event:listen(),
    erlang:send_after(Timeout, self(), giveup),
    State2 = ping_peers(Conf, State),
    {ok, {Conf, State2}}.

handle_call(_Call, _From, State) ->
    {noreply, State}.

handle_cast(_Cast, State) ->
    {ok, State}.

handle_info(giveup, {Conf, State}) ->
    ?INFO([giveup, {state, {Conf, State}}]),
    {stop, {shutdown, gaveup}, {Conf, State}};
handle_info({timeout, Peer}, {Conf, #state{waiting=Waiting}=State}) ->
    ?INFO([timeout, {peer, Peer}]),
    State2 = State#state{waiting=pq_sets:delete(Peer, Waiting)},
    continue(Conf, State2);
handle_info({event, {recv, Address, Telex}}, {#conf{target=Target}=Conf, #state{pinged=Pinged}=State}) ->
    case th_telex:get(Telex, '.see') of
	{error, not_found} ->
	    {noreply, {Conf, State}};
	{ok, Address_binaries} ->
	    Dist = th_util:distance(Address, Target),
	    Peer = {Dist, Address},
	    case sets:is_element(Peer, Pinged) of
		false ->
		    {noreply, {Conf, State}};
		true ->
		    try
			Addresses = lists:map(fun th_util:binary_to_address/1, Address_binaries),
			[{th_util:distance(Target, Addr), Addr} || Addr <- Addresses]
		    of
			Peers ->
			    ?INFO([pong, {from, Peer}, {peers, Peers}]),
			    State2 = ponged(Peer, Peers, State),
			    continue(Conf, State2)
		    catch
			_:Error ->
			    ?WARN([bad_see, {from, Address}, {telex, Telex}, {error, Error}, {trace, erlang:get_stacktrace()}]),
			    {noreply, {Conf, State}}
		    end
	    end
    end;
handle_info({gen_event_EXIT, Handler, Reason}, {Conf, State}) ->
    {stop, {shutdown, {deafened, Handler, Reason}}, {Conf, State}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    th_event:deafen(),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% --- internal functions ---

% contact peers from fresh until the waiting list is full
-spec ping_peers(conf(), state()) -> state().
ping_peers(#conf{target=Target}, #state{fresh=Fresh, waiting=Waiting, pinged=Pinged}=State) ->
    Num = ?A - pq_sets:size(Waiting),
    {Peers, Fresh2} = pq_sets:pop(Fresh, Num),
    Telex = th_telex:end_signal(th_util:to_end(Target)),
    lists:foreach(
      fun ({_Dist, Address}=Peer) ->
	      ?INFO([ping, {peer, Peer}]),
	      th_udp:send(Address, Telex),
	      erlang:send_after(?DIALER_PING_TIMEOUT, self(), {timeout, Peer})
      end,
      Peers),
    Waiting2 = pq_sets:push(Peers, Waiting),
    Pinged2 = sets:union(Pinged, sets:from_list(Peers)),
    State#state{fresh=Fresh2, waiting=Waiting2, pinged=Pinged2}.

% handle a .see command from a peer
-spec ponged(peer(), list(peer()), state()) -> state().
ponged(Peer, See, #state{fresh=Fresh, waiting=Waiting, pinged=Pinged, ponged=Ponged, seen=Seen}=State) ->
    Waiting2 = pq_sets:delete(Peer, Waiting),
    Pinged2 = sets:del_element(Peer, Pinged),
    Ponged2 = pq_sets:push_one(Peer, Ponged),
    New_peers = lists:filter(fun (See_peer) -> not(sets:is_element(See_peer, Seen)) end, See),
    Fresh2 = pq_sets:push(New_peers, Fresh),
    Seen2 = sets:union(Seen, sets:from_list(New_peers)),
    State#state{fresh=Fresh2, waiting=Waiting2, pinged=Pinged2, ponged=Ponged2, seen=Seen2}.

% is the dialing finished yet?
-spec finished(state()) -> boolean().
finished(#state{fresh=Fresh, waiting=Waiting, ponged=Ponged}) ->
    (pq_sets:is_empty(Fresh) and pq_sets:is_empty(Waiting)) % no way to continue
    or
    (case pq_sets:size(Ponged) >= ?K of
	 false ->
	     false; % dont yet have K peers
	 true ->
	     % finish if the K closest peers we know are closer than all the peers we haven't checked yet
	     {Closest_fresh, _} = pq_sets:peek(Fresh),
	     {Closest_waiting, _} = pq_sets:peek(Waiting),
	     {Peers, _} = pq_sets:pop(Ponged, ?K),
	     {Furthest_ponged, _} = lists:last(Peers),
	     (Furthest_ponged < Closest_fresh) and (Furthest_ponged < Closest_waiting)
     end).

% return results to the caller
-spec return(conf(), state()) -> ok.
return(#conf{ref=Ref, caller=Caller}, #state{ponged=Ponged}) ->
    {Peers, _} = pq_sets:pop(Ponged, ?K),
    Result = [Address || {_Dist, Address} <- Peers],
    ?INFO([returning, {result, Result}]),
    Caller ! {dialed, Ref, Result},
    ok.

% either continue to dial or return results
% meant for use at the end of a gen_eventcallback
-spec continue(conf(), state()) -> {stop, Reason::term(), {conf(), state()}} | {noreply, {conf(), state()}}.
continue(Conf, State) ->
    case finished(State) of
	true ->
	    th_event:notify({dialed, Conf#conf.target}),
	    return(Conf, State),
	    {stop, {shutdown, finished}, {Conf, State}};
	false ->
	    State2 = ping_peers(Conf, State),
	    {noreply, {Conf, State2}}
    end.

% --- end ---
