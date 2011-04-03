% a dialer locates the ?K nodes closest to a given end
% each dialer is a gen_event handler attached to the switch manager

-module(dialer).

-include("conf.hrl").
-include("types.hrl").
-include("log.hrl").

-export([dial/3, dial_sync/3]).

-behaviour(gen_event).
-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2, code_change/3]).

% corresponds to k and alpha in kademlia paper
-define(K, ?REPLICATION).
-define(A, ?DIALER_BREADTH).

-record(conf, {
	  target, % the end to dial
	  timeout, % the timeout for the entire dialing process
	  ref, caller % reply details
	 }).
-record(state, {
	  fresh, % nodes which have not yet been contacted
	  pinged, % nodes which have been contacted and have not replied
	  waiting, % nodes in pinged which were contacted less than ?DIALER_PING_TIMEOUT ago
	  ponged, % nodes which have been contacted and have replied
	  seen % all nodes which have been seen 
	 }). % invariant: pq_sets:size(waiting) = ?A or pq_sets:empty(fresh)

% --- api ---

dial(To, From, Timeout) ->
    ?INFO([dialing, {to, To}, {from, From}, {timeout, Timeout}]),
    Ref = erlang:make_ref(),
    Target = util:to_end(To),
    Conf = #conf{
      target = Target,
      timeout = Timeout,
      ref = Ref,
      caller = self()
     },
    Nodes = [{util:distance(Address, Target), Address}
	     || Address <- From],
    State = #state{
      fresh=pq_sets:from_list(Nodes), 
      pinged=sets:new(),
      waiting=pq_sets:empty(),
      ponged=pq_sets:empty(), 
      seen=sets:new()
     },
    ok = switch:add_handler(?MODULE, {Conf, State}),
    Ref.

dial_sync(Target, Addresses, Timeout) ->
    Ref = dial(Target, Addresses, Timeout),
    receive
	{dialed, Ref, Result} ->
	    {ok, Result}
    after Timeout ->
	    {error, timeout}
    end.

% --- gen_event callbacks ---

init({#conf{timeout=Timeout}=Conf, State}) ->
    erlang:send_after(Timeout, self(), giveup),
    State2 = ping_nodes(Conf, State),
    {ok, {Conf, State2}}.

handle_call(_Request, State) ->
    {ok, ok, State}.

handle_event({recv, Address, Telex}, {#conf{target=Target}=Conf, #state{pinged=Pinged}=State}) ->
    case telex:get(Telex, '.see') of
	{error, not_found} -> 
	    {ok, {Conf, State}};
	{ok, Address_binaries} ->
	    Dist = util:distance(Address, Target),
	    Node = {Dist, Address},
	    case sets:is_element(Node, Pinged) of % !!! command ids would make a better check
		false ->
		    {ok, {Conf, State}};
		true ->
		    try 
			Addresses = lists:map(fun util:binary_to_address/1, Address_binaries),
			[{util:distance(Target, Addr), Addr} || Addr <- Addresses]
		    of
			Nodes ->
			    ?INFO([pong, {from, Node}, {nodes, Nodes}]),
			    State2 = ponged(Node, Nodes, State),
			    continue(Conf, State2)
		    catch 
			_:Error ->
			    ?WARN([bad_see, {from, Address}, {telex, Telex}, {error, Error}, {trace, erlang:get_stacktrace()}]),
			    {ok, {Conf, State}}
		    end
	    end
    end;
handle_event(_, State) ->
    {ok, State}.

handle_info(giveup, {Conf, State}) ->
    ?INFO([giveup, {state, {Conf, State}}]),
    remove_handler;
handle_info({timeout, Node}, {Conf, #state{waiting=Waiting}=State}) ->
    ?INFO([timeout, {node, Node}]),
    State2 = State#state{waiting=pq_sets:delete(Node, Waiting)},
    continue(Conf, State2);
handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% --- internal functions ---

% is the dialing finished yet?
finished(#state{fresh=Fresh, waiting=Waiting, ponged=Ponged}) ->
    (pq_sets:is_empty(Fresh) and pq_sets:is_empty(Waiting)) % no way to continue
    or
    (case pq_sets:size(Ponged) >= ?K of
	 false ->
	     false; % dont yet have K nodes
	 true ->
	     % finish if the K closest nodes we know are closer than all the nodes we haven't checked yet
	     {Dist_fresh, _} = pq_sets:peek(Fresh),
	     {Dist_waiting, _} = pq_sets:peek(Waiting),
	     {Nodes, _} = pq_sets:pop(Ponged, ?K),
	     {Dist_ponged, _} = lists:last(Nodes),
	     (Dist_ponged < Dist_fresh) and (Dist_ponged < Dist_waiting)
     end).

% contact nodes from fresh until the waiting list is full
ping_nodes(#conf{target=Target}, #state{fresh=Fresh, waiting=Waiting, pinged=Pinged}=State) ->
    Num = ?A - pq_sets:size(Waiting),
    {Nodes, Fresh2} = pq_sets:pop(Fresh, Num),
    Telex = {struct, [{'+end', util:end_to_hex(Target)}]},
    lists:foreach(
      fun ({_Dist, Address}=Node) -> 
	      ?INFO([ping, {node, Node}]),
	      switch:send(Address, Telex),
	      erlang:send_after(?DIALER_PING_TIMEOUT, self(), {timeout, Node})
      end, 
      Nodes),
    Waiting2 = pq_sets:push(Nodes, Waiting),
    Pinged2 = sets:union(Pinged, sets:from_list(Nodes)),
    State#state{fresh=Fresh2, waiting=Waiting2, pinged=Pinged2}.

% handle a reply from a node
ponged(Node, See, #state{fresh=Fresh, waiting=Waiting, pinged=Pinged, ponged=Ponged, seen=Seen}=State) ->
    Waiting2 = pq_sets:delete(Node, Waiting),
    Pinged2 = sets:del_element(Node, Pinged),
    Ponged2 = pq_sets:push_one(Node, Ponged),
    New_nodes = lists:filter(fun (See_node) -> not(sets:is_element(See_node, Seen)) end, See),
    Fresh2 = pq_sets:push(New_nodes, Fresh),
    Seen2 = sets:union(Seen, sets:from_list(See)),
    State#state{fresh=Fresh2, waiting=Waiting2, pinged=Pinged2, ponged=Ponged2, seen=Seen2}.
				
% return results to the caller	   
return(#conf{ref=Ref, caller=Caller}, #state{ponged=Ponged}) ->
    {Nodes, _} = pq_sets:pop(Ponged, ?K),
    Result = [Address || {_Dist, Address} <- Nodes],
    ?INFO([returning, {result, Result}]),
    Caller ! {dialed, Ref, Result}.

% either continue to dial or return results
% meant for use at the end of a gen_event callback
continue(Conf, State) ->
    case finished(State) of
	true ->
	    switch:notify({dialed, Conf#conf.target}),
	    return(Conf, State),
	    remove_handler;
	false ->
	    State2 = ping_nodes(Conf, State),
	    {ok, {Conf, State2}}
    end.

% --- end ---
  
