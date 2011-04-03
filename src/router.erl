-module(router).

-include("types.hrl").
-include("conf.hrl").
-include("log.hrl").

-export([start/1, bootstrap/2]).

-behaviour(gen_event).
-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2, code_change/3]).

-define(K, ?REPLICATION).

-record(bootstrap, { % the state of the router when bootstrapping
	  timeout, % give up if no address received before this time
	  addresses % list of addresses contacted to find out our address
	 }).

-record(state, { % the state of the router in normal operation
	  self, % the bits of the routers own end
	  pinged, % set of addresses which have been pinged and not yet replied/timedout
	  table % the routing table, a bit_tree containing buckets of nodes
	 }).

% --- api ---	

start(#address{}=Self) ->
    ?INFO([starting]),
    State = #state{self=util:to_bits(Self), table=empty_table(Self)},
    ok = switch:add_handler(?MODULE, State).

bootstrap(Addresses, Timeout) ->
    ?INFO([bootstrapping]),
    State = #bootstrap{timeout=Timeout, addresses=Addresses},    
    ok = switch:add_handler(?MODULE, State),
    Telex = telex:end_signal(util:random_end()),
    lists:foreach(fun (Address) -> switch:send(Address, Telex) end, Addresses),
    ok.

% --- gen_event callbacks ---
	 		 
init(State) ->
    case State of
	#bootstrap{timeout=Timeout} ->
	    erlang:send_after(Timeout, self(), giveup);
	_ ->
	    ok
    end,
    {ok, State}.

handle_call(_Request, State) ->
    {ok, ok, State}.

handle_event({recv, From, Telex}, #bootstrap{addresses=Addresses}=Bootstrap) ->
    % bootstrapping, waiting to receive a message telling us our own address
    case {lists:member(From, Addresses), telex:get(Telex, '_to')} of
	{true, {ok, Binary}} ->
	    try util:to_end(util:binary_to_address(Binary)) of
		End ->
		    Self = util:to_bits(End),
		    Table = touched(From, Self, empty_table(Self)),
		    % cant call add_handler from inside the same manager :(
		    spawn_link(fun () -> dialer:dial(End, [From], ?ROUTER_DIAL_TIMEOUT) end), 
		    Table2 = refresh(Self, Table),
		    ?INFO([bootstrap, finished, {self, Self}, {from, From}]),
		    {ok, #state{self=Self, pinged=sets:new(), table=Table2}}
	    catch 
		_ ->
		    ?WARN([bootstrap, bad_self, {self, Binary}, {from, From}]),
		    {ok, Bootstrap}
	    end;
	_ ->
	    {ok, Bootstrap}
    end;
handle_event({recv, From, Telex}, #state{self=Self, pinged=Pinged, table=Table}=State) ->
    % this counts as a reply
    Pinged2 = sets:del_element(From, Pinged),
    % touched the sender
    % !!! eventually will check _line here
    ?INFO([touched, {node, From}]),
    Table2 = touched(From, Self, Table),
    % maybe seen some nodes
    Table3 =
	case telex:get(Telex, '.see') of
	    {ok, Binaries} ->
		try [util:binary_to_address(Bin) || Bin <- Binaries] of
		    Addresses ->
			?INFO([seen, {nodes, Addresses}, {from, From}]),
			lists:foldl(fun (Address, Table_acc) -> seen(Address, Self, Table_acc) end, Table2, Addresses)
		catch
		    _ ->
			?INFO([bad_seen, {nodes, Binaries}, {from, From}]),
			Table2
		end;
	    _ ->
		Table2
	end,
    % maybe send some nodes back
    case telex:get(Telex, '+end') of
	{ok, Hex} ->
	    try util:hex_to_end(Hex) of
		End ->
		    ?INFO([see, {'end', End}, {from, From}]),
		    see(From, End, Table3)
	    catch
		_ ->
		    ?WARN([bad_see, {'end', Hex}, {from, From}])
	    end;
	_ -> 
	    ok
    end,
    {ok, State#state{pinged=Pinged2, table=Table2}};
handle_event({dialed, End}, #state{self=Self, table=Table}=State) ->
    % record the dialing time
    ?INFO([dialed, {'end', End}]),
    Table2 = dialed(End, Self, Table),
    {ok, ok, State#state{table=Table2}};
handle_event(_, State) ->
    {ok, State}.

handle_info(giveup, #bootstrap{}=Bootstrap) ->
    % failed to bootstrap, die
    ?INFO([giveup, {state, Bootstrap}]),
    remove_handler;
handle_info(giveup, #state{}=State) ->
    % made it in time
    {ok, State};
handle_info(refresh, #state{self=Self, table=Table}=State) ->
    ?INFO([refreshing_table]),
    Table2 = refresh(Self, Table),
    {ok, State#state{table=Table2}};
handle_info({dialed, _, _}, State) ->
    % response from a bucket refresh, we don't care
    {ok, State};
handle_info({pinging, Address}, #state{pinged=Pinged}=State) ->
    % do this in a message to self to avoid some awkward control flow
    ?INFO([recording_ping, {address, Address}]),
    Pinged2 = sets:add_element(Address, Pinged),
    {ok, State#state{pinged=Pinged2}};
handle_info({timeout, Address}, #state{self=Self, pinged=Pinged, table=Table}=State) ->
    case lists:member(Address, Pinged) of
	true ->
	    % ping timedout
	    ?INFO([timeout, {address, Address}]),
	    Table2 = timedout(Address, Self, Table),
	    {ok, State#state{table=Table2}};
	false ->
	    % address already replied
	    {ok, State}
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% --- internal functions ---

empty_table(Self) ->
    % pre-split buckets containing Self, so that refreshes hit a useful number of buckets
    Split = fun (_, _, _, Bucket) -> bucket:split(Bucket) end,
    lists:foldl(
      fun (_, Tree) ->
	      bit_tree:update(Split, Self, Self, Tree)
      end,
      bit_tree:empty(0, bucket:empty()),
      lists:seq(1, ?END_BITS) % !!! probably not right first time
     ).

needs_refresh(Bucket, Now) ->
    case bucket:last_dialed(Bucket) of
	never -> 
	    true;
	Last -> 
	    (timer:now_diff(Now, Last) div 1000) < ?ROUTER_REFRESH_TIME
    end.

refresh(Self, Table) ->
    Now = now(),
    iter:foreach(
      fun ({Prefix, Bucket}) ->
	      case needs_refresh(Bucket, Now) of
		  true ->
		      ?INFO([refreshing_bucket, {prefix, Prefix}, {bucket, Bucket}]),
		      To = util:random_end(Prefix),
		      From = nearest(?K, To, Table),
		      % cant call add_handler from inside the same manager :(
		      spawn(fun () -> dialer:dial(To, From, ?ROUTER_DIAL_TIMEOUT) end);
		  false ->
		      ok
	      end
      end,
      bit_tree:iter(Self, Table)
     ),
    erlang:send_after(?ROUTER_REFRESH_TIME, self(), refresh),
    ok.

touched(Address, Self, Table) ->
    bit_tree:update(
      fun (Suffix, _Depth, Gap, Bucket) ->
	      May_split = (Gap < ?K), % !!! or (Depth < ?ROUTER_TABLE_EXPANSION)
	      bucket:touched(Address, Suffix, now(), Bucket, May_split)
      end,
      util:to_bits(Address),
      Self,
      Table
     ).

seen(Address, Self, Table) ->
    bit_tree:update(
      fun (Suffix, _Depth, _Gap, Bucket) ->
	      case bucket:seen(Address, Suffix, now(), Bucket) of
		  {node, Node, Update} ->
		      % check if this node is stale
		      ping(Node),
		      Update;
		  Update ->
		      Update
	      end
      end,
      util:to_bits(Address),
      Self,
      Table
     ).

timedout(Address, Self, Table) ->
    bit_tree:update(
      fun (_Suffix, _Depth, _Gap, Bucket) ->
	      case bucket:timedout(Address, Bucket) of
		  {node, Node, Update} ->
		      % try to touch this node
		      ping(Node),
		      Update;
		  Update ->
		      Update
	      end
      end,
      util:to_bits(Address),
      Self,
      Table
     ).
    
see(To, End, Table) ->
    Telex = telex:see_command(nearest(?K, End, Table)),
    switch:send(To, Telex).

ping(To) ->
    Telex = telex:end_signal(util:random_end()),
    % do this in a message to self to avoid some awkward control flow
    self() ! {pinging, To},
    switch:send(To, Telex).

dialed(Address, Self, Table) ->
    bit_tree:update(
      fun (_Suffix, _Depth, _Gap, Bucket) ->
	      bucket:dialed(Address, Bucket)
      end,
      util:to_bits(Address),
      Self,
      Table
     ).

nearest(N, End, Table) when N>=0 ->
    Bits = util:to_bits(End),
    iter:take(
      N, 
      iter:flatten(
	iter:map(
	  fun ({_Prefix, Bucket}) -> bucket:by_dist(End, Bucket) end, 
	  bit_tree:iter(Bits, Table)))).
