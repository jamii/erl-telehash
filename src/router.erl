% router manages the interactions between kademlia routing tables and the outside world
% the logic for the tables themselves is in bit_tree and bucket

-module(router).

-include("types.hrl").
-include("conf.hrl").
-include("log.hrl").

-export([start/1, bootstrap/2]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

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
    State = #state{self=util:to_bits(Self), pinged=sets:new(), table=empty_table(Self)},
    {ok, _Pid} = gen_server:start_link(?MODULE, State, []).

bootstrap(Addresses, Timeout) ->
    ?INFO([bootstrapping]),
    State = #bootstrap{timeout=Timeout, addresses=Addresses},    
    {ok, _Pid} = gen_server:start_link(?MODULE, State, []).

% --- gen_server callbacks ---
	 		 
init(State) ->
    switch:listen(),
    case State of
	#bootstrap{timeout=Timeout, addresses=Addresses} ->
	    Telex = telex:end_signal(util:random_end()),
	    lists:foreach(fun (Address) -> switch:send(Address, Telex) end, Addresses),
	    erlang:send_after(Timeout, self(), giveup);
	#state{} ->
	    ok
    end,
    {ok, State}.

handle_call(_Call, _From, State) ->
    {noreply, State}.

handle_cast(_Cast, State) ->
    {ok, State}.

handle_info({switch, {recv, From, Telex}}, #bootstrap{addresses=Addresses}=Bootstrap) ->
    % bootstrapping, waiting to receive a message telling us our own address
    case {lists:member(From, Addresses), telex:get(Telex, '_to')} of
	{true, {ok, Binary}} ->
	    try util:to_end(util:binary_to_address(Binary)) of
		End ->
		    Self = util:to_bits(End),
		    Table = touched(From, Self, empty_table(Self)),
		    dialer:dial(End, [From], ?ROUTER_DIAL_TIMEOUT), 
		    refresh(Self, Table),
		    ?INFO([bootstrap, finished, {self, Binary}, {from, From}]),
		    {noreply, #state{self=Self, pinged=sets:new(), table=Table}}
	    catch 
		_ ->
		    ?WARN([bootstrap, bad_self, {self, Binary}, {from, From}]),
		    {noreply, Bootstrap}
	    end;
	_ ->
	    {noreply, Bootstrap}
    end;
handle_info({switch, {recv, From, Telex}}, #state{self=Self, pinged=Pinged, table=Table}=State) ->
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
    {noreply, State#state{pinged=Pinged2, table=Table2}};
handle_info({switch, {dialed, End}}, #state{self=Self, table=Table}=State) ->
    % record the dialing time
    ?INFO([dialed, {'end', End}]),
    Table2 = dialed(End, Self, Table),
    {noreply, State#state{table=Table2}};
handle_info(giveup, #bootstrap{}=Bootstrap) ->
    % failed to bootstrap, die
    ?INFO([giveup, {state, Bootstrap}]),
    {stop, {shutdown, gaveup}, Bootstrap};
handle_info(giveup, #state{}=State) ->
    % made it in time
    {noreply, State};
handle_info(refresh, #state{self=Self, table=Table}=State) ->
    ?INFO([refreshing_table]),
    refresh(Self, Table),
    {noreply, State};
handle_info({dialed, _, _}, State) ->
    % response from a bucket refresh, we don't care
    {noreply, State};
handle_info({pinging, Address}, #state{pinged=Pinged}=State) ->
    % do this in a message to self to avoid some awkward control flow
    ?INFO([recording_ping, {address, Address}]),
    Pinged2 = sets:add_element(Address, Pinged),
    {noreply, State#state{pinged=Pinged2}};
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
    end;
handle_info({gen_event_EXIT, Handler, Reason}, State) ->
    {stop, {shutdown, {deafened, Handler, Reason}}, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    switch:deafen(),
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
		      dialer:dial(To, From, ?ROUTER_DIAL_TIMEOUT);
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
	      bucket:dialed(now(), Bucket)
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
