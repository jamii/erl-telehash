% router manages the interactions between kademlia routing tables and the outside world
% the logic for the tables themselves is in bit_tree and bucket

-module(th_router).

-include("types.hrl").
-include("conf.hrl").
-include("log.hrl").

-export([start/1, bootstrap/0, bootstrap/2, nearest/3]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(K, ?REPLICATION).

-type table() :: th_bit_tree:bit_tree().

-record(bootstrap, { % the state of the router when bootstrapping
	  timeout :: timeout(), % give up if no address received before this time
	  addresses :: list(address()) % list of addresses contacted to find out our address
	 }).
-type bootstrap() :: #bootstrap{}.

-record(state, { % the state of the router in normal operation
	  self :: bits(), % the bits of the routers own end
	  pinged :: set(), % set of addresses which have been pinged and not yet replied/timedout
	  table :: table() % the routing table, a th_bit_tree containing th_buckets of nodes
	 }).
-type state() :: #state{}.

% --- api ---	

-spec start(address()) -> {ok, pid()}.
start(#address{}=Address) ->
    ?INFO([starting]),
    Self = th_util:to_bits(Address),
    State = #state{self=Self, pinged=sets:new(), table=empty_table(Self)},
    {ok, _Pid} = gen_server:start_link(?MODULE, State, []).

-spec bootstrap() -> {ok, pid()}.
bootstrap() ->
    bootstrap([?TELEHASH_ORG], 10000).

-spec bootstrap(list(address()), timeout()) -> {ok, pid()}.
bootstrap(Addresses, Timeout) ->
    ?INFO([bootstrapping]),
    State = #bootstrap{timeout=Timeout, addresses=Addresses},    
    {ok, _Pid} = gen_server:start_link(?MODULE, State, []).

-spec nearest(pos_integer(), 'end'(), timeout()) -> list(address()).
nearest(N, End, Timeout) ->
    {ok, Nearest} = gen_server:call(?MODULE, {nearest, N, End}, Timeout),
    Nearest.

% --- gen_server callbacks ---
	 		 
init(State) ->
    th_event:listen(),
    case State of
	#bootstrap{timeout=Timeout, addresses=Addresses} ->
	    Telex = th_telex:end_signal(th_util:random_end()),
	    lists:foreach(fun (Address) -> th_udp:send(Address, Telex) end, Addresses),
	    erlang:send_after(Timeout, self(), giveup);
	#state{} ->
	    ok
    end,
    {ok, State}.

handle_call({nearest, N, End}, _From, #state{table=Table}=State) ->
    {reply, get_nearest(N, End, Table), State};
handle_call(_Call, _From, State) ->
    {noreply, State}.

handle_cast(_Cast, State) ->
    {ok, State}.

handle_info({event, {recv, From, Telex}}, #bootstrap{addresses=Addresses}=Bootstrap) ->
    % bootstrapping, waiting to receive a message telling us our own address
    case {lists:member(From, Addresses), th_telex:get(Telex, '_to')} of
	{true, {ok, Binary}} ->
	    try th_util:to_end(th_util:binary_to_address(Binary)) of
		End ->
		    Self = th_util:to_bits(End),
		    Table = touched(From, Self, empty_table(Self)),
		    th_dialer:dial(End, [From], ?ROUTER_DIAL_TIMEOUT),
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
handle_info({event, {recv, From, Telex}}, #state{self=Self, pinged=Pinged, table=Table}=State) ->
    % count this as a reply
    Pinged2 = sets:del_element(From, Pinged),
    % touched the sender
    ?INFO([touched, {node, From}]),
    Table2 = touched(From, Self, Table),
    % maybe seen some nodes
    Table3 =
	case th_telex:get(Telex, '.see') of
	    {ok, Binaries} ->
		try lists:map(fun th_util:binary_to_address/1, Binaries) of
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
    case th_telex:get(Telex, '+end') of
	{ok, Hex} ->
	    try th_util:hex_to_end(Hex) of
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
handle_info({event, {dialed, End}}, #state{self=Self, table=Table}=State) ->
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
    case sets:is_element(Address, Pinged) of
	true ->
	    % ping timedout
	    ?INFO([timeout, {address, Address}]),
	    Table2 = timedout(Address, Self, Table),
	    {noreply, State#state{table=Table2}};
	false ->
	    % address already replied
	    {noreply, State}
    end;
handle_info({gen_event_EXIT, Handler, Reason}, State) ->
    {stop, {shutdown, {deafened, Handler, Reason}}, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    th_event:deafen(),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% --- internal functions ---

-spec empty_table(bits()) -> table().
empty_table(Self) ->
    % pre-split buckets containing Self, so that refreshes hit a useful number of buckets
    Split = fun (_, _, _, Bucket) -> th_bucket:split(Bucket) end,
    lists:foldl(
      fun (_, Tree) ->
	      th_bit_tree:update(Split, Self, Self, Tree)
      end,
      th_bit_tree:new(0, th_bucket:empty()),
      lists:seq(1, ?END_BITS)
     ).

-spec needs_refresh(th_bucket:bucket(), now()) -> boolean().
needs_refresh(Bucket, Now) ->
    case th_bucket:last_dialed(Bucket) of
	never -> 
	    true;
	Last -> 
	    (timer:now_diff(Now, Last) div 1000) > ?ROUTER_REFRESH_TIME
    end.

-spec refresh(bits(), table()) -> ok.
refresh(Self, Table) ->
    Now = now(),
    th_iter:foreach(
         fun ({Prefix, Bucket}) ->
	         case needs_refresh(Bucket, Now) of
		     true ->
		         ?INFO([refreshing_bucket, {prefix, Prefix}, {bucket, Bucket}]),
		         To = th_util:random_end(Prefix),
		         From = get_nearest(?K, To, Table),
		         th_dialer:dial(To, From, ?ROUTER_DIAL_TIMEOUT);
		     false ->
		         ok
	         end
         end,
         th_bit_tree:iter(Self, Table)
        ),
    erlang:send_after(?ROUTER_REFRESH_TIME, self(), refresh),
    ok.

-spec touched(address(), bits(), table()) -> table().
touched(Address, Self, Table) ->
    th_bit_tree:update(
         fun (Suffix, Depth, Gap, Bucket) ->
	         May_split = 
		     % aim to ensure that the subtree containing the nearest k existing nodes is all live, split if there is no room
		     (Gap < ?K) 
		     or 
		     % expand routing table for accelerated lookups
		     (Depth rem ?ROUTER_TABLE_EXPANSION /= 0), 
	         th_bucket:touched(Address, Suffix, now(), Bucket, May_split)
         end,
         th_util:to_bits(Address),
         Self,
         Table
        ).

-spec seen(address(), bits(), table()) -> table().
seen(Address, Self, Table) ->
    th_bit_tree:update(
         fun (Suffix, _Depth, _Gap, Bucket) ->
	         case th_bucket:seen(Address, Suffix, now(), Bucket) of
		     {ping, Address2, Update} ->
		         % check if this node is stale
		         ping(Address2),
		         Update;
		     Update ->
		         Update
	         end
         end,
         th_util:to_bits(Address),
         Self,
         Table
        ).

-spec timedout(address(), bits(), table()) -> table().
timedout(Address, Self, Table) ->
    th_bit_tree:update(
         fun (_Suffix, _Depth, _Gap, Bucket) ->
	         case th_bucket:timedout(Address, Bucket) of
		     {ping, Address2, Update} ->
		         % try to touch this node, might be suitable replacement
		         ping(Address2),
		         Update;
		     Update ->
		         Update
	         end
         end,
         th_util:to_bits(Address),
         Self,
         Table
        ).
    
-spec see(address(), 'end'(), table()) -> ok.
see(To, End, Table) ->
    Telex = th_telex:see_command(get_nearest(?K, End, Table)),
    th_udp:send(To, Telex).

-spec ping(address()) -> ok.
ping(To) ->
    Telex = th_telex:end_signal(th_util:random_end()),
    % do this in a message to self to avoid some awkward control flow
    self() ! {pinging, To},
    th_udp:send(To, Telex),
    erlang:send_after(?ROUTER_PING_TIMEOUT, self(), {timeout, To}),
    ok.

-spec dialed(address(), bits(), table()) -> table().
dialed(Address, Self, Table) ->
    th_bit_tree:update(
         fun (_Suffix, _Depth, _Gap, Bucket) ->
	         th_bucket:dialed(now(), Bucket)
         end,
         th_util:to_bits(Address),
         Self,
         Table
        ).

-spec get_nearest(pos_integer(), 'end'(), table()) -> list(address()).
get_nearest(N, End, Table) when N>=0 ->
    Bits = th_util:to_bits(End),
    th_iter:take(
         N,
         th_iter:flatten(
	      th_iter:map(
	           fun ({_Prefix, Bucket}) -> th_bucket:by_dist(End, Bucket) end,
	           th_bit_tree:iter(Bits, Table)))).
