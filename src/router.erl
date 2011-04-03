-module(router).

-include("types.hrl").
-include("conf.hrl").

-export([start/1, bootstrap/2]).

-behaviour(gen_event).
-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2, code_change/3]).

-define(K, ?REPLICATION).

-record(bootstrap, { % the state of the router when bootstrapping
	  timeout, % give up if no address received before this time
	  addresses % list of addresses contacted to find out our address
	 }).

-record(state, { % the state of the router in normal operation
	  self, % the routers own #address{}
	  pinged, % set of addresses which have been pinged and not yet replied/timedout
	  table % the routing table, a bit_tree containing buckets of nodes
	 }).

% --- api ---	

start(#address{}=Self) ->
    log:info([?MODULE, starting]),
    State = #state{self=Self, table=empty_table(Self)},
    ok = switch:add_handler(?MODULE, State).

bootstrap(Addresses, Timeout) ->
    log:info([?MODULE, bootstrapping]),
    State = #bootstrap{timeout=Timeout, addresses=Addresses},    
    ok = switch:add_handler(?MODULE, State),
    {'end', End} = util:random_end(),
    Telex = {struct, [{'+end', End}]},
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
	    try
		Self = util:to_end(util:binary_to_address(Binary)),
		log:info([?MODULE, bootstrap, finished, {self, Self}, {from, From}]),
		Table = refresh(Self, empty_table(Self)),
		{ok, #state{self=Self, table=Table}}
	    catch 
		_ ->
		    log:warn([?MODULE, bootstrap, bad_self, {self, Binary}, {from, From}]),
		    {ok, Bootstrap}
	    end;
	_ ->
	    ok
    end;
handle_event({recv, From, Telex}, #state{self=Self, pinged=Pinged, table=Table}=State) ->
    % this counts as a reply
    Pinged2 = sets:del_element(From, Pinged),
    % touched the sender
    % !!! eventually will check _line here
    log:info([?MODULE, touched, {from, From}]),
    Table2 = touched(From, Self, Table),
    % maybe seen some nodes
    Table3 =
	case telex:get(Telex, '.see') of
	    {ok, Binaries} ->
		try [util:binary_to_address(Bin) || Bin <- Binaries] of
		    Addresses ->
			log:info([?MODULE, seen, {nodes, Addresses}, {from, From}]),
			lists:foldl(fun (Address, Table_acc) -> seen(Address, Self, Table_acc) end, Table2, Addresses)
		catch
		    _ ->
			log:warn([?MODULE, bad_seen, {nodes, Binaries}, {from, From}]),
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
		    log:info([?MODULE, see, {'end', End}, {from, From}]),
		    see(From, End, Table3)
	    catch
		_ ->
		    log:warn([?MODULE, bad_see, {'end', Hex}, {from, From}])
	    end;
	_ -> 
	    ok
    end,
    {ok, State#state{pinged=Pinged2, table=Table2}};
handle_event({dialed, End}, #state{self=Self, table=Table}=State) ->
    % record the dialing time
    log:info([?MODULE, {dialed, End}]),
    Table2 = dialed(End, Self, Table),
    {ok, ok, State#state{table=Table2}}.

handle_info(giveup, #bootstrap{}=State) ->
    % failed to bootstrap, die
    log:info([?MODULE, giveup, State]),
    remove_handler;
handle_info(giveup, #state{}=State) ->
    % made it in time
    {ok, State};
handle_info(refresh, #state{self=Self, table=Table}=State) ->
    Table2 = refresh(Self, Table),
    {ok, State#state{table=Table2}};
handle_info({dialed, _, _}, State) ->
    % response from a bucket refresh, we don't care
    {ok, State};
handle_info({pinging, Address}, #state{pinged=Pinged}=State) ->
    % do this in a message to self to avoid some awkward control flow
    Pinged2 = sets:add_element(Address, Pinged),
    {ok, State#state{pinged=Pinged2}};
handle_info({timeout, Address}, #state{self=Self, pinged=Pinged, table=Table}=State) ->
    case lists:member(Address, Pinged) of
	true ->
	    % ping timedout
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
    Bits = util:to_bits(Self),
    Split = fun (_, _, _, Bucket) -> bucket:split(Bucket) end,
    lists:foldl(
      fun (_, Tree) ->
	      bit_tree:update(Split, Bits, Self, Tree)
      end,
      bit_tree:empty(0, bucket:empty()),
      lists:seq(0, ?END_BITS+2) % !!! probably not right first time
     ).

refresh(Self, Table) ->
    iter:foreach(
      fun ({Prefix, _Bucket}) ->
	      To = util:random_end(Prefix),
	      From = nearest(?K, To, Table),
	      dialer:dial(To, From, ?ROUTER_DIAL_TIMEOUT)
      end,
      bit_tree:iter(util:to_bits(Self), Table)
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
    Addresses = [util:address_to_binary(Address) || Address <- nearest(?K, End, Table)],
    Telex = {struct, [{'.see', Addresses}]},
    switch:send(To, Telex).

ping(To) ->
    {'end', End} = util:random_end(),
    Telex = {struct, [{'+end', End}]},
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
    iter:take(N, 
	      iter:map(
		fun (Bucket) -> bucket:by_dist(End, Bucket) end, 
		bit_tree:iter(End, Table))).
		      
% !!! logging
