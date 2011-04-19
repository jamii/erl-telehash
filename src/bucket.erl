% implements the bucket part of kademlia k-buckets
% important points:
% * each bucket should contain at most ?K nodes
% * we should only ever report node addresses which we have personally confirmed
% * responsive nodes should never be removed from buckets
% * nodes should never be removed from buckets unless a suitable replacement exists
% these make the router resilient to flooding, poisoning and network failure

-module(bucket).

-include("types.hrl").
-include("conf.hrl").
-include("log.hrl").

-export([empty/0, split/1, touched/5, seen/4, timedout/2, dialed/2, by_dist/2, last_touched/1, last_dialed/1]).

-define(K, ?REPLICATION).

-record(node, {
	  address, % node #address{} record
	  'end', % node end
	  suffix, % the remaining bits of the nodes end left over from the bit_tree
	  status, % one of [live, stale, cache]
	  last_seen % for live/stale nodes, the time of the last received message. for cache nodes the time of the last .see reference to the node
	 }).

-record(bucket, {
	  last_dialed, % time an end in this bucket was last dialed, or never
	  nodes, % gb_tree mapping addresses to {Status, Last_seen}
	  % remaining fields are pq's of nodes sorted by their last_seen field
	  live, % nodes currently expected to be alive
	  stale, % nodes which have not replied recently
	  cache % potential nodes which we have not yet verified 
	 }). % invariant: pq_maps:size(live) + pq_maps:size(stale) <= ?K

% --- api ---

% this address has been verified as actually existing
touched(Address, Suffix, Time, Bucket, May_split) ->
    ?INFO([touching, {address, Address}, {bucket, Bucket}]),
    case get_node(Address, Bucket) of
	{ok, Node} ->
	    case Node#node.status of
		live -> 
		    % update last_seen time
		    ok(update_node(Node#node{last_seen=Time}, Bucket));
		stale ->
		    % update last_seen time and promote to live
		    ok(update_node(Node#node{last_seen=Time, status=live}, Bucket));
		cache ->
		    % potentially promote the node to live
		    Bucket2 = del_node(Node, Bucket),
		    new_node(Address, Suffix, Time, Bucket2, May_split)
	    end;
	none ->
	    % potentially add the node to live
	    new_node(Address, Suffix, Time, Bucket, May_split)
    end.

% this address has been reported to exist by another node
seen(Address, Time, Suffix, Bucket) ->
    ?INFO([seeing, {address, Address}, {bucket, Bucket}]),
    case get_node(Address, Bucket) of
	{ok, Node} ->
	    case Node#node.status of
		cache ->
		    % for cache nodes being in a .see is good enough
		    ok(update_node(Node#node{last_seen=Time}, Bucket));
		_ ->
		    % for live/stale nodes we require direct contact so ignore this
		    ok(Bucket)
	    end;
	none ->
	    % put node in cache, return a live node to ping
	    Node = #node{
	      address = Address,
	      'end' = util:to_end(Address),
	      suffix = Suffix,
	      status = cache,
	      last_seen = Time
	     },
	    Bucket2 = add_node(Node, Bucket),
	    case peek_live_old(Bucket) of
		none -> ok(Bucket2);
		{ok, Live_node} -> {node, Live_node, ok(Bucket2)}
	    end
    end.

% this address failed to reply in a timely manner
timedout(Address, Bucket) ->
    ?INFO([timing_out, {address, Address}, {bucket, Bucket}]),
    case get_node(Address, Bucket) of
	{ok, Node} ->
	    case Node#node.status of
		live ->
		    % mark as stale, return a cache node that might be a suitable replacement
		    Bucket2 = update_node(Node#node{status=stale}, Bucket),
		    pop_cache_new(Bucket2);
		_ -> 
		    % if cache or stale already we don't care 
		    ok(Bucket)
	    end;
	none ->
	    % wtf? we don't even know this node?
	    % one way this could happen: 
	    % send N1, sendN1, timedout N1, add N2 (pushing N1 out of stale), timedout N1 
	    ?WARN([unknown_node_timedout, {address, Address}, {bucket, Bucket}]),
	    ok(Bucket)
    end.

dialed(Time, Bucket) ->
    ok(Bucket#bucket{last_dialed=Time}).

by_dist(End, #bucket{live=Live, stale=Stale}) ->
    Nodes = pq_maps:to_list(Live) ++ pq_maps:to_list(Stale),
    % !!! maybe should prefer to return live nodes even if further away
    Nodes_by_dist = [{util:distance(End, Node#node.'end'), Node} || {_Key, Node} <- Nodes],
    [Node#node.address || {_Dist, Node} <- lists:sort(Nodes_by_dist)].

last_touched(#bucket{live=Live, stale=Stale}) ->
    % !!! ugly
    case {pq_maps:peek_hi(Live), pq_maps:peek_hi(Stale)} of
	{none, none} -> none;
	{none, {{Last_stale, _}, _}} -> Last_stale;
	{{{Last_live, _}, _}, none} -> Last_live;
	{{{Last_live, _}, _}, {{Last_stale, _}, _}} ->
	    % !!! no min in my erlang version :(
	    if Last_live > Last_stale -> Last_live; true -> Last_stale end
    end.

last_dialed(Bucket) ->
    Bucket#bucket.last_dialed.

% --- functions maintaining the bucket invariants ---

% response format for bit_tree
ok(Bucket) ->
    {Lives, Stales, _} = sizes(Bucket),
    {ok, Lives + Stales, Bucket}.

% response format for bit_tree
split(Bucket) ->
    Nodes = to_list(Bucket),
    NodesF = [Node#node{suffix=Suffix2} || #node{suffix=[false|Suffix2]}=Node <- Nodes],
    NodesT = [Node#node{suffix=Suffix2} || #node{suffix=[true|Suffix2]}=Node <- Nodes],
    {split, ok(from_list(NodesF)), ok(from_list(NodesT))}.

% assumes Address is not already in Bucket, otherwise crashes
new_node(Address, Suffix, Time, Bucket, May_split) ->
    Node = #node{
      address = Address,
      'end' = util:to_end(Address),
      suffix = Suffix,
      status = undefined,
      last_seen = Time
     },
    {Lives, Stales, _} = sizes(Bucket),
    if
	Lives + Stales < ?K ->
	    % space left in live
	    ?INFO([adding, {node, Node}, {bucket, Bucket}]),
	    ok(add_node(Node#node{status=live}, Bucket));
	(Lives < ?K) and (Stales > 0) ->
	    % space left in live if we push something out of stale
	    ?INFO([adding, {node, Node}, {bucket, Bucket}]),
	    Bucket2 = drop_stale(Bucket),
	    ok(add_node(Node#node{status=live}, Bucket2));
	May_split and (Suffix /= []) ->
	    % allowed to split the bucket to make space
	    ?INFO([splitting, {node, Node}, {bucket, Bucket}]),
	    {split, {ok, BucketF}, {ok, BucketT}} = split(Bucket),
	    [Bit | Suffix2] = Suffix,
	    case Bit of
		false ->
		    BucketF2 = new_node(Address, Suffix2, Time, BucketF, May_split),
		    {split, BucketF2, BucketT};
		true ->
		    BucketT2 = new_node(Address, Suffix2, Time, BucketT, May_split),
		    {split, BucketF, BucketT2}
	    end;
	true ->
	    % not allowed to split, will have to go in the cache
	    ?INFO([caching, {node, Node}, {bucket, Bucket}]),
	    ok(add_node(Node#node{status=cache}, bucket))
    end.

% --- internal functions handling the (nodes -> live/stale/cache) mapping ---

empty() ->
    #bucket{
       last_dialed = never,
       nodes = gb_trees:empty(),
       live = pq_maps:empty(),
       stale = pq_maps:empty(),
       cache = pq_maps:empty()
      }.

% assumes Node does not already exist in Bucket, crashes otherwise
add_node(#node{address=Address, status=Status, last_seen=Last_seen}=Node, 
	 #bucket{nodes=Nodes, live=Live, stale=Stale, cache=Cache}=Bucket) ->
    Nodes2 = gb_trees:insert(Address, {Status, Last_seen}, Nodes),
    case Status of
	live ->
	    Live2 = pq_maps:push_one({Last_seen, Address}, Node, Live),
	    Bucket#bucket{nodes=Nodes2, live=Live2};
	stale ->
	    Stale2 = pq_maps:push_one({Last_seen, Address}, Node, Stale),
	    Bucket#bucket{nodes=Nodes2, stale=Stale2};
	cache ->
	    Cache2 = pq_maps:push_one({Last_seen, Address}, Node, Cache),
	    Bucket#bucket{nodes=Nodes2, cache=Cache2}
    end.
    
% assumes Node already exists in Bucket, crashes otherwise
del_node(#node{address=Address}, Bucket) ->
    del_node(Address, Bucket);
del_node(Address,
	 #bucket{nodes=Nodes, live=Live, stale=Stale, cache=Cache}=Bucket) ->
    {Status, Last_seen} = gb_trees:get(Address, Nodes),
    Nodes2 = gb_trees:delete(Address, Nodes),
    case Status of
	live ->
	    Live2 = pq_maps:delete({Last_seen, Address}, Live),
	    Bucket#bucket{nodes=Nodes2, live=Live2};
	stale ->
	    Stale2 = pq_maps:delete({Last_seen, Address}, Stale),
	    Bucket#bucket{nodes=Nodes2, stale=Stale2};
	cache ->
	    Cache2 = pq_maps:delete({Last_seen, Address}, Cache),
	    Bucket#bucket{nodes=Nodes2, cache=Cache2}
    end.

% assumes Node already exists in Bucket, crashes otherwise
update_node(Node, Bucket) ->
    add_node(Node, del_node(Node, Bucket)).

get_node(Address, 
	 #bucket{nodes=Nodes, live=Live, stale=Stale, cache=Cache}) ->
    case gb_trees:lookup(Address, Nodes) of
	{value, {Status, Last_seen}} ->
	    case Status of 
		live ->
		    {ok, pq_maps:get({Last_seen, Address}, Live)};
		stale ->
		    {ok, pq_maps:get({Last_seen, Address}, Stale)};
		cache ->
		    {ok, pq_maps:get({Last_seen, Address}, Cache)}
	    end;
	none -> 
	    none
    end.

to_list(#bucket{live=Live, stale=Stale, cache=Cache}) ->
    pq_maps:values(Live) ++ pq_maps:values(Stale) ++ pq_maps:values(Cache).

from_list(Nodes) ->
    lists:foldl(fun add_node/2, empty(), Nodes).

sizes(#bucket{live=Live, stale=Stale, cache=Cache}) ->
    {pq_maps:size(Live), pq_maps:size(Stale), pq_maps:size(Cache)}.

% drop the oldest stale node, crashes if none exist
drop_stale(#bucket{nodes=Nodes, stale=Stale}=Bucket) ->
    {_Key, #node{address=Address}, Stale2} = pq_maps:pop_hi(Stale),
    Nodes2 = gb_trees:delete(Address, Nodes),
    Bucket#bucket{nodes=Nodes2, stale=Stale2}.

% return most recently seen cache node, if any exist
pop_cache_new(#bucket{nodes=Nodes, cache=Cache}=Bucket) ->
    case pq_maps:pop_hi(Cache) of
	{_Key, #node{address=Address}=Node, Cache2} ->
	    Nodes2 = gb_trees:delete(Address, Nodes),
	    {node, Node, ok(Bucket#bucket{nodes=Nodes2, cache=Cache2})};
	false ->
	    ok(Bucket)
    end.

% return the oldest live node
peek_live_old(#bucket{live=Live}) ->
    case pq_maps:peek_lo(Live) of
	none -> none;
	{_, Node} -> {ok, Node}
    end.
	     	    
% --- end ---
