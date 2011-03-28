-module(bucket).

-include("types.hrl").
-include("conf.hrl").

-export([empty/0, touched/5, seen/4, timedout/2]).

-define(K, ?DIAL_DEPTH).

-record(node, {
	  address, % node #address{} record
	  'end', % node end
	  suffix, % the remaining bits of the nodes end left over from the bit_tree
	  status, % one of [live, stale, cache]
	  last_seen % for live/stale nodes, the time of the last received message. for cache nodes the time of the last .see reference to the node
	 }).

-record(bucket, {
	  nodes, % gb_tree mapping addresses to {Status, Last_seen}
	  % remaining fields are pq's of nodes sorted by their last_seen field
	  live, % nodes currently expected to be alive
	  stale, % nodes which have not replied recently
	  cache % potential nodes which we have not yet verified 
	 }). % invariant: pq_maps:size(live) + pq_maps:size(stale) <= ?K

% --- internal functions handling the nodes -> live/stale/cache mapping ---

empty() ->
    #bucket{
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
drop_stale(#bucket{stale=Stale}=Bucket) ->
    {_Key, _Node, Stale2} = pq_maps:pop_one_rev(Stale),
    Bucket#bucket{stale=Stale2}.

% return most recently seen cache node, if any exist
pop_cache(#bucket{cache=Cache}=Bucket) ->
    case pq_maps:pop_hi(Cache) of
	{_Key, Node, Cache2} ->
	    {ok, Node, Bucket#bucket{cache=Cache2}};
	false ->
	    {ok, Bucket}
    end.
    		
% --- functions maintaining the bucket invariants ---

split(Bucket) ->
    Nodes = to_list(Bucket),
    NodesF = [Node#node{suffix=Suffix2} || #node{suffix=[false|Suffix2]}=Node <- Nodes],
    NodesT = [Node#node{suffix=Suffix2} || #node{suffix=[true|Suffix2]}=Node <- Nodes],
    {from_list(NodesF), from_list(NodesT)}.

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
	    log:info([?MODULE, adding, Node, Bucket]),
	    {ok, add_node(Node#node{status=live}, Bucket)};
	(Lives < ?K) and (Stales > 0) ->
	    % space left in live if we push something out of stale
	    log:info([?MODULE, adding, Node, Bucket]),
	    Bucket2 = drop_stale(Bucket),  
	    {ok, add_node(Node#node{status=live}, Bucket2)};
	May_split and (Suffix /= []) ->
	    % allowed to split the bucket to make space
	    log:info([?MODULE, splitting, Node, Bucket]),
	    {BucketF, BucketT} = split(Bucket),
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
	    log:info([?MODULE, caching, Node, Bucket]),
	    {ok, add_node(Node#node{status=cache}, bucket)}
    end.

% --- api ---

% this address has been verified as actually existing
touched(Address, Suffix, Time, Bucket, May_split) ->
    log:info([?MODULE, touching, Address, Bucket]),
    case get_node(Address, Bucket) of
	{ok, Node} ->
	    case Node#node.status of
		live -> 
		    % update last_seen time
		    {ok, update_node(Node#node{last_seen=Time}, Bucket)};
		stale ->
		    % update last_seen time and promote to live
		    {ok, update_node(Node#node{last_seen=Time, status=live}, Bucket)};
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
    log:info([?MODULE, seeing, Address, Bucket]),
    case get_node(Address, Bucket) of
	{ok, Node} ->
	    case Node#node.status of
		cache ->
		    % for cache nodes being in a .see is good enough
		    {ok, update_node(Node#node{last_seen=Time}, Bucket)};
		_ ->
		    % for live/stale nodes we require direct contact so ignore this
		    {ok, Bucket}
	    end;
	none ->
	    % put node in cache
	    Node = #node{
	      address = Address,
	      'end' = util:to_end(Address),
	      suffix = Suffix,
	      status = cache,
	      last_seen = Time
	     },
	    {ok, add_node(Node, Bucket)}
    end.

% this address failed to reply in a timely manner
timedout(Address, Bucket) ->
    log:info([?MODULE, timing_out, Address, Bucket]),
    case get_node(Address, Bucket) of
	{ok, Node} ->
	    case Node#node.status of
		live ->
		    % mark as stale, return a cache node that might be a suitable replacement
		    Bucket2 = update_node(Node#node{status=stale}, Bucket),
		    pop_cache(Bucket2);
		_ -> 
		    % if cache or stale already we don't care 
		    {ok, bucket}
	    end;
	none ->
	    % wtf? we don't even know this node?
	    % one way this could happen: 
	    % send N1, sendN1, timedout N1, add N2 (pushing N1 out of stale), timedout N1 
	    log:warning([?MODULE, unknown_node_timedout, Address, Bucket]),
	    {ok, Bucket}
    end.
