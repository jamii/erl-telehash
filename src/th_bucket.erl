% implements the bucket part of kademlia k-buckets
% important rules:
% * the bucket should contain at most ?K peers (not including cached peers)
% * we should only ever report peer addresses which we have personally confirmed
% * responsive peers should never be removed from the bucket
% * peers should never be removed from the bucket unless a suitable replacement exists
% these rules make the router resilient to flooding, poisoning and network failure

-module(th_bucket).

-include("types.hrl").
-include("conf.hrl").
-include("log.hrl").

-export([empty/0, split/1, touched/5, seen/4, timedout/2, dialed/2, by_dist/2, last_touched/1, last_dialed/1]).

-define(K, ?REPLICATION).

-type status() :: live | stale | cache.

-record(peer, {
	  address :: address(), % peers address
	  'end' :: 'end'(), % peers end
	  suffix :: bits(), % the remaining bits of the peers end left over from the bit_tree
	  status :: status(), % one of [live, stale, cache]
	  last_seen :: now() % for live/stale peers, the time of the last direct contact. for cache peers the time of the last direct or indirect contact
	 }).
-type peer() :: #peer{}.

-record(bucket, {
	  last_dialed :: never | now(), % time an end in this bucket was last dialed, or never
	  peers :: gb_tree(), % gb_tree mapping addresses to {Status, Last_seen}
	  % remaining fields are pq's of peers sorted by their last_seen field
	  live :: pq_maps:pq(), % peers currently expected to be alive
	  stale :: pq_maps:pq(), % peers which have not replied recently
	  cache :: pq_maps:pq() % potential peers which we have not yet verified 
	 }). % invariant: pq_maps:size(live) + pq_maps:size(stale) <= ?K
-type bucket() :: #bucket{}.
-export_types([bucket/0]).

-type update() :: th_bit_tree:bucket_update(bucket()) .
-type ping_and_update() :: {ping, address(), update()}.

% --- api ---

-spec empty() -> bucket().
empty() ->
    #bucket{
       last_dialed = never,
       peers = gb_trees:empty(),
       live = pq_maps:empty(),
       stale = pq_maps:empty(),
       cache = pq_maps:empty()
      }.

% this address has been verified as actually existing (direct contact)
% !!! would be nice for this to return a ping when new node goes in cache
-spec touched(address(), bits(), now(), bucket(), boolean()) -> update().
touched(Address, Suffix, Time, Bucket, May_split) ->
    ?INFO([touching, {address, Address}, {bucket, Bucket}]),
    case get_peer(Address, Bucket) of
	{ok, Peer} ->
	    case Peer#peer.status of
		live -> 
		    % update last_seen time
		    ok(update_peer(Peer#peer{last_seen=Time}, Bucket));
		stale ->
		    % update last_seen time and promote to live
		    ok(update_peer(Peer#peer{last_seen=Time, status=live}, Bucket));
		cache ->
		    % potentially promote the peer to live
		    Bucket2 = del_peer(Peer, Bucket),
		    new_live_peer(Address, Suffix, Time, Bucket2, May_split)
	    end;
	none ->
	    % potentially add the peer to live
	    new_live_peer(Address, Suffix, Time, Bucket, May_split)
    end.

% this address has been reported to exist by another peer (indirect contact)
-spec seen(address(), bits(), now(), bucket()) -> update() | ping_and_update().
seen(Address, Suffix, Time, Bucket) ->
    ?INFO([seeing, {address, Address}, {bucket, Bucket}]),
    case get_peer(Address, Bucket) of
	{ok, Peer} ->
	    case Peer#peer.status of
		cache ->
		    % for cache peers being indirect contact good enough
		    ok(update_peer(Peer#peer{last_seen=Time}, Bucket));
		_ ->
		    % for live/stale peers we require direct contact so ignore this
		    ok(Bucket)
	    end;
	none ->
	    % put peer in cache, return a live peer to ping
	    Peer = #peer{
	      address = Address,
	      'end' = th_util:to_end(Address),
	      suffix = Suffix,
	      status = cache,
	      last_seen = Time
	     },
	    Bucket2 = add_peer(Peer, Bucket),
	    case peek_live_old(Bucket2) of
		none -> ok(Bucket2);
		{ok, Live_peer} -> {ping, Live_peer#peer.address, ok(Bucket2)}
	    end
    end.

% this address failed to reply in a timely manner
-spec timedout(address(), bucket()) -> update() | ping_and_update().
timedout(Address, Bucket) ->
    ?INFO([timing_out, {address, Address}, {bucket, Bucket}]),
    case get_peer(Address, Bucket) of
	{ok, Peer} ->
	    case Peer#peer.status of
		live ->
		    % mark as stale, maybe return a cache peer that might be a suitable replacement
		    Bucket2 = update_peer(Peer#peer{status=stale}, Bucket),
		    pop_cache_new(Bucket2);
		_ -> 
		    % if cache or stale already we don't care 
		    ok(Bucket)
	    end;
	none ->
	    % wtf? we don't even know this peer?
	    % one way this could happen: 
	    % send N1, sendN1, timedout N1, add N2 (pushing N1 out of stale), timedout N1 
	    ?WARN([unknown_peer_timedout, {address, Address}, {bucket, Bucket}]),
	    ok(Bucket)
    end.

-spec dialed(now(), bucket()) -> th_bit_tree:bucket_update(bucket()).
dialed(Time, Bucket) ->
    ok(Bucket#bucket{last_dialed=Time}).

-spec by_dist('end'(), bucket()) -> list(address()).
by_dist(End, #bucket{live=Live, stale=Stale}) ->
    Peers = pq_maps:to_list(Live) ++ pq_maps:to_list(Stale),
    % !!! maybe should prefer to return live peers even if further away
    Peers_by_dist = [{th_util:distance(End, Peer#peer.'end'), Peer} || {_Key, Peer} <- Peers],
    [Peer#peer.address || {_Dist, Peer} <- lists:sort(Peers_by_dist)].

-spec last_touched(bucket()) -> never | now().
last_touched(#bucket{live=Live, stale=Stale}) ->
    % !!! ugly
    case {pq_maps:peek_hi(Live), pq_maps:peek_hi(Stale)} of
	{none, none} -> never;
	{none, {{Last_stale, _}, _}} -> Last_stale;
	{{{Last_live, _}, _}, none} -> Last_live;
	{{{Last_live, _}, _}, {{Last_stale, _}, _}} ->
	    % !!! no min in my erlang version :(
	    if Last_live > Last_stale -> Last_live; true -> Last_stale end
    end.

-spec last_dialed(bucket()) -> now() | never.
last_dialed(Bucket) ->
    Bucket#bucket.last_dialed.

% --- functions maintaining the bucket invariants ---

% response format for bit_tree
-spec ok(bucket()) -> th_bit_tree:bucket_update(bucket()).
ok(Bucket) ->
    {Lives, Stales, _} = sizes(Bucket),
    {ok, Lives + Stales, Bucket}.

% response format for bit_tree
-spec split(bucket()) -> th_bit_tree:bucket_update(bucket()). 
split(Bucket) ->
    Peers = to_list(Bucket),
    BucketF = from_list([Peer#peer{suffix=Suffix2} || #peer{suffix=[false|Suffix2]}=Peer <- Peers]),
    {LivesF, StalesF, _} = sizes(BucketF),
    BucketT = from_list([Peer#peer{suffix=Suffix2} || #peer{suffix=[true|Suffix2]}=Peer <- Peers]),
    {LivesT, StalesT, _} = sizes(BucketT),
    {split, LivesF+StalesF, LivesT+StalesT, BucketF, BucketT}.

% assumes Address is not already in Bucket, otherwise crashes
-spec new_live_peer(address(), bits(), now(), bucket(), boolean()) -> update() | ping_and_update().
new_live_peer(Address, Suffix, Time, Bucket, May_split) ->
    Peer = #peer{
      address = Address,
      'end' = th_util:to_end(Address),
      suffix = Suffix,
      status = undefined,
      last_seen = Time
     },
    {Lives, Stales, _} = sizes(Bucket),
    if
	Lives + Stales < ?K ->
	    % space left in live
	    ?INFO([adding, {peer, Peer}, {bucket, Bucket}]),
	    ok(add_peer(Peer#peer{status=live}, Bucket));
	(Lives < ?K) and (Stales > 0) ->
	    % space left in live if we push something out of stale
	    ?INFO([adding, {peer, Peer}, {bucket, Bucket}]),
	    Bucket2 = drop_stale(Bucket),
	    ok(add_peer(Peer#peer{status=live}, Bucket2));
	May_split and (Suffix /= []) ->
	    % allowed to split the bucket to make space
	    ?INFO([splitting, {peer, Peer}, {bucket, Bucket}]),
	    split(Bucket);
	true ->
	    % not allowed to split, will have to go in the cache
	    ?INFO([caching, {peer, Peer}, {bucket, Bucket}]),
	    ok(add_peer(Peer#peer{status=cache}, Bucket))
    end.

% --- internal functions handling the (peers -> live/stale/cache) mapping ---

% assumes Peer does not already exist in Bucket, crashes otherwise
-spec add_peer(peer(), bucket()) -> bucket().
add_peer(#peer{address=Address, status=Status, last_seen=Last_seen}=Peer, 
	 #bucket{peers=Peers, live=Live, stale=Stale, cache=Cache}=Bucket) ->
    Peers2 = gb_trees:insert(Address, {Status, Last_seen}, Peers),
    case Status of
	live ->
	    Live2 = pq_maps:push_one({Last_seen, Address}, Peer, Live),
	    Bucket#bucket{peers=Peers2, live=Live2};
	stale ->
	    Stale2 = pq_maps:push_one({Last_seen, Address}, Peer, Stale),
	    Bucket#bucket{peers=Peers2, stale=Stale2};
	cache ->
	    Cache2 = pq_maps:push_one({Last_seen, Address}, Peer, Cache),
	    Bucket#bucket{peers=Peers2, cache=Cache2}
    end.
    
% assumes Peer already exists in Bucket, crashes otherwise
-spec del_peer(peer() | address(), bucket()) -> bucket().
del_peer(#peer{address=Address}, Bucket) ->
    del_peer(Address, Bucket);
del_peer(Address,
	 #bucket{peers=Peers, live=Live, stale=Stale, cache=Cache}=Bucket) ->
    {Status, Last_seen} = gb_trees:get(Address, Peers),
    Peers2 = gb_trees:delete(Address, Peers),
    case Status of
	live ->
	    Live2 = pq_maps:delete({Last_seen, Address}, Live),
	    Bucket#bucket{peers=Peers2, live=Live2};
	stale ->
	    Stale2 = pq_maps:delete({Last_seen, Address}, Stale),
	    Bucket#bucket{peers=Peers2, stale=Stale2};
	cache ->
	    Cache2 = pq_maps:delete({Last_seen, Address}, Cache),
	    Bucket#bucket{peers=Peers2, cache=Cache2}
    end.

% assumes Peer already exists in Bucket, crashes otherwise
-spec update_peer(peer(), bucket()) -> bucket().
update_peer(Peer, Bucket) ->
    add_peer(Peer, del_peer(Peer, Bucket)).

-spec get_peer(address(), bucket()) -> none | {ok, peer()}.
get_peer(Address, 
	 #bucket{peers=Peers, live=Live, stale=Stale, cache=Cache}) ->
    case gb_trees:lookup(Address, Peers) of
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

-spec to_list(bucket()) -> list(peer()).
to_list(#bucket{live=Live, stale=Stale, cache=Cache}) ->
    pq_maps:values(Live) ++ pq_maps:values(Stale) ++ pq_maps:values(Cache).

-spec from_list(list(peer())) -> bucket().
from_list(Peers) ->
    lists:foldl(fun add_peer/2, empty(), Peers).

-spec sizes(bucket()) -> {integer(), integer(), integer()}.
sizes(#bucket{live=Live, stale=Stale, cache=Cache}) ->
    {pq_maps:size(Live), pq_maps:size(Stale), pq_maps:size(Cache)}.

% drop the oldest stale peer, crashes if none exist
-spec drop_stale(bucket()) -> bucket().
drop_stale(#bucket{peers=Peers, stale=Stale}=Bucket) ->
    {_Key, #peer{address=Address}, Stale2} = pq_maps:pop_lo(Stale),
    Peers2 = gb_trees:delete(Address, Peers),
    Bucket#bucket{peers=Peers2, stale=Stale2}.

% return most recently seen cache peer, if any exist
-spec pop_cache_new(bucket()) -> update() | ping_and_update().
pop_cache_new(#bucket{peers=Peers, cache=Cache}=Bucket) ->
    case pq_maps:pop_hi(Cache) of
	{_Key, #peer{address=Address}=Peer, Cache2} ->
	    Peers2 = gb_trees:delete(Address, Peers),
	    {ping, Peer#peer.address, ok(Bucket#bucket{peers=Peers2, cache=Cache2})};
	none ->
	    ok(Bucket)
    end.

% return the oldest live peer
-spec peek_live_old(bucket()) -> none | {ok, peer()}.
peek_live_old(#bucket{live=Live}) ->
    case pq_maps:peek_lo(Live) of
	none -> none;
	{_, Peer} -> {ok, Peer}
    end.
	     	    
% --- end ---
