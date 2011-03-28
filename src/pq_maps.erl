% priority queues implemented on top of gb_trees
% keys are unique. only need key to delete

-module(pq_maps).

-export([empty/0, is_empty/1, size/1, push_one/2, pop_lo/1, pop_hi/1, push/2, pop_los/2, peek/1, to_list/1, from_list/1, get/2, delete/2]).

empty() ->
    gb_trees:empty().

is_empty(Q) ->
    gb_trees:is_empty(Q).

size(Q) ->
    gb_trees:size(Q).

push_one(Key, Value, Q) ->
    case gb_trees:lookup(Key, Q)  of
	{value, _} ->
	    gb_trees:update(Key, Value, Q);
	false ->
	    gb_trees:insert(Key, Value, Q)
    end.

pop_lo(Q) ->
    case is_empty(Q) of 
	true -> false;
	false ->
	    gb_trees:take_smallest(Q)
    end.

pop_hi(Q) ->
    case is_empty(Q) of 
	true -> false;
	false ->
	    gb_trees:take_largest(Q)
    end.

push(Items, Q) ->
    lists:foldl(fun ({Key, Value}, Q2) -> push_one({Key, Value}, Q2) end, Q, Items).

pop_los(Q, 0) ->
    {[], Q};
pop_los(Q, K) when K > 0 ->
    case is_empty(Q) of
	true -> 
	    {[], Q};
	false ->
	    {Key, Value, Q2} = pop_lo(Q),
	    {Items, Q3} = pop_los(Q2, K-1),
	    {[{Key, Value} | Items], Q3}
    end.

peek(Q) ->
    gb_trees:smallest(Q).

% assumes Key is in Q, crashes otherwise 
get(Key, Q) ->
    gb_trees:get(Key, Q).

delete(Key, Q) ->
    gb_trees:delete_any(Key, Q).

to_list(Q) ->
    gb_trees:to_list(Q).

from_list(List) ->
    push(List, empty()).

values(Q) ->
    gb_trees:values(Q).
