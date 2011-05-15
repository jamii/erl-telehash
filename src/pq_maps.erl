% priority queues implemented on top of gb_trees
% keys are unique. only need key to delete

-module(pq_maps).

-export([empty/0, is_empty/1, size/1, push_one/3, pop_lo/1, pop_hi/1, push/2, pop_los/2, peek_lo/1, peek_hi/1, to_list/1, from_list/1, get/2, delete/2, values/1]).

-type pq_map() :: gb_tree().

-export_types([pq_map/0]).

-spec empty() -> pq_map().
empty() ->
    gb_trees:empty().

-spec is_empty(pq_map()) -> boolean().
is_empty(Q) ->
    gb_trees:is_empty(Q).

-spec size(pq_map()) -> integer().
size(Q) ->
    gb_trees:size(Q).

-spec push_one(term(), term(), pq_map()) -> pq_map().
push_one(Key, Value, Q) ->
    case gb_trees:lookup(Key, Q)  of
	{value, _} ->
	    gb_trees:update(Key, Value, Q);
	none ->
	    gb_trees:insert(Key, Value, Q)
    end.

-spec pop_lo(pq_map()) -> none | {Key::term(), Value::term(), pq_map()}.
pop_lo(Q) ->
    case is_empty(Q) of 
	true -> none;
	false ->
	    gb_trees:take_smallest(Q)
    end.

-spec pop_hi(pq_map()) -> none | {Key::term(), Value::term(), pq_map()}.
pop_hi(Q) ->
    case is_empty(Q) of 
	true -> none;
	false ->
	    gb_trees:take_largest(Q)
    end.

-spec push(list({term(), term()}), pq_map()) -> pq_map().
push(Items, Q) ->
    lists:foldl(fun ({Key, Value}, Q2) -> push_one(Key, Value, Q2) end, Q, Items).

-spec pop_los(pq_map(), integer()) -> {list({Key::term(), Value::term()}), pq_map()}.
pop_los(Q, 0) ->
    {[], Q};
pop_los(Q, K) when K > 0 ->
    case pop_lo(Q) of
	none -> 
	    {[], Q};
	{Key, Value, Q2} ->
	    {Items, Q3} = pop_los(Q2, K-1),
	    {[{Key, Value} | Items], Q3}
    end.

-spec peek_lo(pq_map()) -> none | {Key::term(), Value::term()}.
peek_lo(Q) ->
    case is_empty(Q) of
	true -> none;
	false -> gb_trees:smallest(Q)
    end.

-spec peek_hi(pq_map()) -> none | {Key::term(), Value::term()}.
peek_hi(Q) ->
    case is_empty(Q) of
	true -> none;
	false -> gb_trees:largest(Q)
    end.

% assumes Key is in Q, crashes otherwise 
-spec get(term(), pq_map()) -> Value::term(). 
get(Key, Q) ->
    gb_trees:get(Key, Q).

-spec delete(term(), pq_map()) -> pq_map().
delete(Key, Q) ->
    gb_trees:delete_any(Key, Q).

-spec to_list(pq_map()) -> list({Key::term(), Value::term()}).
to_list(Q) ->
    gb_trees:to_list(Q).

-spec from_list(list({Key::term(), Value::term()})) -> pq_map().
from_list(List) ->
    push(List, empty()).

-spec values(pq_map()) -> list(Value::term()).
values(Q) ->
    gb_trees:values(Q).
