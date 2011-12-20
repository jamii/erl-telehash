% priority queues implemented on top of gb_trees
% keys are unique. only need key to delete

-module(pq_maps).

-export([empty/0, is_empty/1, size/1, push_one/3, pop_lo/1, pop_hi/1, push/2, pop_los/2, peek_lo/1, peek_hi/1, to_list/1, from_list/1, get/2, delete/2, values/1]).

-type pq() :: gb_tree().

-export_types([pq/0]).

-spec empty() -> pq().
empty() ->
    gb_trees:empty().

-spec is_empty(pq()) -> boolean().
is_empty(Q) ->
    gb_trees:is_empty(Q).

-spec size(pq()) -> integer().
size(Q) ->
    gb_trees:size(Q).

-spec push_one(term(), term(), pq()) -> pq().
push_one(Key, Value, Q) ->
    case gb_trees:lookup(Key, Q)  of
	{value, _} ->
	    gb_trees:update(Key, Value, Q);
	none ->
	    gb_trees:insert(Key, Value, Q)
    end.

-spec pop_lo(pq()) -> none | {Key::term(), Value::term(), pq()}.
pop_lo(Q) ->
    case is_empty(Q) of
	true -> none;
	false ->
	    gb_trees:take_smallest(Q)
    end.

-spec pop_hi(pq()) -> none | {Key::term(), Value::term(), pq()}.
pop_hi(Q) ->
    case is_empty(Q) of
	true -> none;
	false ->
	    gb_trees:take_largest(Q)
    end.

-spec push(list({term(), term()}), pq()) -> pq().
push(Items, Q) ->
    lists:foldl(fun ({Key, Value}, Q2) -> push_one(Key, Value, Q2) end, Q, Items).

-spec pop_los(pq(), integer()) -> {list({Key::term(), Value::term()}), pq()}.
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

-spec peek_lo(pq()) -> none | {Key::term(), Value::term()}.
peek_lo(Q) ->
    case is_empty(Q) of
	true -> none;
	false -> gb_trees:smallest(Q)
    end.

-spec peek_hi(pq()) -> none | {Key::term(), Value::term()}.
peek_hi(Q) ->
    case is_empty(Q) of
	true -> none;
	false -> gb_trees:largest(Q)
    end.

% assumes Key is in Q, crashes otherwise
-spec get(term(), pq()) -> Value::term().
get(Key, Q) ->
    gb_trees:get(Key, Q).

-spec delete(term(), pq()) -> pq().
delete(Key, Q) ->
    gb_trees:delete_any(Key, Q).

-spec to_list(pq()) -> list({Key::term(), Value::term()}).
to_list(Q) ->
    gb_trees:to_list(Q).

-spec from_list(list({Key::term(), Value::term()})) -> pq().
from_list(List) ->
    push(List, empty()).

-spec values(pq()) -> list(Value::term()).
values(Q) ->
    gb_trees:values(Q).
