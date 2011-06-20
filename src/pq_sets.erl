% priority queues implemented on top of gb_sets
% {key, node} pairs are unique. need {key, node} pair to delete

-module(pq_sets).

-export([empty/0, is_empty/1, size/1, push_one/2, pop_one/1, push/2, pop/2, peek/1, to_list/1, from_list/1, delete/2]).

-type pq() :: gb_set().

-export_types([pq/0]).

-spec empty() -> pq().
empty() ->
    gb_sets:empty().

-spec is_empty(pq()) -> boolean().
is_empty(Q) ->
    gb_sets:is_empty(Q).

-spec size(pq()) -> integer().
size(Q) ->
    gb_sets:size(Q).

-spec push_one({term(), term()}, pq()) -> pq().
push_one({Prio, Item}, Q) ->
    gb_sets:add_element({Prio, Item}, Q).

-spec pop_one(pq()) -> none | {{Prio::term(), Item::term()}, pq()}.
pop_one(Q) ->
    case is_empty(Q) of 
	true -> none;
	false ->
	    {{Prio, Item}, Q2} = gb_sets:take_smallest(Q),
	    {{Prio, Item}, Q2}
    end.

-spec push(list({Prio::term(), Item::term()}), pq()) -> pq().
push(Items, Q) ->
    lists:foldl(fun ({Prio, Item}, Q2) -> push_one({Prio, Item}, Q2) end, Q, Items).

-spec pop(pq(), integer()) -> {list({Prio::term(), Value::term()}), pq()}.
pop(Q, 0) ->
    {[], Q};
pop(Q, K) when K > 0 ->
    case pop_one(Q) of
	none -> 
	    {[], Q};
	{Item, Q2} ->
	    {Items, Q3} = pop(Q2, K-1),
	    {[Item | Items], Q3}
    end.

-spec peek(pq()) -> {Prio::term(), Value::term()}.
peek(Q) ->
    gb_sets:smallest(Q).

-spec delete({Prio::term(), Value::term()}, pq()) -> pq().
delete({Prio, Item}, Q) ->
    gb_sets:delete_any({Prio, Item}, Q).

-spec to_list(pq()) -> list({Prio::term(), Value::term()}).
to_list(Q) ->
    gb_sets:to_list(Q).

-spec from_list(list({Prio::term(), Value::term()})) -> pq().
from_list(List) ->
    gb_sets:from_list(List).
