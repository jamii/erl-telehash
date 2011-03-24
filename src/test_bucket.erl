% simple buckets used for testing bit_tree

-module(test_bucket).

-include("conf.hrl").

-export([bits/1, add/3, move_to/2, add_to_tree/2, make_tree/2, distance/2, list_from/2]).

-define(MAX_SIZE, 3).
-define(BITS, ?END_BITS).

bits(Int) ->
    util:to_bits(<<Int:?BITS>>).

add(Suffix, Int, Bucket) ->
    Bucket2 = [{Suffix, Int} | Bucket],
    if 
	length(Bucket) > ?MAX_SIZE -> 
	    BucketF = [{Suffix2, Int2} || {[false | Suffix2], Int2} <- Bucket2],
	    BucketT = [{Suffix2, Int2} || {[true | Suffix2], Int2} <- Bucket2],
	    {split, BucketF, BucketT};
	true ->
	    {one, Bucket2}
    end.

move_to(Int, Tree) ->
    bit_tree:move_to(bits(Int), Tree).

add_to_tree(Int, Tree) ->
    {Suffix, Tree2} = move_to(Int, Tree),
    bit_tree:update(fun (Bucket) -> add(Suffix, Int, Bucket) end, Tree2).

make_tree(Int, Ints) ->   
    Tree = bit_tree:empty(bits(Int), [], fun (Bucket) -> length(Bucket) end),
    lists:foldl(fun add_to_tree/2, Tree, Ints).

distance(IntA, IntB) ->
    util:distance({'end', <<IntA:?BITS>>}, {'end', <<IntB:?BITS>>}).

% output *should* be in ascending order
list_from(Int, Tree) -> 
    {Suffix, Tree2} = bit_tree:move_to(bits(Int), Tree),
    List = util:iter_to_list(bit_tree:iter(Suffix, Tree2)),
    lists:map(
      fun (Bucket) ->
	      lists:sort([{distance(Int, Elem), Elem} || {_,Elem} <- Bucket])
      end,
      List).
