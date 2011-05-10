% simple buckets used for testing bit_tree

-module(test_bucket).

-include("conf.hrl").

-export([bits/1, add/3, split/1, add_to_tree/2, make_tree/1, distance/2, list_from/2]).

-define(MAX_SIZE, 3).
-define(BITS, ?END_BITS).

bits(Int) ->
    th_util:to_bits(<<Int:?BITS>>).

add(Suffix, Int, Bucket) ->
    split([{Suffix, Int} | Bucket]).

split(Bucket) ->
    if 
	length(Bucket) > ?MAX_SIZE -> 
	    BucketF = [{Suffix2, Int2} || {[false | Suffix2], Int2} <- Bucket],
	    BucketT = [{Suffix2, Int2} || {[true | Suffix2], Int2} <- Bucket],
	    {split, split(BucketF), split(BucketT)};
	true ->
	    {ok, length(Bucket), Bucket}
    end.

add_to_tree(Int, Tree) ->
    th_bit_tree:update(
         fun (Suffix, _Depth, _Gap_size, Bucket) ->
	         add(Suffix, Int, Bucket)
         end,
         bits(Int),
         bits(Int),  % dont care about gap for now
         Tree).

make_tree(Ints) ->   
    Tree = th_bit_tree:empty(0, []),
    lists:foldl(fun add_to_tree/2, Tree, Ints).

distance(IntA, IntB) ->
    th_util:distance({'end', <<IntA:?BITS>>}, {'end', <<IntB:?BITS>>}).

% output *should* be in ascending order
list_from(Int, Tree) ->
    List = th_iter:to_list(th_bit_tree:iter(bits(Int), Tree)),
    lists:map(
      fun (Bucket) ->
	      lists:sort([{distance(Int, Elem), Elem} || {_,Elem} <- Bucket])
      end,
      List).
