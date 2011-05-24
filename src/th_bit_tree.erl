% implements the tree part of kademlias k-buckets
% a bit_tree maps ends (lists of bits) to buckets
% as far as the bit_tree is concerned the buckets are completely opaque
% the bit_tree also calculates various numbers needed for splitting decisions

-module(th_bit_tree).

-include("types.hrl").
-include("conf.hrl").

-export([empty/2, update/4, iter/2]).

% a bit_tree is either a leaf or a branch
-record(leaf, {
	  size :: integer(), % size of bucket
	  bucket % some opaque bucket of stuff
	 }).
-type leaf(Bucket) :: #leaf{bucket::Bucket}.

-record(branch, {
	  size :: integer(), % size(childF) + size(childT)
	  childF, % tree containing nodes whose next bit is false
	  childT % tree containing nodes whose next bit is true
	 }).
-type branch(Bucket) :: #branch{childF::bit_tree(Bucket), childT::bit_tree(Bucket)}.

-type bit_tree(Bucket) :: leaf(Bucket) | branch(Bucket).
-export_types([bit_tree/1]).

-type bucket_update(Bucket) :: 
	  {ok, Size::integer, Bucket} 
	| {split, Bucket, Bucket}.
-export_types(bucket_update/1).
	
-type update_fun(Bucket) :: fun(
	      (
                Suffix :: bits(),  
	        Depth :: integer(),
	        Gap :: integer(), % Gap is the size of largest subtree containing self but not containing this bucket
	        Bucket :: Bucket
	      ) ->
		bucket_update(Bucket)
	      ).

% --- api ---

-spec empty(integer(), Bucket) -> leaf(Bucket).
empty(Size, Bucket) ->
    #leaf{size=Size, bucket=Bucket}.
	
-spec update(update_fun(Bucket), bits(), bits(), bit_tree(Bucket)) -> bit_tree(Bucket).
update(Fun, Bits, Self, Tree) when is_function(Fun), is_list(Bits), is_list(Self) ->
    update(Fun, Bits, {self, Self}, 0, Tree).

-type gap() :: {gap, integer()} | {self, bits()}.

-spec update(update_fun(Bucket), bits(), gap(), integer(), bit_tree(Bucket)) -> bit_tree(Bucket).
update(Fun, Bits, Self, Depth, #leaf{bucket=Bucket}) ->
    Gap =
	case Self of
	    {gap, G} -> G;
	    {self, _} -> 0
	end,
    bucket_update_to_tree(Fun(Bits, Depth, Gap, Bucket));
update(Fun, Bits, Self, Depth, #branch{childF=ChildF, childT=ChildT}) ->
    [Next|Bits2] = Bits,
    Self2 =
	case Self of
	    {gap, _} -> Self;
	    {self, [Next|Rest]} -> {self, Rest};
	    {self, [false|_]} -> {gap, tree_size(ChildF)};
	    {self, [true|_]} -> {gap, tree_size(ChildT)}
	end,
    Depth2 = Depth+1,
    case Next of
	true ->
	    ChildT2 = update(Fun, Bits2, Self2, Depth2, ChildT),
	    Size = tree_size(ChildF) + tree_size(ChildT2),
	    #branch{size=Size, childF=ChildF, childT=ChildT2};
	false ->
	    ChildF2 = update(Fun, Bits2, Self2, Depth2, ChildF),
	    Size = tree_size(ChildF2) + tree_size(ChildT),
	    #branch{size=Size, childF=ChildF2, childT=ChildT}
    end.

% iterate through buckets in ascending order of xor distance to Bits
-spec iter(bits(), bit_tree(Bucket)) -> th_iter:iter(Bucket).
iter(Bits, Tree) ->
    iter(Bits, [], Tree, fun() -> done end).
			     
-spec iter(bits(), bits(), bit_tree(Bucket), th_iter:iter(Bucket)) -> th_iter:iter(Bucket).
iter(_Suffix, Prefix_rev, #leaf{bucket=Bucket}, Iter) ->
    fun () ->
	    {{lists:reverse(Prefix_rev), Bucket}, Iter}
    end;
iter(Suffix, Prefix_rev, #branch{childF=ChildF, childT=ChildT}, Iter) ->
    [Bit|Suffix2] = Suffix,
    Prefix_rev2 = [Bit|Prefix_rev],
    case Bit of 
	true ->
	    iter(Suffix2, Prefix_rev2, ChildT, iter(Suffix2, Prefix_rev2, ChildF, Iter));
	false ->
	    iter(Suffix2, Prefix_rev2, ChildF, iter(Suffix2, Prefix_rev2, ChildT, Iter))
    end.

% --- internal functions ---

-spec tree_size(bit_tree(_Bucket)) -> integer().
tree_size(#leaf{size=Size}) ->
    Size;
tree_size(#branch{size=Size}) ->
    Size.

-spec bucket_update_to_tree(bucket_update(Bucket)) -> bit_tree(Bucket).
bucket_update_to_tree({ok, Size, Bucket}) ->
    #leaf{size=Size, bucket=Bucket};
bucket_update_to_tree({split, SplitF, SplitT}) ->
    ChildF = bucket_update_to_tree(SplitF),
    ChildT = bucket_update_to_tree(SplitT),
    #branch{size=tree_size(ChildF)+tree_size(ChildT), childF=ChildF, childT=ChildT}.

% --- end ---
