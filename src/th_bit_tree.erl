% implements the tree part of kademlias k-buckets
% a bit_tree maps ends (lists of bits) to buckets
% as far as the bit_tree is concerned the buckets are completely opaque
% the bit_tree also calculates various numbers needed for splitting decisions

-module(th_bit_tree).

-include("conf.hrl").

-export([empty/2, update/4, iter/2]).

% a bit_tree is either a leaf or a branch
-record(leaf, {
	  size, % size of bucket
	  bucket % some opaque bucket of stuff
	 }).
-record(branch, {
	  size, % size(childF) + size(childT)
	  childF, % tree containing nodes whose next bit is false
	  childT % tree containing nodes whose next bit is true
	 }).

% --- api ---

empty(Size, Bucket) ->
    #leaf{size=Size, bucket=Bucket}.
		
update(Fun, Bits, Self, Tree) when is_function(Fun), is_list(Bits), is_list(Self) ->
    update(Fun, Bits, {self, Self}, 0, Tree).

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
iter(Bits, Tree) ->
    iter(Bits, [], Tree, fun() -> done end).
			     
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

tree_size(#leaf{size=Size}) ->
    Size;
tree_size(#branch{size=Size}) ->
    Size.

bucket_update_to_tree({ok, Size, Bucket}) ->
    #leaf{size=Size, bucket=Bucket};
bucket_update_to_tree({split, SplitF, SplitT}) ->
    ChildF = bucket_update_to_tree(SplitF),
    ChildT = bucket_update_to_tree(SplitT),
    #branch{size=tree_size(ChildF)+tree_size(ChildT), childF=ChildF, childT=ChildT}.

% --- end ---
