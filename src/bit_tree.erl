% implements the tree part of kademlias k-buckets
% a bit_tree maps ends (lists of bits) to buckets
% as far as the bit_tree is concerned the buckets are completely opaque
% a bit_tree also knows the nodes own end and can calculate various numbers needed for splitting decisions

-module(bit_tree).

-include("conf.hrl").

-export([empty/3, move_to/2, extend/2, retract/2, update/2, iter/2]).
-export([gap/1, depth/1]).

% a bit_tree is either a leaf or a branch
-record(leaf, {
	  size, % size of bucket
	  bucket % some opaque bucket of stuff
	 }).
-record(branch, {
	  size, % size(childF) + size(childT)
	  childF, % tree containing nodes whose next bit is false
	  childT % % tree containing nodes whose next bit is true
	 }).

% zipper-esque structure marking a position in a bit_tree
-record(finger, {
	  sizer, % a size function for buckets
	  tree, % current sub-tree
	  self, % the path *to* self. either {down, Down_bits} or {up, Up_bits, Down_bits, Gap}
		% where Gap is the size of the largest tree containing self but not touching this finger
	  depth, % the number of bits away from the root tree
	  zipper % a list of {Bit, Tree} pairs marking branches NOT taken
	 }).

% --- api ---

gap(#finger{self=Self}) ->
    case(Self) of
	{down, _} ->
	    0;
	{up, _, _, Gap} ->
	    Gap
    end.

depth(#finger{depth=Depth}) ->
    Depth.

empty(Self, Bucket, Sizer) ->
    #finger{
       sizer = Sizer,
       tree = #leaf{size=Sizer(Bucket), bucket=Bucket},
       self = {down, Self},
       depth = 0,
       zipper = []
      }.

move_to(Bits, #finger{depth=Depth}=Finger) when length(Bits) == ?END_BITS ->
    % !!! naive version
    extend(Bits, retract(Depth, Finger)).
		
update(Fun,
       #finger{
	 sizer=Sizer,
	 tree=#leaf{bucket=Bucket}
	}=Finger) ->
    Tree =
	case Fun(Bucket) of
	    {one, Bucket2} ->
		#leaf{size=Sizer(Bucket2), bucket=Bucket2};
	    {split, BucketF, BucketT} ->
		SizeF = Sizer(BucketF),
		SizeT = Sizer(BucketT),
		ChildF = #leaf{size=SizeF, bucket=BucketF},
		ChildT = #leaf{size=SizeT, bucket=BucketT},
		#branch{size=SizeF+SizeT, childF=ChildF, childT=ChildT}
	end,
    Finger#finger{tree=Tree}.
    
% iterate through buckets in ascending order of xor distance to (current position ++ Suffix)
iter(Suffix, #finger{tree=Tree, zipper=Zipper}) ->
    iter_buckets(Tree, Suffix, iter_zipper(Zipper, Suffix)).

% --- internal functions ---

tree_size(#leaf{size=Size}) ->
    Size;
tree_size(#branch{size=Size}) ->
    Size.

extend(Bits, #finger{tree=#leaf{}}=Finger) -> % must always end on a leaf
    {Bits, Finger};
extend([Next | Bits],
       #finger{
	 tree = #branch{childF=ChildF, childT=ChildT},
	 self = Self,
	 depth = Depth,
	 zipper = Zipper
	}=Finger) ->
    {Branch_taken, Branch_missed} =
	case Next of
	    false -> {ChildF, ChildT};
	    true -> {ChildT, ChildF}
	end,
    Self2 = 
	case Self of
	    {up, Up, Down, Gap} -> 
		% already stepped out of gap
		{up, [not(Next)|Up], Down, Gap};
	    {down, [Bit|Down]} when Bit == Next ->
		% still in the gap
		{down, Down};
	    {down, [Bit|Down]} when Bit /= Next ->
		% leaving gap, check its size
		{up, [not(Next)], [Bit|Down], tree_size(Branch_missed)}
	end,
    Depth2 = Depth+1,
    Zipper2 = [{not(Next), Branch_missed} | Zipper],
    Finger2 = Finger#finger{
      tree = Branch_taken,
      self = Self2,
      depth = Depth2,
      zipper = Zipper2
     },
    extend(Bits, Finger2).

retract(0, Finger) ->
    Finger;
retract(N, 
	#finger{
	  tree = Tree,
	  self = Self,
	  depth = Depth,
	  zipper = [{Last,Branch}|Zipper]
	 }=Finger) when N>0 ->
    Size = tree_size(Tree) + tree_size(Branch),
    Tree2 =
	case Last of
	    false -> #branch{size=Size, childF=Branch, childT=Tree};
	    true -> #branch{size=Size, childF=Tree, childT=Branch}
	end,
    Self2 = 
	case Self of
	    {down, Down} ->
		% already in gap
		{down, [Last|Down]};
	    {up, [], Down, _Gap} ->
		% just entered gap
		{down, [Last|Down]};
	    {up, [Bit|Up], Down, Gap} ->
		% still outside gap
		true = (Bit==Last), % assert
		{up, Up, Down, Gap}
	end,
    Depth2 = Depth-1,
    Finger2 =
	Finger#finger{
	  tree=Tree2,
	  self=Self2,
	  depth=Depth2,
	  zipper=Zipper
	 },
    retract(N-1, Finger2).

% iterate through buckets in ascending order of xor distance to Bits, then hand over to Iter
iter_buckets(#leaf{bucket=Bucket}, _Bits, Iter) ->
    fun () ->
	    {Bucket, Iter}
    end;
iter_buckets(#branch{childF=ChildF, childT=ChildT}, [Bit|Bits], Iter) ->
    case Bit of 
	true ->
	    iter_buckets(ChildT, Bits, iter_buckets(ChildF, Bits, Iter));
	false ->
	    iter_buckets(ChildF, Bits, iter_buckets(ChildT, Bits, Iter))
    end.

% iterate through buckets in ascending order of xor distance to (current position ++ Suffix)
iter_zipper([], _Suffix) ->
    fun () -> 
	    done
    end;
iter_zipper([{Bit, Tree} | Zipper], Suffix) ->
    iter_buckets(Tree, Suffix, iter_zipper(Zipper, [Bit|Suffix])).

% --- end ---
