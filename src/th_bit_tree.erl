% implements the tree part of kademlias k-buckets
% a bit_tree maps ends (list of bits) to buckets
% the bit_tree also calculates various numbers needed for deciding when to split a bucket

-module(th_bit_tree).

-include("types.hrl").
-include("conf.hrl").

-include_lib("proper/include/proper.hrl").

-export([new/2, update/4, iter/2]).

-type bucket() :: any(). % totally opaque

% a bit_tree is either a leaf or a branch
-record(leaf, {size, bucket}).
-type leaf() :: #leaf{
	    size :: integer(), % size of bucket
	    bucket :: bucket() % bucket of stuff
}.

-record(branch, {size, childF, childT}).
-type branch() :: #branch{
	  size :: integer(), % size(childF) + size(childT)
	  childF :: leaf() | branch(), % tree containing nodes whose next bit is false
	  childT :: leaf() | branch() % tree containing nodes whose next bit is true
	 }.

-type bit_tree() :: leaf() | branch().
-export_types([bit_tree/0]).
	
-type update_fun() :: fun(
	      (
                Suffix :: bits(),  
	        Depth :: integer(),
	        Gap :: integer(), % Gap is the total size of buckets closer to the bucket containing self than the bucket containing the target of the update
	        Bucket :: bucket()
	      ) ->
	        {ok, Size::integer(), bucket()} 
	      | {split, SizeF::integer(), bucket(), SizeT::integer(), bucket()}
	      ).
-export_types(update_fun/0).

% --- api ---

-spec new(Size::integer(), bucket()) -> leaf().
new(Size, Bucket) ->
    #leaf{size=Size, bucket=Bucket}.
	
-spec update(update_fun(), bits(), bits(), bit_tree()) -> bit_tree().
update(Fun, Bits, Self, Tree) when is_function(Fun), is_list(Bits), is_list(Self) ->
    update(Fun, Bits, Self, 0, 0, Tree).

-spec update(update_fun(), bits(), bits(), integer(), integer(), bit_tree()) -> bit_tree().
update(Fun, Bits, Self, Gap, Depth, #leaf{bucket=Bucket}) ->
    case Fun(Bits, Depth, Gap, Bucket) of
	{ok, Size, Bucket2} ->
	    % update done
	    #leaf{size=Size, bucket=Bucket2};
	{split, SizeF, SizeT, BucketF, BucketT} ->
	    % have to split the bucket first then run the update on the new tree
	    Tree = #branch{size=SizeF+SizeT, 
			   childF=#leaf{size=SizeF, bucket=BucketF}, 
			   childT=#leaf{size=SizeT, bucket=BucketT}
			  },
	    update(Fun, Bits, Self, Gap, Depth, Tree)
    end;
update(Fun, Bits, Self, Gap, Depth, #branch{childF=ChildF, childT=ChildT}) ->
    [Next|Bits2] = Bits,
    {Self2, Gap2} =
	case Self of
	    [Next|Rest] -> {Rest, Gap};
	    [_|Rest] ->
		Other = case Next of true -> ChildF; false -> ChildT end,
		{Rest, Gap + tree_size(Other)}
	end,
    Depth2 = Depth+1,
    case Next of
	true ->
	    ChildT2 = update(Fun, Bits2, Self2, Gap2, Depth2, ChildT),
	    Size = tree_size(ChildF) + tree_size(ChildT2),
	    #branch{size=Size, childF=ChildF, childT=ChildT2};
	false ->
	    ChildF2 = update(Fun, Bits2, Self2, Gap2, Depth2, ChildF),
	    Size = tree_size(ChildF2) + tree_size(ChildT),
	    #branch{size=Size, childF=ChildF2, childT=ChildT}
    end.

% iterate through buckets in ascending order of xor distance to Bits
-spec iter(bits(), bit_tree()) -> th_iter:iter(bucket()).
iter(Bits, Tree) ->
    iter(Bits, [], Tree, th_iter:empty()).

-spec iter(bits(), bits(), bit_tree(), th_iter:iter(bucket())) -> th_iter:iter(bucket()).
iter(_Suffix, Prefix, #leaf{bucket=Bucket}, Iter) ->
    th_iter:cons({lists:reverse(Prefix), Bucket}, Iter);
iter(Suffix, Prefix, #branch{childF=ChildF, childT=ChildT}, Iter) ->
    [Bit|Suffix2] = Suffix,
    Prefix2 = [Bit|Prefix],
    case Bit of
	false ->
	    % childF first, childT second
	    iter(Suffix2, Prefix2, ChildF, iter(Suffix2, Prefix2, ChildT, Iter));
	true ->
	    % childT first, childF second
	    iter(Suffix2, Prefix2, ChildT, iter(Suffix2, Prefix2, ChildF, Iter))
    end.

% --- internal functions ---

-spec tree_size(bit_tree()) -> integer().
tree_size(#leaf{size=Size}) ->
    Size;
tree_size(#branch{size=Size}) ->
    Size.

% --- test bucket ---
% these simple buckets just split whenever they contain more than 3 elements

-type test_bucket() :: list({Suffix::bits(), 'end'()}).

-define(MAX_SIZE, 3).

add_to_tree(End, Tree) ->   
    update(
      fun (Suffix, Depth, _Gap, Bucket) ->
	      if 
		  (length(Bucket) > ?MAX_SIZE) and (Depth < ?END_BITS) ->
		      ChildF = [{SuffixB, EndB} || {[false|SuffixB], EndB} <- Bucket],
		      ChildT = [{SuffixB, EndB} || {[true|SuffixB], EndB} <- Bucket],
		      {split, length(ChildF), length(ChildT), ChildF, ChildT};
		  true ->
		      Bucket2 = [{Suffix,End}|Bucket],
		      {ok, length(Bucket2), Bucket2}
	      end
      end,
      th_util:to_bits(End),
      th_util:to_bits(End),  % dont care about gap for now
      Tree
     ).

list_to_tree(Ends) ->   
    Tree = new(0, []),
    lists:foldl(fun add_to_tree/2, Tree, Ends).

tree_to_buckets(Target, Tree) ->
    th_iter:to_list(iter(th_util:to_bits(Target), Tree)).

% --- tests ---

% iter(Bits, Tree) should return Tree's buckets in ascending order of xor distance to Bits
prop_iter_order() ->
    ?FORALL(Ends, list(th_test:'end'()), 
	    ?FORALL(Target, th_test:'end'(),
		    begin
			DistancesA = lists:sort([th_util:distance(Target, End) || End <- Ends]),
			Buckets = tree_to_buckets(Target, list_to_tree(Ends)),
			% iter should sort buckets, still need to sort inside buckets
			DistancesB = lists:flatten([lists:sort([th_util:distance(Target, End) || {_Suffix, End} <- Bucket]) || Bucket <- Buckets]),
			DistancesA == DistancesB
		    end
		   )
	   ).

% gap can be defined as the total size of all buckets closer to self than the target
prop_update_gap() ->
    ?FORALL(Ends, list(th_test:'end'()), 
	    ?FORALL({Self, Target}, {th_test:'end'(), th_test:'end'()},
		    begin
			Tree = add_to_tree(Target, list_to_tree(Ends)),
			% run through buckets starting at Self until we find one containing Target
			Nearer = 
			    lists:flatten(
			      lists:takewhile(
				fun (Bucket) -> 
					lists:keyfind(Target, 2, Bucket) == false 
				end, 
				tree_to_buckets(Self, Tree)
			       )
			     ),
			update(
			  fun (_Suffix, _Depth, Gap, Bucket) ->
				  self() ! {gap, Gap},
				  {ok, length(Bucket), Bucket}
			  end,
			  th_util:to_bits(Target),
			  th_util:to_bits(Self),
			  Tree
			 ),
			receive
			    {gap, Gap} ->
				Gap2 = length(Nearer),
				Gap == Gap2
			end
		    end
		   )
	   ).	    

% --- end ---
