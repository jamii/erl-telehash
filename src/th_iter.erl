% pure(ish) lazy lists

-module(th_iter).

-include_lib("proper/include/proper.hrl").

-export([to_list/1, from_list/1, map/2, take/2, foreach/2, flatten/1]).

-type iter(X) :: fun(() -> done | {X, iter(X)}).
-export_types([iter/1]).

% --- api ---

-spec to_list(iter(X)) -> list(X). 
to_list(Iter) ->
    case Iter() of
	done ->
	    [];
	{Head, Iter2} ->
	    [Head | to_list(Iter2)]
    end.

-spec from_list(list(X)) -> iter(X).
from_list(List) ->
    fun () ->
	    case List of 
		[] ->
		    done;
		[Head | List2] ->
		    {Head, from_list(List2)}
	    end
    end.

-spec map(fun((X) -> Y), iter(X)) -> iter(Y).
map(F, Iter) ->
                                         fun () ->
	    case Iter() of
		done ->
		    done;
		{Head, Iter2} ->
		    {F(Head), map(F, Iter2)}
	    end
    end.

-spec take(integer(), iter(X)) -> list(X).
take(0, _Iter) ->
    [];
take(N, Iter) when N>0 ->
    case Iter() of
	done ->
	    [];
	{Head, Iter2} ->
	    [Head | take(N-1, Iter2)]
    end.

-spec foreach(fun((X) -> term()), iter(X)) -> ok.
foreach(F, Iter) ->
    case Iter() of
	done ->
	    ok;
	{Head, Iter2} ->
	    F(Head),
	    foreach(F, Iter2)
    end.

-spec push(list(X), iter(X)) -> iter(X).
push([], Iter) ->
    Iter;
push([Elem | Elems], Iter) ->
    fun () ->
	    {Elem, push(Elems, Iter)}
    end.

-type lists(X) :: X | list(lists(X)).

-spec flatten(iter(lists(X))) -> iter(X).
flatten(Iter) ->
    fun () ->
	    case Iter() of
		done ->
		    done;
		{List, Iter2} when is_list(List) ->
		    Push = push(lists:flatten(List), flatten(Iter2)),
		    Push();
		{Elem, Iter2} ->
		    {Elem, flatten(Iter2)}
	    end
    end.

% --- tests ---

prop_list_to_list() ->
    ?FORALL(List, list(),
	    to_list(from_list(List)) == List
	   ).

prop_map() ->
    ?FORALL(List, list(),
	    begin
		F = fun(Elem) -> {Elem, Elem} end,
		to_list(map(F,from_list(List))) == lists:map(F, List)
	    end
	   ).

prop_take() ->
    ?FORALL({List, N}, {list(), non_neg_integer()},
	    take(N, from_list(List)) == lists:sublist(List, 1, N)
	   ).

prop_foreach() ->
    ?FORALL(List, list(),
	    begin
		foreach(
		  fun (Elem) -> 
			  self() ! {prop_foreach, Elem} 
		  end, 
		  from_list(List)),
		lists:foreach(
		  fun (Elem) ->
			  receive 
			      {prop_foreach, Elem2} ->
				  Elem = Elem2
			  after 0 ->
				  error(prop_foreach)
			  end
		  end,
		  List),
		true
	    end
	   ).

prop_flatten() ->
    ?FORALL(List, list(),
	    to_list(flatten(from_list(List))) == lists:flatten(List)
	   ).

prop_flatten2() ->
    ?FORALL(List, list(list()),
	    to_list(flatten(from_list(List))) == lists:flatten(List)
	   ).

% --- end ---
