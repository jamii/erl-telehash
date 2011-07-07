% pure(ish) lazy lists

-module(th_iter).

-export([to_list/1, map/2, take/2, foreach/2, flatten/1]).

-type iter(X) :: fun(() -> done | {X, iter(X)}).
-export_types([iter/1]).

-spec to_list(iter(X)) -> list(X). 
to_list(Iter) ->
    case Iter() of
	done ->
	    [];
	{Head, Iter2} ->
	    [Head | to_list(Iter2)]
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
		    {Elem, Iter2}
	    end
    end.
