% impure lazy lists

-module(iter).

-export([to_list/1, map/2, take/2, foreach/2]).

to_list(Iter) ->
    case Iter() of
	done ->
	    [];
	{Head, Iter2} ->
	    [Head | to_list(Iter2)]
    end.

map(F, Iter) ->
    fun () ->
	    case Iter() of
		done ->
		    done;
		{Head, Iter2} ->
		    {F(Head), map(F, Iter2)}
	    end
    end.

take(0, _Iter) ->
    [];
take(N, Iter) when N>0 ->
    case Iter() of
	done ->
	    [];
	{Head, Iter2} ->
	    [Head | take(N-1, Iter2)]
    end.

foreach(F, Iter) ->
    case Iter() of
	done ->
	    ok;
	{Head, Iter2} ->
	    F(Head),
	    foreach(F, Iter2)
    end.
    
    

