% handles encoding/decoding and manipulating telexes

-module(th_telex).

-include("conf.hrl").
-include("types.hrl").

-export([encode/1, decode/1, get/2, set/3, update/4, end_signal/1, see_command/1, tap_command/1, key_type/1]).

-export([tap_to_json/1, json_to_tap/1]).

-type json_string() :: atom | binary().
-type json_number() :: integer() | float().
-type json_array() :: [json()].
-type json_object() :: {struct, [{json_string(), json()}]}.
-type json() :: json_string() | json_number() | 
		json_array() | json_object().
-type telex() :: json().

-type key_type() :: header | signal | command | normal.

-type path() :: atom() | json_string() | integer() | list(path()).

-export_type([json_string/0, json_number/0, json_array/0, json_object/0, json/0, telex/0, key_type/0, path/0]).

% --- encoding / decoding ---

-spec encode(telex()) -> binary().
encode(Telex) ->
    Json =
	try
	    iolist_to_binary(mochijson2:encode(Telex))
	catch 
	    _:Error ->
		erlang:error({telex, encode, Error, Telex})
	end,
    case byte_size(Json) =< ?TELEX_MAX_BYTES of
	true -> Json;
	false -> erlang:error({telex, encode, too_big, Telex})
    end.

-spec decode(binary()) -> telex().
decode(Json) ->
    case byte_size(Json) =< ?TELEX_MAX_BYTES of
	true -> ok;
	false -> erlang:error({telex, decode, too_big, Json})
    end,
    try
	mochijson2:decode(Json)
    catch 
	_:Error ->
	    erlang:error({telex, decode, Error, Json})
    end.

% --- json accessors ---

-spec get(json(), path()) -> {ok, json()} | {error, not_found}.
get(Json, Path) when is_atom(Path) ->
    get(Json, list_to_binary(atom_to_list(Path)));
get({struct, Json}, Path) when is_list(Json) and is_binary(Path) ->
    case lists:keyfind(Path, 1, Json) of
	false -> {error, not_found};
	{Path, Value} -> {ok, Value}
    end;
get(Json, Index) when is_list(Json) and is_integer(Index) ->
    case Index =< length(Json) of
	false -> {error, not_found};
	true -> {ok, lists:nth(Index, Json)}
    end;
get(Json, Path) when is_list(Path) ->
    get_list(Json, Path);
get(_Json, _Path) ->
    % !!! not too happy about obscuring badarg
    {error, not_found}.

-spec get_list(json(), list(path())) -> {ok, json()} | {error, not_found}.
get_list(Json, []) ->
    {ok, Json};
get_list(Json, [Tuple_elem | Tuple]) ->
    case get(Json, Tuple_elem) of
	{error, not_found} -> {error, not_found};
	{ok, Json2} -> get_list(Json2, Tuple)
    end.

-spec set(json(), path(), json()) -> json().
set(Json, Path, Value) ->
    update(Json, Path, fun (_T) -> Value end, Value).

-spec update(json(), path(), fun((json()) -> json()), json()) -> json().
update(Json, Path, F, Default) when is_atom(Path) ->
    update(Json, list_to_binary(atom_to_list(Path)), F, Default);
update({struct, Json}, Path, F, Default) when is_list(Json) and is_binary(Path) ->
    case lists:keyfind(Path, 1, Json) of
	{Path, Value} -> {struct, lists:keyreplace(Path, 1, Json, {Path, F(Value)})};
	false -> {struct, [{Path, Default} | Json]}
    end;
update(Json, Index, F, _Default) when is_list(Json) and is_integer(Index) ->
    % !!! behaviour for out of range indexes?
    set_nth(Index, Json, F(lists:nth(Index, Json)));
update(Json, Path, F, Default) when is_list(Path) ->
    update_list(Json, Path, F, Default).

-spec update_list(json(), list(path()), fun((json()) -> json()), json()) -> json().
update_list(Json, [], F, _Default) ->
    F(Json);
update_list(Json, [Tuple_elem | Tuple], F, Default) ->
    update(Json, Tuple_elem, fun (T) -> update_list(T, Tuple, F, Default) end, Default).

-spec set_nth(integer(), list(), term()) -> list().
set_nth(1, [_Head | Tail], Value) ->
    [Value | Tail];
set_nth(N, [Head | Tail], Value) when N>1 ->
    [Head | set_nth(N-1, Tail, Value)].

% --- common telexes ---

-spec end_signal('end'()) -> json_object().	
end_signal({'end', _}=End) ->
    {struct, [{'+end', th_util:end_to_hex(End)}]}.

-spec see_command(list(address())) -> json_object().
see_command(Addresses) ->
    {struct, [{'.see', [th_util:address_to_binary(Address) || Address <- Addresses]}]}.

-spec tap_command(tap()) -> json_object().
tap_command(Tap) ->
    {struct, [{'.tap', tap_to_json(Tap)}]}.

-spec key_type(binary()) -> key_type().
key_type(Key) when is_binary(Key) ->
    case Key of
	<< "_", _/binary >> ->
	    header;
	<< "+", _/binary >> ->
	    signal;
	<< ".", _/binary >> ->
	    command;
	_ ->
	    normal
    end.

% --- internal functions ---

% !!! check that keys are all signals, same for json_to_tap
tap_to_json(#tap{subtaps=Subtaps}) ->
    lists:map(
      fun (#subtap{is=Is, has=Has}) ->
	      {struct, [{is, {struct, Is}}, {has, Has}]}
      end,
      Subtaps
     ).

json_to_tap({struct, Json}) ->
    #tap{subtaps = lists:map(fun json_to_subtap/1, Json)}.

json_to_subtap(Json) ->
    Is =
	case th_telex:get(Json, is) of
	    {ok, {struct, Args}} ->
		Args;
	    _ ->
		[]
	end,
    Has =
	case th_telex:get(Json, has) of
	    {ok, Keys} ->
		lists:foreach(fun (Key) -> true = is_binary(Key) end, Keys),
		Keys;
	    _ ->
		[]
	end,
    #subtap{is=Is, has=Has}.

% --- end ---
