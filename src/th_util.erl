% stuff that doesnt fit anywhere else

-module(th_util).

-include("conf.hrl").
-include("types.hrl").

-export([address_to_binary/1, binary_to_address/1, binary_to_end/1, hex_to_end/1, end_to_hex/1, to_end/1, random_end/0, random_end/1, distance/2, to_bits/1]).
-export([tap_to_json/1, json_to_tap/1]).
-export([ensure_started/1, set_nth/3]).

% --- api --- 

-spec address_to_binary(address()) -> binary().
address_to_binary(#address{host={A,B,C,D}, port=Port}) ->
    iolist_to_binary(io_lib:format("~B.~B.~B.~B:~w", [A, B, C, D, Port])).

-spec binary_to_address(binary()) -> address().
binary_to_address(Binary) when is_binary(Binary) ->
    {ok, [A, B, C, D, Port], ""} = io_lib:fread("~u.~u.~u.~u:~u", binary_to_list(Binary)), 
    #address{host={A,B,C,D}, port=Port}.

-spec binary_to_end(binary()) -> 'end'().
binary_to_end(String) ->
    ok = ensure_started(crypto),
    {'end', crypto:sha(String)}.

-spec end_to_hex('end'()) -> binary().
end_to_hex({'end', End}) when is_binary(End) ->
    iolist_to_binary([io_lib:format("~2.16.0b", [Byte]) || Byte <- binary_to_list(End)]).

-spec hex_to_end(binary()) -> 'end'().
hex_to_end(Hex) when is_binary(Hex) ->
    {'end', iolist_to_binary([Char - 48 || Char <- binary_to_list(Hex)])}.

-spec to_end(address() | 'end'()) -> 'end'(). 
to_end(#address{}=Address) ->
    binary_to_end(address_to_binary(Address));
to_end({'end', _} = End) ->
    End.

-spec distance(address() | 'end'(), address() | 'end'()) -> integer().
distance(A, B) ->
    {'end', EndA} = to_end(A),
    {'end', EndB} = to_end(B),
    Bytes = lists:zip(binary_to_list(EndA), binary_to_list(EndB)),
    Xor = list_to_binary([ByteA bxor ByteB || {ByteA, ByteB} <- Bytes]),
    <<Dist:?END_BITS>> = Xor,
    Dist.

-spec bit(boolean()) -> integer().
bit(false) ->
    0;
bit(true) ->
    1.

-spec random_end() -> 'end'().	
random_end() ->
    {'end', crypto:rand_bytes(?END_BITS div 8)}.

-spec random_end(binary()) -> 'end'().
random_end(Prefix) when is_list(Prefix) ->
    random_end(<< <<(bit(B)):1>> || B <- Prefix >>);
random_end(Prefix) when is_bitstring(Prefix) ->
    Gap = (8 - (bit_size(Prefix) rem 8)) rem 8,
    << Bits:Gap, _/bitstring >> = crypto:rand_bytes(1),
    Bytes = crypto:rand_bytes((?END_BITS div 8) - byte_size(Prefix)),
    {'end', << Prefix/bitstring , Bits:Gap, Bytes/binary >>}.

-spec to_bits(address() | 'end'() | binary()) -> bits().
to_bits(#address{}=Address) ->
    to_bits(to_end(Address));
to_bits({'end', End}) ->
    to_bits(End);
to_bits(<<>>) ->
    [];
to_bits(Bin) when is_bitstring(Bin) ->
    [(Bit>0) || <<Bit:1>> <= Bin].

-spec ensure_started(atom()) -> ok.
ensure_started(Module) ->
    case Module:start() of
	ok -> ok;
	{error, {already_started, Module}} -> ok
    end.

-spec set_nth(integer(), list(), term()) -> list().
set_nth(1, [_Head | Tail], Value) ->
    [Value | Tail];
set_nth(N, [Head | Tail], Value) when N>1 ->
    [Head | set_nth(N-1, Tail, Value)].

tap_to_json(#tap{subtaps=Subtaps}) ->
    lists:map(fun subtap_to_json/1, Subtaps).

subtap_to_json(#subtap{is=Is, has=Has}) ->
    {struct, [ {<<"is">>, {struct, Is}}, {<<"has">>, Has} ]}.

json_to_tap(Json) ->
    #tap{subtaps = lists:map(fun json_to_subtap/1, Json)}.

json_to_subtap({struct, Json}) ->
    {struct, Is} = proplists:get_value(<<"is">>, Json, {struct, []}),
    Has = proplists:get_value(<<"has">>, Json, []),
    #subtap{is=Is, has=Has}.

% --- end ---
