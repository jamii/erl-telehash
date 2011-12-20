-module(telehash).

-export([start/0]).

start() ->
    ensure(crypto),
    ok = application:start(telehash).

ensure(App) ->
    case application:start(App) of
        ok -> ok;
        {error, {already_started, App}} -> ok
    end.
