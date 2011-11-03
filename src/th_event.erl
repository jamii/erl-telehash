% a global gen_event which publishes incoming/outgoing telexes

-module(th_event).

-include("conf.hrl").
-include("types.hrl").
-include("log.hrl").

-export([start_link/0, listen/0, deafen/0, notify/1]).

-define(EVENT, th_event).
-define(HANDLER, th_event_handler).

% --- api ---

-spec start_link() -> {ok, pid()}.
start_link() ->
    {ok, _Pid} = gen_event:start_link({local, ?EVENT}).

-spec listen() -> ok.
listen() ->
    ok = gen_event:add_sup_handler(?EVENT, {?HANDLER, self()}, self()).

-spec deafen() -> ok.
deafen() ->
    ok = gen_event:delete_handler(?EVENT, {?HANDLER, self()}, deafen).

-spec notify(term()) -> ok.
notify(Event) ->
    gen_event:notify(?EVENT, Event).

% --- end ---
