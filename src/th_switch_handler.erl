% a simple gen_event handler which forwards events to the specified pid

-module(th_switch_handler).

-include("types.hrl").
-include("conf.hrl").
-include("log.hrl").

-behaviour(gen_event).
-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {pid}).

% --- gen_event callbacks ---

init(Pid) ->
    {ok, #state{pid=Pid}}.

handle_call(_Call, State) ->
    {ok, ok, State}.

handle_event(Event, #state{pid=Pid}=State) ->
    Pid ! {switch, Event},
    {ok, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% --- end ---
