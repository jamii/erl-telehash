
-module(telehash_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, brutal_kill, Type, []}).
-define(CHILD(I, Type, Fun), {I, {I, Fun, []}, permanent, brutal_kill, Type, []}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, {{one_for_one, 5, 10},
          [?CHILD(th_event, worker),
           ?CHILD(th_udp, worker),
           ?CHILD(th_router, worker, bootstrap)]}}.
