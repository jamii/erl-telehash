% simple wrappers around error_logger

-module(log).

-export([info/1, warn/1, error/1]).

% --- api ---

info(Info) ->
    error_logger:info_report([{pid, self()} | Info]).

warn(Warn) ->
    error_logger:warning_report([{pid, self()} | Warn]).

error(Error) ->
    error_logger:error_report([{pid, self()} | Error]).

% --- end ---
