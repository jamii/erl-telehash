% simple wrappers around error_logger

-module(th_log).

-export([info/1, warn/1, error/1]).

% --- api ---

info(Info) ->
    error_logger:info_report(Info).

warn(Warn) ->
    error_logger:warning_report(Warn).

error(Error) ->
    error_logger:error_report(Error).

% --- end ---
