%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% %
%% This is a very simple implementation of map-reduce, in both
%% sequential and parallel versions.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% %
-module(map_reduce).
-compile(export_all).
-import(page_rank, [map/2, reduce/2]).
%% We begin with a simple sequential implementation, just to define
%% the semantics of map-reduce.
%% The input is a collection of key-value pairs. The map function maps
%% each key value pair to a list of key-value pairs. The reduce
%% function is then applied to each key and list of corresponding
%% values, and generates in turn a list of key-value pairs. These are
%% the result.

map_reduce_seq(Map,Reduce,Input) ->
    Mapped = [{K2,V2} || {K,V} <- Input,
                         {K2,V2} <- Map(K,V)],
    Results = reduce_seq(Reduce,Mapped),
	file:write_file("results_seq.txt", io_lib:fwrite("~p.\n", [Results])).
    
reduce_seq(Reduce,KVs) ->
    [KV || {K,Vs} <- group(lists:sort(KVs)),
           KV <- Reduce(K,Vs)].
             
group([]) -> [];
group([{K,V}|Rest]) -> group(K,[V],Rest).
    
group(K,Vs,[{K,V}|Rest]) -> group(K,[V|Vs],Rest);
group(K,Vs,Rest) -> [{K,lists:reverse(Vs)}|group(Rest)].

map_reduce_par(Map,M,Reduce,R,Input) ->
    Parent = self(),
    Splits = split_into(M,Input),
    Mappers = [spawn_mapper(Parent,Map,R,Split) || Split <- Splits],
    Mappeds = [receive {Pid,L} -> L end || Pid <- Mappers],
    Reducers = [spawn_reducer(Parent,Reduce,I,Mappeds) || I <- lists:seq(0,R-1)],
    Reduceds = [receive {Pid,L} -> L end || Pid <- Reducers],
    Results = lists:sort(lists:flatten(Reduceds)).

spawn_mapper(Parent,Map,R,Split) ->
    spawn_link(fun() ->
                    Mapped = [{erlang:phash2(K2,R),{K2,V2}}
                                || {K,V} <- Split,
                                    {K2,V2} <- Map (K,V) ],
                    Parent ! {self(),group(lists:sort(Mapped))}
                end).


           
split_into(N,L) -> split_into(N,L,length(L)).
    
split_into(1,L,_) -> [L];
split_into(N,L,Len) -> {Pre,Suf} = lists:split(Len div N,L),
                       [Pre|split_into(N-1,Suf,Len-(Len div N))].
    
spawn_reducer(Parent,Reduce,I,Mappeds) ->
    Inputs = [KV || Mapped <- Mappeds,
                    {J,KVs} <- Mapped,
                    I==J,
                    KV <- KVs],
    spawn_link(fun() -> Parent ! {self(),reduce_seq(Reduce,Inputs)} end).

% Distributed version of map reduce.
map_reduce_dist(Map,M,Reduce,R,Input) ->
    Parent = self(),
    Splits = split_into(M,Input),
    io:fwrite(user,"map_reduce_par_dist: input is split into ~w chunks\n", [length(Splits)]),
    % map
    Mappers = [generate_mapper(Parent,Map,R,Split) || Split <- Splits],
    % open file on all nodes
    [spawn(E, page_rank,open_file,[])|| E <- nodes()],
    % sending list of functions into the worker pool
    Mappeds = worker_pool(Mappers),

    % reduce
    Reducers = [generate_reducer(Parent,Reduce,I,Mappeds) || I <- lists:seq(0,R-1)],
    Reduceds = worker_pool(Reducers),
    Results = lists:sort(lists:flatten(Reduceds)),
	file:write_file("results.txt", io_lib:fwrite("~p.\n", [Results])).

% generate list of reducer functions
generate_reducer(Parent,Reduce,I,Mappeds) ->
    Inputs = [KV || Mapped <- Mappeds,
                    {J,KVs} <- Mapped,
                    I==J,
                    KV <- KVs],
    fun() -> Parent ! {self(),reduce_seq(Reduce,Inputs)} end.

% generate list of mapper functions
generate_mapper(Parent,Map,R,Split) ->
    fun() ->
        Mapped = [{erlang:phash2(K2,R),{K2,V2}}
                    || {K,V} <- Split,
                        {K2,V2} <- Map (K,V) ],
        Parent ! {self(),group(lists:sort(Mapped))}
    end.

% starting point of worker pool
worker_pool(Funs) ->
    io:fwrite("Starting worker pool\n"),
    worker_pool(Funs,nodes() ++ [node()], [], []).

% will go through list of functions and keep all nodes busy with work
worker_pool([H| Funs] ,[N|Nodes], Results, InFlight) ->
    spawn(N,H),
    worker_pool(Funs, Nodes, Results, InFlight ++ [{N,H}]);

% all workers busy. Wait for worker to finish
worker_pool(Funs, [], Results, InFlight) ->
    %io:fwrite("All workers busy\n",[]),
    io:fwrite("Splits remaining: ~w\nActive workers: ~w\n",[length(Funs), length(InFlight)]),
    % if any process finished work. save result and put node back in worker pool
    receive {Pid,L} -> 
        Node = node(Pid),
        %io:fwrite("Received results from node ~w\n",[Node]),
        worker_pool(Funs, [Node], Results ++ [L], [{N,F}||{N,F}<-InFlight, N =/= Node])
    end;

% no more splits to process, all results received
worker_pool([], _, Results, [])->
    io:fwrite("All results received\n"),
    Results;
    
% no more splits to process,
worker_pool([], Nodes, Results, InFlight)  -> 
    io:fwrite("Waiting for processes to finish\n"),
    receive {Pid,L} -> 
        Node = node(Pid),
        worker_pool([], Nodes ++ [Node], Results ++ [L], [{N,F}||{N,F}<-InFlight, N =/= Node])

    % 3000 ms timeout works quite well
    % If timeout is too low, 
    % it can result in infinite timeouts since no process has time to report results
    after 3000 ->
        io:fwrite("Timeout\n"),
        worker_pool([F||{_,F}<-InFlight], Nodes, Results, [])
    end.


