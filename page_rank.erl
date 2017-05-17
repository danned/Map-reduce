
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%% This implements a page rank algorithm using map-reduce
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%

-module(page_rank).
-compile(export_all).
-import(crawl, [find_urls/2]).
%-import(lists,map/2).

%% Use map_reduce to count word occurrences
map(Url,ok) ->
    %io:fwrite(user,"url = ~w\n\n\n\n\n",[Url]),
    [{Url,Body}] = dets:lookup(web,Url),
    Urls = crawl:find_urls(Url,Body),
    [{U,1} || U <- Urls].
    
reduce(Url,Ns) ->
    [{Url,lists:sum(Ns)}].
    

page_rank(_,_) ->
    dets:open_file(web,[{file,"web.dat"}]),
    Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
    map_reduce:map_reduce_seq(fun map/2, fun reduce/2,
                                    [{Url,ok} || Url <- Urls]).


page_rank_distr(M,R) ->
   dets:open_file(web,[{file,"web.dat"}]),
   Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
   map_reduce:map_reduce_dist(fun map/2, M, fun reduce/2, R,
                                    [{Url,ok} || Url <- Urls]).

% Takes input argument a 2-ary funtion, and the arguments
benchmark(F,M,R) ->
    statistics(runtime),
    statistics(wall_clock),
    F(M,R),
    {_, Time1} = statistics(runtime),
    {_, Time2} = statistics(wall_clock),
    U1 = Time1 * 1000,
    U2 = Time2 * 1000,
    io:format("Code time=~p (~p) microseconds~n",
[U1,U2]).

% All nodes needs to open file before participating in worker pool
open_file()->
    io:fwrite(user,"Opening file\n",[]),
    R = dets:open_file(web,[{file,"web.dat"}]),
    io:fwrite("~w\n",[R]).