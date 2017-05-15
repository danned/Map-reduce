
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%% This implements a page rank algorithm using map-reduce
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%

-module(page_rank).
-compile(export_all).

%% Use map_reduce to count word occurrences
map(Url,ok) ->
    %io:fwrite(user,"url = ~w\n\n\n\n\n",[Url]),
    [{Url,Body}] = dets:lookup(web,Url),
    Urls = crawl:find_urls(Url,Body),
    [{U,1} || U <- Urls].
    
reduce(Url,Ns) ->
    [{Url,lists:sum(Ns)}].
    

page_rank() ->
    dets:open_file(web,[{file,"web.dat"}]),
    Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
    map_reduce:map_reduce_seq(fun map/2, fun reduce/2,
                                    [{Url,ok} || Url <- Urls]).


page_rank_distr() ->
   dets:open_file(web,[{file,"web.dat"}]),
   Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
   map_reduce:map_reduce_par_dist(fun map/2, 64, fun reduce/2, 64,
                                    [{Url,ok} || Url <- Urls]).


benchmark(F) ->
    statistics(runtime),
    statistics(wall_clock),
    F(),
    {_, Time1} = statistics(runtime),
    {_, Time2} = statistics(wall_clock),
    U1 = Time1 * 1000,
    U2 = Time2 * 1000,
    io:format("Code time=~p (~p) microseconds~n",
[U1,U2]).

open_file()->
    io:fwrite(user,"Opening file\n",[]),
    dets:open_file(web,[{file,"web.dat"}]).