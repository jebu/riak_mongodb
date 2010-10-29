%% -------------------------------------------------------------------
%%
%% riak_kv_mongodb_backend: MongoDB Driver for Riak
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% This is fully based on riak_kv_bitcask_backend
%% 
%% -------------------------------------------------------------------

-module(riak_kv_mongodb_backend).
-behavior(riak_kv_backend).
-author('Jebu Ittiachen <jebu@iyottasoft.com>').

-export([start/2,
         stop/1,
         get/2,
         put/3,
         delete/2,
         list/1,
         list_bucket/2,
         fold/3,
         drop/1,
         is_empty/1,
         callback/3]).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
-record(state, {partition, pool}).

start(Part, _Config) ->
  application:start(emongo),
  Partition = integer_to_list(Part),
  Pool = list_to_atom("pool_" ++ Partition),
  emongo:add_pool(Pool, "localhost", 27017, Partition, 10),
  {ok, #state{pool = Pool, partition = Partition}}.

stop(_State) ->
    ok.

get(#state{pool = Pool}, {Bucket, Key}) ->
  case emongo:find_one(Pool, binary_to_list(Bucket), [{<<"_id">>, Key}], [{fields, [<<"value">>]}]) of
    [] -> {error, notfound};
    [Val] when is_list(Val) -> 
      {ok, proplists:get_value(<<"value">>, Val)};
    _ ->
      {error, notfound}
  end.

put(#state{pool = Pool}, {Bucket, Key}, Value) ->
  Document = [{<<"_id">>, Key}, {<<"value">>, Value}, {<<"date">>, get_timestamp()}],
  emongo:update(Pool, binary_to_list(Bucket), [{<<"_id">>, Key}], Document, true),
  ok.

delete(#state {pool = Pool }, {Bucket, Key}) ->
  emongo:delete(Pool, binary_to_list(Bucket), [{<<"_id">>, Key}]),
  ok.

list(#state {partition = Partition, pool = Pool}) ->
  Collections = emongo:find_all(Pool, "system.namespaces"),
  PartLength = length(Partition),
  PartBinary = list_to_binary(Partition),
  
  CollKeys = 
    lists:map(fun
      ([{<<"name">>, <<_:PartLength/binary, ".system.indexes">>}]) ->
        [];
      ([{<<"name">>, <<PartBinary1:PartLength/binary, $., CollectionName/binary>>}]) when PartBinary1 == PartBinary ->
        KeysFun = fun(Doc, Acc) ->
          case proplists:get_value(<<"_id">>, Doc) of
            undefined -> Acc;
            Key -> [{CollectionName, Key} | Acc]
          end
        end,
        emongo:fold_all(KeysFun, [], Pool, binary_to_list(CollectionName), [], [{fields, [<<"_id">>]}]);
      (_) ->
        []
    end, Collections),
  lists:append(CollKeys).

list_bucket(#state {partition = Partition, pool = Pool}, '_')->
  PartLength = length(Partition),
  Collections = emongo:find_all(Pool, "system.namespaces"),
  lists:foldl(fun
    ([{<<"name">>, <<_:PartLength/binary, ".system.indexes">>}], Acc) ->
      Acc;
    ([{<<"name">>, <<_:PartLength/binary, $., CollectionName/binary>>}], Acc) ->
      [CollectionName | Acc];
    (_, Acc) ->
      Acc
  end, [], Collections);

list_bucket(State, {filter, Bucket, Fun})->
  lists:filter(Fun, list_bucket(State, Bucket));
list_bucket(#state {pool = Pool }, Bucket) ->
  KeysFun = fun(Doc, Acc) ->
    case proplists:get_value(<<"_id">>, Doc) of
      undefined -> Acc;
      Key -> [Key | Acc]
    end
  end,
  emongo:fold_all(KeysFun, [], Pool, binary_to_list(Bucket), [], []).

fold(#state {partition=Partition, pool = Pool}, Fun0, Acc0) ->
  Collections = emongo:find_all(Pool, "system.namespaces"),
  PartLength = length(Partition),
  
  lists:foldl(fun
    ([{<<"name">>, <<_:PartLength/binary, ".system.indexes">>}], Acc1) ->
      Acc1;
    ([{<<"name">>, <<_:PartLength/binary, $., CollectionName/binary>>}], Acc1) ->
      KeysFun = fun(Doc, Acc2) ->
        case {proplists:get_value(<<"_id">>, Doc), proplists:get_value(<<"value">>, Doc)} of
          {undefined, undefined} -> Acc2;
          {Key, Value} -> Fun0({CollectionName, Key}, Value, Acc2)
        end
      end,
      emongo:fold_all(KeysFun, Acc1, Pool, binary_to_list(CollectionName), [], []);
    (_, Acc1) ->
      Acc1
  end, Acc0, Collections).

drop(_State) ->
  ok.

is_empty(#state{partition = Partition, pool = Pool}) ->
  PartLength = length(Partition),
  Collections = emongo:find_all(Pool, "system.namespaces"),
  
  TotalCount = 
    lists:foldl(fun
      ([{<<"name">>, <<_:PartLength/binary, ".system.indexes">>}], Acc) ->
        Acc;
      ([{<<"name">>, <<_:PartLength/binary, $., CollectionName/binary>>}], Acc) ->
        CollLength = size(CollectionName) - 6,
        case CollectionName of
          <<_:CollLength/binary, ".$_id_">> -> Acc;
          _ -> emongo:count(Pool, CollectionName) + Acc
        end;
      (_, Acc) ->
        Acc
    end, 0, Collections),
  case TotalCount of
    0 -> true;
    _ -> false
  end.
%% Ignore callbacks for other backends so multi backend works
callback(_State, _Ref, _Msg) ->
    ok.

%% ===================================================================
%% Internal functions
%% ===================================================================
get_env(App, ConfParam, Default) ->
  case application:get_env(App, ConfParam) of
    {ok, Val} -> Val;
    _ -> Default
  end.
%
get_timestamp() ->
  {M, S, _} = erlang:now(),
  M * 1000000 + S.
%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).
standard_test(BackendMod, Config) ->
    {ok, S} = BackendMod:start(42, Config),
    ?assertEqual(ok, BackendMod:put(S,{<<"b1">>,<<"k1">>},<<"v1">>)),
    ?assertEqual(ok, BackendMod:put(S,{<<"b2">>,<<"k2">>},<<"v2">>)),
    ?assertEqual({ok,<<"v2">>}, BackendMod:get(S,{<<"b2">>,<<"k2">>})),
    ?assertEqual({error, notfound}, BackendMod:get(S, {<<"b1">>,<<"k3">>})),
    ?assertEqual([{<<"b1">>,<<"k1">>},{<<"b2">>,<<"k2">>}],
                 lists:sort(BackendMod:list(S))),
    ?assertEqual([<<"k2">>], BackendMod:list_bucket(S, <<"b2">>)),
    ?assertEqual([<<"k1">>], BackendMod:list_bucket(S, <<"b1">>)),
    ?assertEqual([<<"k1">>], BackendMod:list_bucket(
                               S, {filter, <<"b1">>, fun(_K) -> true end})),
    ?assertEqual([], BackendMod:list_bucket(
                       S, {filter, <<"b1">>, fun(_K) -> false end})),
    BucketList = BackendMod:list_bucket(S, '_'),
    ?assert(lists:member(<<"b1">>, BucketList)),
    ?assert(lists:member(<<"b2">>, BucketList)),
    ?assertEqual(ok, BackendMod:delete(S,{<<"b2">>,<<"k2">>})),
    ?assertEqual({error, notfound}, BackendMod:get(S, {<<"b2">>, <<"k2">>})),
    ?assertEqual([{<<"b1">>, <<"k1">>}], BackendMod:list(S)),
    Folder = fun(K, V, A) -> [{K,V}|A] end,
    ?assertEqual([{{<<"b1">>,<<"k1">>},<<"v1">>}], BackendMod:fold(S, Folder, [])),
    ?assertEqual(ok, BackendMod:put(S,{<<"b3">>,<<"k3">>},<<"v3">>)),
    ?assertEqual([{{<<"b1">>,<<"k1">>},<<"v1">>},
                  {{<<"b3">>,<<"k3">>},<<"v3">>}], lists:sort(BackendMod:fold(S, Folder, []))),
    ?assertEqual(false, BackendMod:is_empty(S)),
    ?assertEqual(ok, BackendMod:delete(S,{<<"b1">>,<<"k1">>})),
    ?assertEqual(ok, BackendMod:delete(S,{<<"b3">>,<<"k3">>})),
    ?assertEqual(true, BackendMod:is_empty(S)),
    ok = BackendMod:stop(S).

simple_test() ->
    application:load(riak_mongodb),
    standard_test(?MODULE, []).

-endif.
