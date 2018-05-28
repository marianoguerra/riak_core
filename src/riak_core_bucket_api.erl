-module(riak_core_bucket_api).
-export([init/0, sup_children/0, ring_changed/2]).
-export([get_bucket/1, get_bucket/2,
         get_bucket_type/2, get_bucket_type/3,
         get_bucket_type_status/1,
         get_bucket_attr/3, get_bucket_default_attr/2,
         bucket_type_put/2, merge_props/2]).

-include("riak_core_bucket_type.hrl").

-type property() :: {PropName::atom(), PropValue::any()}.
-type properties() :: [property()].

-type bucket_type()       :: binary().
-type bucket() :: binary() | {bucket_type(), binary()}.

-export_type([bucket/0]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Timeout, Args), {I, {I, start_link, Args}, permanent, Timeout, Type, [I]}).
-define(CHILD(I, Type, Timeout), ?CHILD(I, Type, Timeout, [])).
-define(CHILD(I, Type), ?CHILD(I, Type, 5000)).

init() ->
    %% add these defaults now to supplement the set that may have been
    %% configured in app.config
    riak_core_bucket:append_bucket_defaults(riak_core_bucket_type:defaults(default_type)),
    ok.

sup_children() ->
    [?CHILD(riak_core_metadata_evt_sup, supervisor),
     ?CHILD(riak_core_metadata_manager, worker),
     ?CHILD(riak_core_metadata_hashtree, worker),
     ?CHILD(riak_core_broadcast, worker),
     ?CHILD(riak_core_gossip, worker)].

ring_changed(Node, CState) ->
    riak_core_claimant:ring_changed(Node, CState).

get_bucket(Bucket) ->
    riak_core_bucket:get_bucket(Bucket).

get_bucket(Bucket, Ring) ->
    riak_core_bucket:get_bucket(Bucket, Ring).

merge_props(Overriding, Other) ->
    riak_core_bucket_props:merge(Overriding, Other).

get_bucket_attr(Bucket, Attr, DefaultValue) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    proplists:get_value(Attr, BucketProps, DefaultValue).

get_bucket_default_attr(Attr, DefaultValue) ->
    Defaults = app_helper:get_env(riak_core, default_bucket_props),
    case get_value(Attr, Defaults) of
        undefined -> DefaultValue;
        Value -> Value
    end.

%% @private
-spec get_bucket_type_status(BucketType::binary()) ->
    {AllProps::[any()], BadNodes::[node()]}.
get_bucket_type_status(BucketType) ->
    rpc:multicall(all_members(),
                  riak_core_metadata,
                  get, [?BUCKET_TYPE_PREFIX, BucketType, [{default, []}]]).

%% @doc Lookup the properties for `BucketType'. If there are no properties or
%% the type is inactive, the given `Default' value is returned.
-spec get_bucket_type(bucket_type(), X) -> [{atom(), any()}] | X.
get_bucket_type(BucketType, Default) ->
    get_bucket_type(BucketType, Default, true).

%% @doc Lookup the properties for `BucketType'. If there are no properties or
%% the type is inactive and `RequireActive' is `true', the given `Default' value is
%% returned.
-spec get_bucket_type(bucket_type(), X, boolean()) ->
                             [{atom(), any()}] | X.
get_bucket_type(BucketType, Default, RequireActive) ->
    %% we resolve w/ last-write-wins because conflicts only occur
    %% during creation when the claimant is changed and create on a
    %% new claimant happens before the original propogates. In this
    %% case we want the newest create. Updates can also result in
    %% conflicts so we choose the most recent as well.
    case riak_core_metadata:get(?BUCKET_TYPE_PREFIX, BucketType,
                                [{default, Default}]) of
        Default -> Default;
        Props -> maybe_filter_inactive_type(RequireActive, Default, Props)
    end.

bucket_type_put(BucketType, Props) ->
    riak_core_metadata:put(?BUCKET_TYPE_PREFIX, BucketType, Props).

% a slighly faster version of proplists:get_value
-spec get_value(atom(), properties()) -> any().
get_value(Key, Proplist) ->
    case lists:keyfind(Key, 1, Proplist) of
        {Key, Value} -> Value;
        _ -> undefined
    end.

all_members() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:all_members(Ring).

maybe_filter_inactive_type(false, _Default, Props) ->
    Props;
maybe_filter_inactive_type(true, Default, Props) ->
    case type_active(Props) of
        true -> Props;
        false -> Default
    end.

%% @private
type_active(Props) ->
    {active, true} =:= lists:keyfind(active, 1, Props).
