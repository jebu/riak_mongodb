{application, riak_mongodb,
 [
  {description, "A MongoDB for Riak"},
  {vsn, "1.0"},
  {modules, [
             riak_mongodb_app,
             riak_mongodb_sup,
             riak_kv_mongodb_backend
            ]},
  {registered, []},
  {applications, [
                  kernel,
                  stdlib
                 ]},
  {mod, { riak_mongodb_app, []}},
  {env, []}
 ]}.
