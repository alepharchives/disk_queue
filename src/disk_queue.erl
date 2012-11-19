%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%% @copyright Bjorn Jensen-Urstad 2012
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(disk_queue).
-behaviour(gen_server).

%%%_* Exports ==========================================================
-export([ start_link/1
        , stop/1
        , enqueue/2
        , dequeue/1
        , peek/1
        ]).

-export([ init/1
        , terminate/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , code_change/3
        ]).

%%%_* Includes =========================================================
%% -include_lib("disk_queue/include/disk_queue.hrl").

%%%_* Macros ===========================================================
-define(t_ptr, 0).
-define(t_hdr, 1).
-define(size_t_ptr, (1+8)).
-define(size_t_hdr, (1+20+8)).

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(f, { name  = throw(name)
           , start = 0
           , fd    = throw(fd)
           , size  = 0
           }).

-record(s, { path    = throw(path) :: list()
           , size    = throw(size) :: integer()
           , archive = []          :: [#f{}]
           , active  = undefined   :: #f{}
           , r_ptr   = 0           :: integer()
           , w_ptr   = 0           :: integer()
           }).

%%%_ * API -------------------------------------------------------------
%% start_link(Args) -> {ok, Ref} | {error, Rsn}
start_link(Args) ->
  gen_server:start_link(?MODULE, Args, []).

%% stop(Ref) -> ok
stop(Ref) ->
  gen_server:call(Ref, stop).

%% enqueue(Ref, Term) -> ok
enqueue(Ref, Term) ->
  gen_server:call(Ref, {enqueue, Term}).

%% dequeue(Ref) -> {ok, Term} | {error, empty}
dequeue(Ref) ->
  gen_server:call(Ref, dequeue).

%% peek(Ref) -> {ok, Term} | {error, empty}
peek(Ref) ->
  gen_server:call(Ref, peek).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  {ok, Path} = assoc(path, Args),
  {ok, Size} = assoc(size, Args),
  ensure_path(Path),
  case lists:sort(filelib:wildcard(filename:join([Path, "dq_*"]))) of
    [] -> init_new(Path, Size);
    Fs -> init_old(Path, Size, Fs)
  end.

terminate(_Rsn, #s{archive=Archive, active=Active}) ->
  lists:foreach(fun(#f{fd=Fd}) -> ok = file:close(Fd) end, Archive),
  ok = file:close(Active#f.fd),
  ok.

handle_call(stop, _From, S) ->
  {stop, normal, ok, S};
handle_call({enqueue, Term}, _From, #s{} = S) ->
  {Archive, Active0} = maybe_switch(S#s.archive, S#s.active, S#s.path, S#s.size),
  {Active, WPtr}     = do_enqueue(Term, Active0, S#s.w_ptr),
  {reply, ok, S#s{archive=Archive, active=Active, w_ptr=WPtr}};
handle_call(dequeue, _From, S) ->
  {Archive0, Active0} = maybe_switch(S#s.archive, S#s.active, S#s.path, S#s.size),
  case do_dequeue(Archive0, Active0, S#s.r_ptr, S#s.w_ptr) of
    {ok, {Term, Active, RPtr, WPtr}} ->
      Archive = maybe_gc(Archive0, RPtr),
      {reply, {ok, Term}, S#s{archive = Archive,
                              active  = Active,
                              r_ptr   = RPtr,
                              w_ptr   = WPtr}};
    {error, empty} ->
      {reply, {error, empty}, S#s{archive = Archive0,
                                  active  = Active0}}
  end;
handle_call(peek, _From, S) ->
  case do_peek(S#s.archive, S#s.active, S#s.r_ptr) of
    {ok, Term} ->
      {reply, {ok, Term}, S};
    {error, empty} ->
      {reply, {error, empty}, S}
  end.

handle_cast(_Msg, S) ->
  {stop, bad_cast, S}.

handle_info(Msg, S) ->
  %% ?warning("~p", [Msg]),
  {noreply, S}.

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals Init --------------------------------------------------
init_new(Path, Size) ->
  Fn = filename_create(Path, 0),
  io:format("FN: ~p~n", [Fn]),
  {ok, Fd} = file:open(Fn, [read, write, binary]),
  {ok, #s{ path    = Path
         , size    = Size
         , archive = []
         , active  = #f{name=Fn, fd=Fd}
         , r_ptr   = 0
         , w_ptr   = 0}}.

%% init_old(Path, Size, Fs) ->
  %% init_old(Fs, 0, 0).

init_old([F|Fs], R, W) ->
  %% _ = parse_filename(F),
  case traverse(F) of
    {ok, {short, RPtr, WPtr}} when Fs =:= [] -> ok;
    {ok, {full,  RPtr, WPtr}} -> ok;
    {error, Rsn}              -> {error, Rsn}
  end.

traverse(File) ->
  %% {ok, FD} = file:open(F, [read, write]),
  %% traverse(FD, 0, 0).
  ok.

%%%_ * Internals Enqueue/Dequeue/Peek ----------------------------------
do_enqueue(Term, File, WPtr) ->
  Bin   = erlang:term_to_binary(Term),
  Size  = erlang:size(Bin),
  Hash  = crypto:sha(Bin),
  Hdr   = <<?t_hdr:8/integer, Hash/binary, Size:64/integer>>,
  Entry = <<Hdr/binary, Bin/binary>>,
  io:format("SIZE: ~p~n", [erlang:size(Entry)]),

  ok    = file:pwrite(File#f.fd, WPtr-File#f.start, Entry),
  Ez    = erlang:size(Entry),
  {File#f{size=File#f.size+Ez}, WPtr+Ez}.

do_dequeue(Archive, Active, RPtr0, WPtr) ->
  case read_next(Archive, Active, RPtr0) of
    {ok, {mark, _RPtr, RPtr}} ->
      do_dequeue(Archive, Active, RPtr, WPtr);
    {ok, {entry, Bin, RPtr}} ->
      Mark = <<?t_ptr:8/integer, RPtr:64/integer>>,
      ok = file:pwrite(Active#f.fd, WPtr-Active#f.start, Mark),
      {ok, {erlang:binary_to_term(Bin),
            Active#f{size=Active#f.size+9}, RPtr, WPtr+9}};
    {error, empty} ->
      {error, empty}
  end.

do_peek(Archive, Active, RPtr) ->
  case read_next(Archive, Active, RPtr) of
    {ok, {mark, _Ptr, NewPtr}} ->
      do_peek(Archive, Active, NewPtr);
    {ok, {entry, Bin, NewPtr}} ->
      {ok, erlang:binary_to_term(Bin)};
    {error, empty} ->
      {error, empty}
  end.

%%%_ * Internals Misc/GC -----------------------------------------------
maybe_gc([#f{start=Start,size=Size}=F|Fs], RPtr)
  when Start+Size =< RPtr ->
  ok = file:close(F#f.fd),
  ok = file:delete(F#f.name),
  maybe_gc(Fs, RPtr);
maybe_gc(Archive, _RPtr) ->
  Archive.

read_next([], #f{start=Start, size=Size}, RPtr)
  when Start+Size =:= RPtr -> {error, empty};
read_next([#f{start=Start, size=Size}|Fs], Active, RPtr)
  when Start+Size =< RPtr -> read_next(Fs, Active, RPtr);
read_next([#f{fd=Fd, start=Start}|_], _Active, RPtr) ->
  read_next(Fd, RPtr-Start);
read_next([], #f{fd=Fd, start=Start}, RPtr) ->
  read_next(Fd, RPtr-Start).

read_next(Fd, Offset) ->
  case fd_read(Fd, Offset, 1) of
    {ok, <<?t_ptr:8/integer>>} ->
      case fd_read(Fd, Offset+1, 8) of
        {ok, <<Ptr:64/integer>>} ->
          {ok, {mark, Ptr, Offset+9}};
        eof ->
          {error, short_read}
      end;
    {ok, <<?t_hdr:8/integer>>} ->
      case fd_read(Fd, Offset+1, 28) of
        {ok, <<Hash:20/binary, Size:64/integer>>} ->
          case fd_read(Fd, Offset+29, Size) of
            {ok, Bin} ->
              case crypto:sha(Bin) of
                Hash -> {ok, {entry, Bin, Offset+29+Size}};
                _    -> {error, hash_check_failed}
              end;
            eof ->
              {error, short_read}
          end;
        eof->
          {error, short_read}
      end;
    eof ->
      {error, empty}
  end.

maybe_switch(Archive, #f{fd=Fd, size=Fz} = Active, _Path, Size)
  when Fz < Size ->
  {Archive, Active};
maybe_switch(Archive, F, Path, _Size) ->
  ok          = file:sync(F#f.fd),
  ok          = file:close(F#f.fd),
  NewName     = filename_create(Path, F#f.start+F#f.size),
  {ok, OldFd} = file:open(F#f.name, [read, binary]),
  {ok, NewFd} = file:open(NewName, [read, write, binary]),
  {Archive++[F#f{fd=OldFd}], #f{name  = NewName,
                                fd    = NewFd,
                                start = F#f.start+F#f.size
                               }}.

fd_read(Fd, Offset, Bytes) ->
  io:format("FD: ~p~nOffset: ~p~nBytes: ~p~n", [Fd,Offset,Bytes]),
  case file:pread(Fd, Offset, Bytes) of
    {ok, <<Bin:Bytes/binary>>} -> {ok, Bin};
    {ok, _}                    -> eof;
    eof                        -> eof
  end.

%%%_ * Misc ------------------------------------------------------------
ensure_path(Path) ->
  filelib:ensure_dir(filename:join([Path, "dummy"])).

filename_create(Path, Offset) ->
  Fn = lists:flatten(io_lib:format("dq_~20..0B", [Offset])),
  filename:join([Path, Fn]).

filename_parse(Fn) ->
  "dq_" ++ Offset = filename:basename(Fn),
  erlang:list_to_integer(Offset).

assoc(K, L) ->
  case lists:keyfind(K, 1, L) of
    {K, V} -> {ok, V};
    false  -> {error, notfound}
  end.

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

basic_test() ->
  {ok, Ref}      = disk_queue:start_link([{size, 10000}, {path, "tmp"}]),
  ok             = disk_queue:enqueue(Ref, foo),
  ok             = disk_queue:enqueue(Ref, bar),
  {ok, foo}      = disk_queue:dequeue(Ref),
  {ok, bar}      = disk_queue:dequeue(Ref),
  {error, empty} = disk_queue:dequeue(Ref),
  ok             = disk_queue:enqueue(Ref, baz),
  {ok, baz}      = disk_queue:peek(Ref),
  {ok, baz}      = disk_queue:dequeue(Ref),
  ok             = disk_queue:stop(Ref),
  ok.

persistant_queue_test() ->
  {ok, Ref1} = disk_queue:start_link([{size, 100}, {path, "tmp"}]),
  ok         = disk_queue:enqueue(Ref1, foo),
  ok         = disk_queue:enqueue(Ref1, bar),
  ok         = disk_queue:stop(Ref1),
  {ok, Ref2} = disk_queue:start_link([{size, 100}, {path, "tmp"}]),
  ok         = disk_queue:enqueue(Ref2, baz),
  {ok, foo}  = disk_queue:dequeue(Ref2),
  {ok, bar}  = disk_queue:dequeue(Ref2),
  {ok, baz}  = disk_queue:dequeue(Ref2),
  ok         = disk_queue:stop(Ref2),
  ok.

-else.
-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:

