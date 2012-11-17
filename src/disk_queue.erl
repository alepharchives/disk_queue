%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%% @copyright
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
-include_lib("disk_queue/include/disk_queue.hrl").

%%%_* Macros ===========================================================
-define(t_ptr, 0).
-define(t_hdr, 1).
-define(size_t_ptr, (1+8)).
-define(size_t_hdr, (1+20+8)).

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(s, { path    = throw(path) :: list()
           , size    = throw(size) :: integer()
           , archive = []          :: [#f{}]
           , active  = undefined   :: #f{}
           , r_ptr   = 0           :: integer()
           , w_ptr   = 0           :: integer()
           }).

-record(f, { name  = throw(name)
           , start = 0
           , fd    = throw(fd)
           , size  = 0
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
    [] -> init_new(Path);
    Fs -> init_old(Path)
  end.

terminate(_Rsn, S) ->
  lists:foreach(fun(#f{fd=Fd}) -> ok = file:close(Fd) end, S#s.files),
  ok.

handle_call(stop) ->
  {stop, normal, ok, S};
handle_call({enqueue, Term}, _From, S) ->
  {Archive, Active, WPtr} = maybe_switch(S#s.archive, S#s.active, Size, WPtr),
  {WPtr, Active}          = do_enqueue(Term, S#s.active, S#s.w_ptr),
  {reply, ok, S#s{archive=Archive, active=Active, w_ptr=WPtr}};
handle_call(dequeue, _From, S) ->
  maybe_switch(),
  case do_dequeue(S#s.archive, S#s.active, S#s.r_ptr) of
    {ok, {Term, RPtr}} ->
      {reply, {ok, Term}, S#s{r_ptr=RPtr}};
    {error, {empty, RPtr}} ->
      {reply, {error, empty}, S#s{r_ptr=RPtr}}
  end.
handle_call(peek, _From, S) ->
  case try_peek(S#s.archive, S#s.active, S#s.r_ptr) of
    {ok, Term} ->
      {reply, {ok, Term}, S};
    {error, empty} ->
      {reply, {error, empty}, S}
  end.

handle_cast(_Msg, S) ->
  {stop, bad_cast, S}.

handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S}.

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals Init --------------------------------------------------
init_new(Path) ->
  Fn = filename(Path, 0),
  {ok, Fd} = file:open(Fn, [write]),
  {ok, #s{ archive = []
         , active  = #f{size=0, fd=Fd}
         , r_ptr   = 0
         , w_ptr   = 0}}.

init_old(Fs) ->
  init_old(Fs, 0, 0).

init_old([F|Fs], R, W) ->
  _ = parse_filename(F),
  case traverse(F) of
    {ok, {short, RPtr, WPtr}} when Fs =:= [] -> ok;
    {ok, {full,  RPtr, WPtr}} -> ok;
    {error, Rsn}              -> {error, Rsn}
  end.

traverse(File) ->
  {ok, FD} = file:open(F, [read, write]),
  traverse(FD, 0, 0).

traverse(Fd, R, W) ->
  case fd_read(Fd, 1) of
    {ok, <<?t_ptr:8/integer>>} ->
      case fd_read(Fd, 8) of
        {ok, <<Ptr:64/integer>>} ->
          traverse(Fd, Ptr, ?size_t_ptr);
        eof ->
          {ok, {short, R, W}}
      end;
    {ok, <<?t_hdr:8/integer>>} ->
      case fd_read(Fd, 28) of
        {ok, <<Hash:20/binary, Size:64/integer>>} ->
          case fd_read(Fd, Size) of
            {ok, Bin} ->
              case crypto:sha(Bin) of
                Hash -> traverse(Fd, R, ?size_t_hdr);
                _    -> {error, hash_failed}
              end;
            eof ->
              {ok, {short, R, W}}
          end;
        eof ->
          {ok, {short, R, W}}
      end;
    eof ->
      {ok, {full, R, W}}
  end.

%%%_ * Internals Enqueue/Dequeue/GC ------------------------------------
do_enqueue(Term, F, WPtr) ->
  Bin   = erlang:term_to_binary(Term),
  Size  = erlang:size(TermBin),
  Hash  = crypto:sha(Bin),
  Hdr   = <<?t_hdr:8/integer, Hash/binary, Size:64/integer>>,
  Entry = <<Hdr/binary, Bin/binary>>,
  ok    = file:pwrite(F#.fd, WPtr-F#f.start, Entry),
  Ez    = erlang:size(Entry),
  {WPtr + Ez, F#f{size=F#f.size+Ez}}.

do_dequeue([], Active, RPtr) ->
  do_dequeue(Active, RPtr);
do_dequeue([F|Fs], Active, RPtr0) ->
  case do_dequeue(F, RPtr0) of
    {ok, {Term, RPtr}}     -> {ok, {Term, RPtr}};
    {error, {empty, RPtr}} -> do_dequeue(Fs, Active, RPtr)
  end.

do_dequeue(#f{start=Start, size=Size}, RPtr)
  when RPtr > Start + Size -> {error, {empty, RPtr}};
do_dequeue(#f{fd=Fd}, RPtr) ->
  case fd_read(Fd, 1) of
    {ok, <<?t_ptr:8/integer>>} ->
      ok;
    {ok, <<?t_hdr:8/integer>>} ->
      ok;
    eof -> {error, {empty, RPtr}}
  end.

read_entry(Fd, Ptr) ->
  case fd_read(Fd, 1, Ptr) of
    {ok, <<?t_ptr:8/integer>>} ->
      case fd_read(Fd, 8, Ptr+1) of
        {ok, <<Ptr:8/integer>>} ->
          {ok, {pointer, Ptr}};
        eof ->
          {error, short}
      end;
    {ok, <<?t_hdr:8/integer>>} ->
      case fd_read(Fd, 28) of
        {ok, <<Hash:20/binary, Size:64/integer>>} ->
          case fd_read(Fd, Size, Ptr+29) of
            {ok, Bin} ->
              case crypto:sha(Bin) of
                Hash -> {ok, {item, Bin}};
                _    -> {error, hash_check_failed}
              end;
            eof ->
              {error, short}
          end;
        eof->
          {error, short}
      end;
    eof ->
      {error, eof}
  end.

maybe_switch(Archive, #f{fd=Fd, size=Fz} = Active, Size)
  when Fz < Size ->
  {Archive, Active};
maybe_switch(Archive, #f{} = F, _Size) ->
  ok          = file:sync(F#f.fd),
  ok          = file:close(F#f.fd),
  NewName     = filename_create(Path, WPtr),
  {ok, OldFd} = file:open(F#f.name, [read]),
  {ok, NewFd} = file:open(NewName, [write]),
  {Archive++[F#{fd=OldFd}], #f{name=NewName, fd=NewFd, start=WPtr}}.

fd_read(FD, Bytes) ->
  case file:read(FD, Bytes) of
    {ok, <<Bin:Bytes/binary>>} -> {ok, Bin};
    {ok, _}                    -> eof;
    eof                        -> eof
  end.

append_item(Term, FD) ->

append_rptr(RPtr, Fd) ->
  Ptr = <<?t_ptr:8/integer, RPtr:64/integer>>,
  ok = file:write(Fd, RPtr).

maybe_switch(Archive, Active) ->
  ok.

%%%_ * Misc ------------------------------------------------------------
ensure_path(Path) ->
  filelib:ensure_dir(filename:join([Dir, "dummy"])),

filename_create(Path, Offset) ->
  Fn = lists:flatten(io_lib:format("dq_~20..0B", [Offset])),
  filename:join([Path, Fn]).

filename_parse(Fn) ->
  "dq_" ++ Offset = filename:basename(Fn),
  erlang:list_to_integer(Offset).

assoc(K, L) ->
  {K,V} = lists:keyfind(K, 1, L),
  V.

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

