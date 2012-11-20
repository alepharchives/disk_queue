%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc A small persistent queue
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
-include_lib("kernel/include/file.hrl").

%%%_* Macros ===========================================================
-define(t_ptr, 0).
-define(t_hdr, 1).

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(f, { name  = undefined
           , start = 0
           , fd    = undefined
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
  ok = file:close(Active#f.fd).

handle_call(stop, _From, S) ->
  {stop, normal, ok, S};
handle_call({enqueue, Term}, _From, S) ->
  {Archive, Active0} =
    maybe_switch(S#s.archive, S#s.active, S#s.path, S#s.size),
  {Active, WPtr} = do_enqueue(Term, Active0, S#s.w_ptr),
  {reply, ok, S#s{archive=Archive, active=Active, w_ptr=WPtr}};
handle_call(dequeue, _From, S) ->
  {Archive0, Active0} =
    maybe_switch(S#s.archive, S#s.active, S#s.path, S#s.size),
  case do_dequeue(Archive0, Active0, S#s.r_ptr, S#s.w_ptr) of
    {ok, {Term, Active, RPtr, WPtr}} ->
      Archive = maybe_gc(Archive0, RPtr),
      {reply, {ok, Term}, S#s{ archive = Archive
                             , active  = Active
                             , r_ptr   = RPtr
                             , w_ptr   = WPtr}};
    {error, empty} ->
      %% possible to move readpointer to writepointer..
      {reply, {error, empty}, S#s{ archive = Archive0
                                 , active  = Active0
                                 , r_ptr   = S#s.w_ptr}}
  end;
handle_call(peek, _From, S) ->
  case do_peek(S#s.archive, S#s.active, S#s.r_ptr) of
    {ok, Term} ->
      {reply, {ok, Term}, S};
    {error, empty} ->
      %% possible to move readpointer to writepointer..
      {reply, {error, empty}, S#s{r_ptr=S#s.w_ptr}}
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
  {ok, Fd} = file:open(Fn, [read, write, binary]),
  {ok, #s{ path    = Path
         , size    = Size
         , archive = []
         , active  = #f{name=Fn, fd=Fd}
         , r_ptr   = 0
         , w_ptr   = 0}}.

init_old(Path, Size, Fs0) ->
  {ArchiveFiles, ActiveFile} = lists:split(erlang:length(Fs0)-1, Fs0),
  Archive = open_archive(ArchiveFiles),
  Active  = open_active(ActiveFile),
  Start   = case Archive of
              []    -> Active#f.start;
              [F|_] -> F#f.start
            end,
  {RPtr, WPtr} = traverse(Archive, Active, Start, Start),
  {ok, #s{ path    = Path
         , size    = Size
         , archive = Archive
         , active  = Active
         , r_ptr   = RPtr
         , w_ptr   = WPtr}}.

open_archive(Fs) ->
  lists:map(fun(F) ->
                {ok, #file_info{size=Size}} = file:read_file_info(F),
                {ok, Fd} = file:open(F, [read, binary]),
                #f{name=F, fd=Fd, start=filename_parse(F), size=Size}
            end, Fs).

open_active(F) ->
  {ok, #file_info{size=Size}} = file:read_file_info(F),
  {ok, Fd} = file:open(F, [read, write, binary]),
  #f{name=F, fd=Fd, start=filename_parse(F), size=Size}.

traverse(Archive, Active, Offset, RPtr) ->
  case read_next(Archive, Active, Offset) of
    {ok, {mark, Ptr, NewOffset}} ->
      traverse(Archive, Active, NewOffset, Ptr);
    {ok, {entry, _Bin, NewOffset}} ->
      traverse(Archive, Active, NewOffset, RPtr);
    {error, empty} ->
      {RPtr, Offset};
    {error, short_read} when Offset >= Active#f.start ->
      %% not the whole entry was written/synced to disk
      %% this is ok if it's the last file
      {RPtr, Offset}
  end.

%%%_ * Internals Enqueue/Dequeue/Peek ----------------------------------
do_enqueue(Term, File, WPtr) ->
  Bin   = erlang:term_to_binary(Term),
  Size  = erlang:size(Bin),
  Hash  = crypto:sha(Bin),
  Hdr   = <<?t_hdr:8/integer, Hash/binary, Size:64/integer>>,
  Entry = <<Hdr/binary, Bin/binary>>,
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
    {ok, {entry, Bin, _NewPtr}} ->
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
  do_read_next(Fd, RPtr-Start, RPtr);
read_next([], #f{fd=Fd, start=Start}, RPtr) ->
  do_read_next(Fd, RPtr-Start, RPtr).

do_read_next(Fd, Start, Offset) ->
  %% lots of tiny reads, rely on os reading larger blocks and cache
  case pread(Fd, Start, 1) of
    {ok, <<?t_ptr:8/integer>>} ->
      case pread(Fd, Start+1, 8) of
        {ok, <<Ptr:64/integer>>} ->
          {ok, {mark, Ptr, Offset+9}};
        eof ->
          {error, short_read}
      end;
    {ok, <<?t_hdr:8/integer>>} ->
      case pread(Fd, Start+1, 28) of
        {ok, <<Hash:20/binary, Size:64/integer>>} ->
          case pread(Fd, Start+29, Size) of
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

maybe_switch(Archive, #f{size=Size} = Active, _Path, MaxSize)
  when Size < MaxSize ->
  {Archive, Active};
maybe_switch(Archive, Active, Path, _Size) ->
  ok          = file:sync(Active#f.fd),
  ok          = file:close(Active#f.fd),
  NewName     = filename_create(Path, Active#f.start+Active#f.size),
  {ok, OldFd} = file:open(Active#f.name, [read, binary]),
  {ok, NewFd} = file:open(NewName, [read, write, binary]),
  {Archive++[Active#f{fd=OldFd}],
   #f{ name  = NewName
     , fd    = NewFd
     , start = Active#f.start+Active#f.size
     }}.

pread(Fd, Offset, Bytes) ->
  case file:pread(Fd, Offset, Bytes) of
    {ok, Bin}
      when erlang:size(Bin) =:= Bytes -> {ok, Bin};
    {ok, _}                           -> eof; %consider short reads eof
    eof                               -> eof
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

clean_dir(Path) ->
  ensure_path(Path),
  lists:foreach(fun(F) ->
                    ok = file:delete(F)
                end,
                filelib:wildcard(filename:join([Path, "dq_*"]))).

basic_test() ->
  clean_dir("tmp"),
  {ok, Ref}      = disk_queue:start_link([{size, 1000}, {path, "tmp"}]),
  ok             = disk_queue:enqueue(Ref, foo),
  ok             = disk_queue:enqueue(Ref, bar),
  {ok, foo}      = disk_queue:dequeue(Ref),
  {ok, bar}      = disk_queue:dequeue(Ref),
  {error, empty} = disk_queue:dequeue(Ref),
  ok             = disk_queue:enqueue(Ref, baz),
  {ok, baz}      = disk_queue:peek(Ref),
  {ok, baz}      = disk_queue:dequeue(Ref),
  {error, empty} = disk_queue:peek(Ref),
  ok             = disk_queue:stop(Ref),
  ok.

persistant_queue_test() ->
  clean_dir("tmp"),
  {ok, Ref1} = disk_queue:start_link([{size, 50}, {path, "tmp"}]),
  ok         = disk_queue:enqueue(Ref1, foo),
  ok         = disk_queue:enqueue(Ref1, bar),
  ok         = disk_queue:enqueue(Ref1, baz),
  {ok, foo}  = disk_queue:dequeue(Ref1),
  ok         = disk_queue:stop(Ref1),
  {ok, Ref2} = disk_queue:start_link([{size, 100}, {path, "tmp"}]),
  ok         = disk_queue:enqueue(Ref2, baz),
  {ok, bar}  = disk_queue:dequeue(Ref2),
  {ok, baz}  = disk_queue:dequeue(Ref2),
  {ok, baz}  = disk_queue:dequeue(Ref2),
  ok         = disk_queue:stop(Ref2),
  ok.

short_read_test() ->
  clean_dir("tmp"),
  {ok, Ref1} = disk_queue:start_link([{size, 50}, {path, "tmp"}]),
  ok = disk_queue:enqueue(Ref1, foo),
  ok = disk_queue:enqueue(Ref1, bar),
  ok = disk_queue:stop(Ref1),
  Files = lists:sort(filelib:wildcard(filename:join(["tmp", "dq_*"]))),
  [Lastfile|_] = lists:reverse(Files),
  {ok, Fd} = file:open(Lastfile, [append, binary]),
  ok = file:write(Fd, <<?t_hdr:8/integer>>),
  ok = file:close(Fd),
  {ok, Ref2} = disk_queue:start_link([{size, 100}, {path, "tmp"}]),
  {ok, foo} = disk_queue:dequeue(Ref2),
  {ok, bar} = disk_queue:dequeue(Ref2),
  {error, empty} = disk_queue:dequeue(Ref2),
  ok = disk_queue:stop(Ref2),
  ok.

-else.
-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:

