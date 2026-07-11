from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Stream(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STREAM_UNSPECIFIED: _ClassVar[Stream]
    STREAM_STDOUT: _ClassVar[Stream]
    STREAM_STDERR: _ClassVar[Stream]
    STREAM_CONSOLE: _ClassVar[Stream]
STREAM_UNSPECIFIED: Stream
STREAM_STDOUT: Stream
STREAM_STDERR: Stream
STREAM_CONSOLE: Stream

class JsonView(_message.Message):
    __slots__ = ("json",)
    JSON_FIELD_NUMBER: _ClassVar[int]
    json: str
    def __init__(self, json: _Optional[str] = ...) -> None: ...

class Ok(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SandboxRef(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class CreateSandboxRequest(_message.Message):
    __slots__ = ("spec_json",)
    SPEC_JSON_FIELD_NUMBER: _ClassVar[int]
    spec_json: str
    def __init__(self, spec_json: _Optional[str] = ...) -> None: ...

class ListSandboxesRequest(_message.Message):
    __slots__ = ("tags",)
    TAGS_FIELD_NUMBER: _ClassVar[int]
    tags: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, tags: _Optional[_Iterable[str]] = ...) -> None: ...

class ListSandboxesResponse(_message.Message):
    __slots__ = ("sandboxes_json",)
    SANDBOXES_JSON_FIELD_NUMBER: _ClassVar[int]
    sandboxes_json: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, sandboxes_json: _Optional[_Iterable[str]] = ...) -> None: ...

class StopSandboxRequest(_message.Message):
    __slots__ = ("id", "returncode")
    ID_FIELD_NUMBER: _ClassVar[int]
    RETURNCODE_FIELD_NUMBER: _ClassVar[int]
    id: str
    returncode: int
    def __init__(self, id: _Optional[str] = ..., returncode: _Optional[int] = ...) -> None: ...

class ExtendSandboxRequest(_message.Message):
    __slots__ = ("id", "secs")
    ID_FIELD_NUMBER: _ClassVar[int]
    SECS_FIELD_NUMBER: _ClassVar[int]
    id: str
    secs: int
    def __init__(self, id: _Optional[str] = ..., secs: _Optional[int] = ...) -> None: ...

class LogsRequest(_message.Message):
    __slots__ = ("id", "follow", "tail")
    ID_FIELD_NUMBER: _ClassVar[int]
    FOLLOW_FIELD_NUMBER: _ClassVar[int]
    TAIL_FIELD_NUMBER: _ClassVar[int]
    id: str
    follow: bool
    tail: int
    def __init__(self, id: _Optional[str] = ..., follow: _Optional[bool] = ..., tail: _Optional[int] = ...) -> None: ...

class LogChunk(_message.Message):
    __slots__ = ("data",)
    DATA_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    def __init__(self, data: _Optional[bytes] = ...) -> None: ...

class ExecStart(_message.Message):
    __slots__ = ("cmd", "workdir", "env", "timeout", "tty", "sandbox_id")
    class EnvEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    CMD_FIELD_NUMBER: _ClassVar[int]
    WORKDIR_FIELD_NUMBER: _ClassVar[int]
    ENV_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    TTY_FIELD_NUMBER: _ClassVar[int]
    SANDBOX_ID_FIELD_NUMBER: _ClassVar[int]
    cmd: _containers.RepeatedScalarFieldContainer[str]
    workdir: str
    env: _containers.ScalarMap[str, str]
    timeout: float
    tty: bool
    sandbox_id: str
    def __init__(self, cmd: _Optional[_Iterable[str]] = ..., workdir: _Optional[str] = ..., env: _Optional[_Mapping[str, str]] = ..., timeout: _Optional[float] = ..., tty: _Optional[bool] = ..., sandbox_id: _Optional[str] = ...) -> None: ...

class ExecCaptureRequest(_message.Message):
    __slots__ = ("id", "exec")
    ID_FIELD_NUMBER: _ClassVar[int]
    EXEC_FIELD_NUMBER: _ClassVar[int]
    id: str
    exec: ExecStart
    def __init__(self, id: _Optional[str] = ..., exec: _Optional[_Union[ExecStart, _Mapping]] = ...) -> None: ...

class ExecCaptureResponse(_message.Message):
    __slots__ = ("code", "signal", "stdout", "stderr")
    CODE_FIELD_NUMBER: _ClassVar[int]
    SIGNAL_FIELD_NUMBER: _ClassVar[int]
    STDOUT_FIELD_NUMBER: _ClassVar[int]
    STDERR_FIELD_NUMBER: _ClassVar[int]
    code: int
    signal: int
    stdout: bytes
    stderr: bytes
    def __init__(self, code: _Optional[int] = ..., signal: _Optional[int] = ..., stdout: _Optional[bytes] = ..., stderr: _Optional[bytes] = ...) -> None: ...

class ExecInput(_message.Message):
    __slots__ = ("start", "shell_params_json", "stdin", "eof", "resize")
    START_FIELD_NUMBER: _ClassVar[int]
    SHELL_PARAMS_JSON_FIELD_NUMBER: _ClassVar[int]
    STDIN_FIELD_NUMBER: _ClassVar[int]
    EOF_FIELD_NUMBER: _ClassVar[int]
    RESIZE_FIELD_NUMBER: _ClassVar[int]
    start: ExecStart
    shell_params_json: str
    stdin: bytes
    eof: Eof
    resize: Resize
    def __init__(self, start: _Optional[_Union[ExecStart, _Mapping]] = ..., shell_params_json: _Optional[str] = ..., stdin: _Optional[bytes] = ..., eof: _Optional[_Union[Eof, _Mapping]] = ..., resize: _Optional[_Union[Resize, _Mapping]] = ...) -> None: ...

class ExecOutput(_message.Message):
    __slots__ = ("chunk", "exit", "ready")
    CHUNK_FIELD_NUMBER: _ClassVar[int]
    EXIT_FIELD_NUMBER: _ClassVar[int]
    READY_FIELD_NUMBER: _ClassVar[int]
    chunk: Output
    exit: Exit
    ready: Ready
    def __init__(self, chunk: _Optional[_Union[Output, _Mapping]] = ..., exit: _Optional[_Union[Exit, _Mapping]] = ..., ready: _Optional[_Union[Ready, _Mapping]] = ...) -> None: ...

class FilePathRequest(_message.Message):
    __slots__ = ("id", "path")
    ID_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    id: str
    path: str
    def __init__(self, id: _Optional[str] = ..., path: _Optional[str] = ...) -> None: ...

class FileContent(_message.Message):
    __slots__ = ("data",)
    DATA_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    def __init__(self, data: _Optional[bytes] = ...) -> None: ...

class FileWriteRequest(_message.Message):
    __slots__ = ("id", "path", "data")
    ID_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    id: str
    path: str
    data: bytes
    def __init__(self, id: _Optional[str] = ..., path: _Optional[str] = ..., data: _Optional[bytes] = ...) -> None: ...

class FileDeleteRequest(_message.Message):
    __slots__ = ("id", "path", "recursive")
    ID_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    RECURSIVE_FIELD_NUMBER: _ClassVar[int]
    id: str
    path: str
    recursive: bool
    def __init__(self, id: _Optional[str] = ..., path: _Optional[str] = ..., recursive: _Optional[bool] = ...) -> None: ...

class StringList(_message.Message):
    __slots__ = ("values",)
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, values: _Optional[_Iterable[str]] = ...) -> None: ...

class NetworkSetRequest(_message.Message):
    __slots__ = ("id", "block_network", "cidr_allow", "domain_allow")
    ID_FIELD_NUMBER: _ClassVar[int]
    BLOCK_NETWORK_FIELD_NUMBER: _ClassVar[int]
    CIDR_ALLOW_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_ALLOW_FIELD_NUMBER: _ClassVar[int]
    id: str
    block_network: bool
    cidr_allow: StringList
    domain_allow: StringList
    def __init__(self, id: _Optional[str] = ..., block_network: _Optional[bool] = ..., cidr_allow: _Optional[_Union[StringList, _Mapping]] = ..., domain_allow: _Optional[_Union[StringList, _Mapping]] = ...) -> None: ...

class MigrateRequest(_message.Message):
    __slots__ = ("id", "target")
    ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    id: str
    target: str
    def __init__(self, id: _Optional[str] = ..., target: _Optional[str] = ...) -> None: ...

class SnapshotRequest(_message.Message):
    __slots__ = ("id", "name", "stop")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    STOP_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    stop: bool
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., stop: _Optional[bool] = ...) -> None: ...

class SnapshotFsRequest(_message.Message):
    __slots__ = ("id", "name")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class ListSnapshotsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SnapshotList(_message.Message):
    __slots__ = ("snapshots",)
    SNAPSHOTS_FIELD_NUMBER: _ClassVar[int]
    snapshots: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, snapshots: _Optional[_Iterable[str]] = ...) -> None: ...

class RestoreSnapshotRequest(_message.Message):
    __slots__ = ("name", "body_json")
    NAME_FIELD_NUMBER: _ClassVar[int]
    BODY_JSON_FIELD_NUMBER: _ClassVar[int]
    name: str
    body_json: str
    def __init__(self, name: _Optional[str] = ..., body_json: _Optional[str] = ...) -> None: ...

class ForkSnapshotRequest(_message.Message):
    __slots__ = ("name", "body_json")
    NAME_FIELD_NUMBER: _ClassVar[int]
    BODY_JSON_FIELD_NUMBER: _ClassVar[int]
    name: str
    body_json: str
    def __init__(self, name: _Optional[str] = ..., body_json: _Optional[str] = ...) -> None: ...

class ListVolumesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class VolumeList(_message.Message):
    __slots__ = ("volumes",)
    VOLUMES_FIELD_NUMBER: _ClassVar[int]
    volumes: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, volumes: _Optional[_Iterable[str]] = ...) -> None: ...

class VolumeRef(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class ListPoolsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class PoolSetRequest(_message.Message):
    __slots__ = ("reference", "body_json")
    REFERENCE_FIELD_NUMBER: _ClassVar[int]
    BODY_JSON_FIELD_NUMBER: _ClassVar[int]
    reference: str
    body_json: str
    def __init__(self, reference: _Optional[str] = ..., body_json: _Optional[str] = ...) -> None: ...

class PoolRef(_message.Message):
    __slots__ = ("reference",)
    REFERENCE_FIELD_NUMBER: _ClassVar[int]
    reference: str
    def __init__(self, reference: _Optional[str] = ...) -> None: ...

class InfoRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class MeshStatusRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class EventsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class Eof(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class Resize(_message.Message):
    __slots__ = ("rows", "cols")
    ROWS_FIELD_NUMBER: _ClassVar[int]
    COLS_FIELD_NUMBER: _ClassVar[int]
    rows: int
    cols: int
    def __init__(self, rows: _Optional[int] = ..., cols: _Optional[int] = ...) -> None: ...

class Output(_message.Message):
    __slots__ = ("stream", "data")
    STREAM_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    stream: Stream
    data: bytes
    def __init__(self, stream: _Optional[_Union[Stream, str]] = ..., data: _Optional[bytes] = ...) -> None: ...

class Exit(_message.Message):
    __slots__ = ("code", "signal")
    CODE_FIELD_NUMBER: _ClassVar[int]
    SIGNAL_FIELD_NUMBER: _ClassVar[int]
    code: int
    signal: int
    def __init__(self, code: _Optional[int] = ..., signal: _Optional[int] = ...) -> None: ...

class Ready(_message.Message):
    __slots__ = ("sandbox_id",)
    SANDBOX_ID_FIELD_NUMBER: _ClassVar[int]
    sandbox_id: str
    def __init__(self, sandbox_id: _Optional[str] = ...) -> None: ...
