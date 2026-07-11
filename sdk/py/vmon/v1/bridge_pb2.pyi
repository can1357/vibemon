from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class BridgeFrame(_message.Message):
    __slots__ = ("call", "message", "half_close", "end")
    CALL_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    HALF_CLOSE_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    call: BridgeCall
    message: bytes
    half_close: BridgeHalfClose
    end: BridgeEnd
    def __init__(self, call: _Optional[_Union[BridgeCall, _Mapping]] = ..., message: _Optional[bytes] = ..., half_close: _Optional[_Union[BridgeHalfClose, _Mapping]] = ..., end: _Optional[_Union[BridgeEnd, _Mapping]] = ...) -> None: ...

class BridgeCall(_message.Message):
    __slots__ = ("method", "metadata")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    METHOD_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    method: str
    metadata: _containers.ScalarMap[str, str]
    def __init__(self, method: _Optional[str] = ..., metadata: _Optional[_Mapping[str, str]] = ...) -> None: ...

class BridgeHalfClose(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class BridgeEnd(_message.Message):
    __slots__ = ("status", "message", "trailers")
    class TrailersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    STATUS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    TRAILERS_FIELD_NUMBER: _ClassVar[int]
    status: int
    message: str
    trailers: _containers.ScalarMap[str, str]
    def __init__(self, status: _Optional[int] = ..., message: _Optional[str] = ..., trailers: _Optional[_Mapping[str, str]] = ...) -> None: ...
