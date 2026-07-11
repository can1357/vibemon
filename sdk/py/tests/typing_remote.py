"""Static consumer contract for typed durable functions."""

from collections.abc import AsyncIterator, Iterator
from typing import assert_type

from vmon import App, Client, FunctionCall, function  # pyright: ignore


def plain_add(left: int, right: int = 1) -> int:
    return left + right


add = function(plain_add)


@function
def numbers(limit: int) -> Iterator[int]:
    yield from range(limit)


@function
async def async_add(value: int) -> int:
    return value + 1


@function
async def async_numbers(limit: int) -> AsyncIterator[int]:
    for value in range(limit):
        yield value


_add_local = assert_type(add.local(1), int)
_add_remote = assert_type(add.remote(1), int)
_add_call = assert_type(add.spawn(1), FunctionCall[int])

_numbers_local = assert_type(numbers.local(2), Iterator[int])
_numbers_remote = assert_type(numbers.remote_gen(2), Iterator[int])
_numbers_call = assert_type(numbers.spawn(2), FunctionCall[int])
_numbers_async = assert_type(numbers.aio.remote_gen(2), AsyncIterator[int])

_async_numbers_local = assert_type(async_numbers.local(2), AsyncIterator[int])
_async_numbers_remote = assert_type(async_numbers.remote_gen(2), Iterator[int])
_async_numbers_async = assert_type(async_numbers.aio.remote_gen(2), AsyncIterator[int])

app = App("typing")


@app.function
def app_numbers(limit: int) -> Iterator[int]:
    yield from range(limit)


_app_numbers_local = assert_type(app_numbers.local(2), Iterator[int])
_app_numbers_remote = assert_type(app_numbers.remote_gen(2), Iterator[int])


async def async_contract() -> None:
    _add_async = assert_type(await add.aio.remote(1), int)
    _async_add_local = assert_type(await async_add.local(1), int)
    _async_add_remote = assert_type(await async_add.aio.remote(1), int)


def client_contract(client: Client) -> None:
    bound = client.function(plain_add)
    _bound_local = assert_type(bound.local(1), int)
    _bound_call = assert_type(bound.spawn(1), FunctionCall[int])
