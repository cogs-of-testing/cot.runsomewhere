"""Services the runsomewhere test suite reaches through entry points."""

from __future__ import annotations

import os
import sys
import time

import anyio

from cot import runsomewhere as rsh


async def echo(channel):
    async for item in channel:
        await channel.send(item)


async def total(channel):
    # input ends at a None item; add is the same service ended by close_send
    result = 0
    while (item := await channel.receive()) is not None:
        result += item
    return result


async def add(channel):
    return sum([item async for item in channel])


def sync_add(channel):
    return sum(channel)


async def ask(channel, *, question):
    await channel.send(question)
    channel.close_send()
    return await channel.receive()


async def take(channel, *, count, delay=0):
    # the delay lets a caller finish sending before items go missing
    await anyio.sleep(delay)
    return [await channel.receive() for _ in range(count)]


async def fail(channel, *, message):
    raise ValueError(message)


async def produce(channel, *, count, size):
    for index in range(count):
        await channel.send(bytes([index % 256]) * size)
    return count


def env(channel, *, name):
    return os.environ.get(name)


def noisy_echo(channel):
    # the protocol must survive the worker's own stdout and stderr
    for item in channel:
        print("noise on stdout", item)
        print("noise on stderr", item, file=sys.stderr)
        channel.send(item)


def crash(channel, *, code):
    os._exit(code)


def stubborn(channel):
    # ignores its closed channel, so only killing the worker ends it
    while True:
        time.sleep(0.1)


class Echo(rsh.Client, service="rsh_test_services.echo"):
    async def roundtrip(self, item):
        await self.channel.send(item)
        return await self.channel.receive()
