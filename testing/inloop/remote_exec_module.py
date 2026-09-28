"""Sent whole by test_remote_exec; runs on the worker with `channel` bound."""

channel.send("module ran")  # noqa: F821
