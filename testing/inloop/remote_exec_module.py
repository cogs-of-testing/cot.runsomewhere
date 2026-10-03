"""Sent whole by test_remote_exec; runs on the worker with `channel` bound."""

# imported by the test only to be sent; its body runs on the worker alone
if __name__ == "__remote_exec__":
    channel.send("module ran")  # noqa: F821
