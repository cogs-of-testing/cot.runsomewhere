#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["nox>=2025"]
# ///
"""Task runner: ``nox -s tests``, ``nox -s bench``, ``nox -s lint``, ``nox -s docs``."""

import nox

nox.needs_version = ">=2025"
nox.options.default_venv_backend = "uv"

PYTHONS = ["3.10", "3.11", "3.12", "3.13", "3.14"]


@nox.session(python=PYTHONS)
def tests(session: nox.Session) -> None:
    session.run_install(
        "uv",
        "sync",
        "--frozen",
        "--group=test",
        f"--python={session.virtualenv.location}",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    session.run("pytest", *session.posargs)


@nox.session(python=PYTHONS)
def bench(session: nox.Session) -> None:
    session.run_install(
        "uv",
        "sync",
        "--frozen",
        "--group=test",
        "--group=bench",
        f"--python={session.virtualenv.location}",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    session.run("pytest", "benchmarks", "--benchmark-autosave", *session.posargs)


@nox.session
def lint(session: nox.Session) -> None:
    session.install("pre-commit")
    session.run("pre-commit", "run", "--all-files", *session.posargs)


@nox.session
def docs(session: nox.Session) -> None:
    session.run_install(
        "uv",
        "sync",
        "--frozen",
        "--only-group=docs",
        f"--python={session.virtualenv.location}",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    session.run("mkdocs", "build", "--strict", *session.posargs)


if __name__ == "__main__":
    nox.main()
