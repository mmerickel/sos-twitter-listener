from cached_property import cached_property
from contextlib import contextmanager
import logging
from pyaml_env import parse_config
import subparse
import sys

from . import commands

class AbortCLI(Exception):
    def __init__(self, message, code):
        self.message = message
        self.code = code

class App:
    stdin = sys.stdin
    stdout = sys.stdout
    stderr = sys.stderr

    def __init__(self, profile_file):
        self.profile_file = profile_file
        self.dbs = []

    def out(self, msg):
        if not msg.endswith('\n'):
            msg = msg + '\n'
        self.stdout.write(msg)

    def error(self, msg):
        if not msg.endswith('\n'):
            msg = msg + '\n'
        self.stderr.write(msg)

    def abort(self, error, code=1):
        self.error(error)
        raise AbortCLI(error, code)

    @cached_property
    def profile(self):
        return parse_config(self.profile_file, default_value='')

    @contextmanager
    def input_file(self, path, *, text=True):
        if path == '-':
            if text:
                yield sys.stdin
            else:
                yield sys.stdin.buffer
        else:
            mode = 'rb' if not text else 'r'
            with open(path, mode) as fp:
                yield fp

    @contextmanager
    def output_file(self, path, *, text=True):
        if path == '-':
            if text:
                yield sys.stdout
            else:
                yield sys.stdout.buffer
        else:
            mode = 'wb' if not text else 'w'
            with open(path, mode) as fp:
                yield fp

def context_factory(cli, args, with_db=False):
    if getattr(args, 'reload', False):
        import hupper
        reloader = hupper.start_reloader(
            __name__ + '.main',
            shutdown_interval=30,
        )
        reloader.watch_files([args.profile])

    app = App(args.profile)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)-15s %(levelname)-8s [%(name)s] %(message)s',
    )

    yield app

def main(argv=sys.argv):
    cli = subparse.CLI(prog='sos', context_factory=context_factory)
    cli.add_generic_options(commands.generic_options)
    cli.load_commands(commands)
    try:
        return cli.run()
    except AbortCLI as ex:
        return ex.code
