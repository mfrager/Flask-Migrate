import argparse
from functools import wraps
import logging
import os
import sys
from flask import current_app, g
try:
    from flask_script import Manager
except ImportError:
    Manager = None
from alembic import __version__ as __alembic_version__
from alembic.config import Config as AlembicConfig
from alembic import command
from alembic.util import CommandError

alembic_version = tuple([int(v) for v in __alembic_version__.split('.')[0:3]])
log = logging.getLogger()


class _MigrateConfig(object):
    def __init__(self, migrate, db, **kwargs):
        self.migrate = migrate
        self.db = db
        self.directory = migrate.directory
        self.configure_args = kwargs

    @property
    def metadata(self):
        """
        Backwards compatibility, in old releases app.extensions['migrate']
        was set to db, and env.py accessed app.extensions['migrate'].metadata
        """
        return self.db.metadata


class Config(AlembicConfig):
    def get_template_directory(self):
        package_dir = os.path.abspath(os.path.dirname(__file__))
        return os.path.join(package_dir, 'templates')


class Migrate(object):
    def __init__(self, app=None, db=None, directory='migrations', **kwargs):
        self.configure_callbacks = []
        self.db = db
        self.directory = directory
        self.alembic_ctx_kwargs = kwargs
        if app is not None and db is not None:
            self.init_app(app, db, directory)

    def init_app(self, app, db=None, directory=None, **kwargs):
        self.db = db or self.db
        self.directory = directory or self.directory
        self.alembic_ctx_kwargs.update(kwargs)
        if not hasattr(app, 'extensions'):
            app.extensions = {}
        app.extensions['migrate'] = _MigrateConfig(self, self.db,
            **self.alembic_ctx_kwargs)

    def configure(self, f):
        self.configure_callbacks.append(f)
        return f

    def call_configure_callbacks(self, config):
        for f in self.configure_callbacks:
            config = f(config)
        return config

    def get_config(self, directory=None, x_arg=None, opts=None):
        if directory is None:
            directory = self.directory
        config = Config(os.path.join(directory, 'alembic.ini'))
        config.set_main_option('script_location', directory)
        if config.cmd_opts is None:
            config.cmd_opts = argparse.Namespace()
        for opt in opts or []:
            setattr(config.cmd_opts, opt, True)
        if not hasattr(config.cmd_opts, 'x'):
            if x_arg is not None:
                setattr(config.cmd_opts, 'x', [])
                if isinstance(x_arg, list) or isinstance(x_arg, tuple):
                    for x in x_arg:
                        config.cmd_opts.x.append(x)
                else:
                    config.cmd_opts.x.append(x_arg)
            else:
                setattr(config.cmd_opts, 'x', None)
        return self.call_configure_callbacks(config)


def catch_errors(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            f(*args, **kwargs)
        except (CommandError, RuntimeError) as exc:
            log.error('Error: ' + str(exc))
            sys.exit(1)
    return wrapped


if Manager is not None:
    MigrateCommand = Manager(usage='Perform database migrations')
else:
    class FakeCommand(object):
        def option(self, *args, **kwargs):
            def decorator(f):
                return f
            return decorator

    MigrateCommand = FakeCommand()

@MigrateCommand.option('-e', '--engine', dest='engine', default='mysql',
                       help=('Specify engine: mysql, postgresql, sqlite (default: mysql)'))
@MigrateCommand.option('-d', '--directory', dest='directory', default=None,
                       help=("migration script directory (default is "
                             "'migrations')"))
@catch_errors
def update(directory=None, engine='mysql'):
    """Perform a migration."""
    path = '/usr/src/app/sql_tables'
    tb = TableBuilder()
    tb.build_sqlalchemy_schema(self, path, engine=engine)
    g.migrate_url = current_app.config.get('SQLALCHEMY_DATABASE_URI')
    g.migrate_table = None # process all tables (remove extras too)
    g.migrate_metadata = tb.metadata
    if directory is None:
        directory = current_app.extensions['migrate'].directory
    if not os.path.isdir(directory):
        init(directory=directory)
    migrate(directory=directory)
    upgrade(directory=directory)

def init(directory=None, multidb=False):
    """Creates a new migration repository"""
    if directory is None:
        directory = current_app.extensions['migrate'].directory
    config = Config()
    config.set_main_option('script_location', directory)
    config.config_file_name = os.path.join(directory, 'alembic.ini')
    config = current_app.extensions['migrate'].\
        migrate.call_configure_callbacks(config)
    if multidb:
        command.init(config, directory, 'flask-multidb')
    else:
        command.init(config, directory, 'flask')

def migrate(directory=None, message=None, sql=False, head='head', splice=False,
            branch_label=None, version_path=None, rev_id=None, x_arg=None):
    """Alias for 'revision --autogenerate'"""
    config = current_app.extensions['migrate'].migrate.get_config(
        directory, opts=['autogenerate'], x_arg=x_arg)
    if alembic_version >= (0, 7, 0):
        command.revision(config, message, autogenerate=True, sql=sql,
                         head=head, splice=splice, branch_label=branch_label,
                         version_path=version_path, rev_id=rev_id)
    else:
        command.revision(config, message, autogenerate=True, sql=sql)

def upgrade(directory=None, revision='head', sql=False, tag=None, x_arg=None):
    """Upgrade to a later version"""
    config = current_app.extensions['migrate'].migrate.get_config(directory,
                                                                  x_arg=x_arg)
    command.upgrade(config, revision, sql=sql, tag=tag)


