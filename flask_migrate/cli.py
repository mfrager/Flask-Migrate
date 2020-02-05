import os
import sys
import click
from flask import current_app, g
from flask.cli import with_appcontext
from flask_migrate import init as _init
from flask_migrate import migrate as _migrate
from flask_migrate import upgrade as _upgrade

sys.path.append('/usr/src/app')
from note.database import tb

@click.group()
def db():
    """Perform database migrations."""
    pass

@db.command()
@click.option('-d', '--directory', default=None,
              help=('migration script directory (default is "migrations")'))
@click.option('-e', '--engine', default='mysql',
                       help=('Specify engine: mysql, postgresql, sqlite (default: mysql)'))
@click.option('-m', '--migrate', default=True)
@click.option('-u', '--upgrade', default=True)
@click.option('--multidb', is_flag=True,
              help=('Support multiple databases'))
@with_appcontext
def migrate(directory, engine, do_migrate, do_upgrade, multidb):
    """Perform a migration."""
    flask_dir = current_app.root_path
    path = os.path.join(flask_dir, 'sql_tables')
    g.migrate_metadata = tb.metadata
    g.migrate_url = current_app.config.get('SQLALCHEMY_DATABASE_URI')
    g.migrate_table = None # process all tables (remove extras too)
    if not os.path.isdir(directory):
        _init(directory)
    if do_migrate:
        _migrate(directory)
    if do_update:
        _upgrade(directory)

