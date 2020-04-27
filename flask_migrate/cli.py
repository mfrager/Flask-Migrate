import os
import sys
import click
from flask import current_app, g
from flask.cli import with_appcontext
from flask_migrate import init as _init
from flask_migrate import migrate as _migrate
from flask_migrate import upgrade as _upgrade

sys.path.append('/usr/src/app')
from note.table import TableBuilder

@click.group()
def db():
    """Perform database migrations."""
    pass

@db.command()
@click.option('-d', '--directory', default=None,
              help=('migration script directory (default is "migrations")'))
@click.option('-e', '--engine', default='mysql',
                       help=('Specify engine: mysql, postgresql, sqlite (default: mysql)'))
@click.option('-m', '--migrate', 'migrate_flag', is_flag=True)
@click.option('-u', '--upgrade', 'upgrade_flag', is_flag=True)
@click.option('--multidb', is_flag=True,
              help=('Support multiple databases'))
@with_appcontext
def migrate(directory, engine, migrate_flag, upgrade_flag, multidb):
    """Perform a migration."""
    if (migrate_flag and upgrade_flag) or not(migrate_flag or upgrade_flag):
        do_migrate = True
        do_upgrade = True
    elif migrate_flag:
        do_migrate = True
        do_upgrade = False
    elif upgrade_flag:
        do_migrate = False
        do_upgrade = True
    
    path = '/usr/src/app/sql_tables'
    tb = TableBuilder()
    tb.build_sqlalchemy_schema(path, engine=engine)
    g.migrate_metadata = tb.metadata
    g.migrate_url = current_app.config.get('SQLALCHEMY_DATABASE_URI')
    g.migrate_table = None # process all tables (remove extras too)
    if not os.path.isdir(directory):
        _init(directory)
    if do_migrate:
        _migrate(directory)
    if do_upgrade:
        _upgrade(directory)

