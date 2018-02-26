import os
import sys
import logging
import argparse


if __name__ == '__main__':
    # Options
    parser = argparse.ArgumentParser(usage='python -m images7')

    parser.add_argument(
        '-c', '--config',
        default=os.getenv('IMAGES6_CONFIG', 'images.ini'),
        help='specify what config file to run on')

    parser.add_argument(
        '-g', '--debug', action='store_true',
        help='show debug messages')

    parser.add_argument(
        'command', nargs='*', default=['serve'],
        help='command to run')

    args = parser.parse_args()

    # Logging
    FORMAT = '%(asctime)s [%(threadName)s] %(filename)s +%(levelno)s ' + \
             '%(funcName)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        level=logging.DEBUG if args.debug else logging.INFO,
    )

    # Load modules
    from .config import Config
    from .system import System

    from . import web
    from . import entry
    from . import files
    #from . import date
    #from . import tag
    from . import importer
    #from . import deleter
    #from . import publish
    #from . import debug

    from . import job
    from .job import (
        register,
        transcode,
        to_cut,
        calculate_hash,
        read_metadata,
        to_main,
        create_proxy,
        clean_cut,
    )
    from .analyse import exif
    from .job.transcode import imageproxy

    # Config
    logging.info("*** Reading config from %s.", args.config)
    config = Config(args.config)
    system = System(config)
    logging.info("*** Done setting up Database.")

    command, args = args.command[0], args.command[1:]
    if command == 'serve':

        # Apps
        logging.info("*** Setting up apps...")
        app = web.App.create()
        system.close_hooks.append(lambda: app.close())
        for module in (
            entry,
            files,
            #date,
            #tag,
            importer,
            #deleter,
            #publish,
            job,
            #debug,
        ):
            logging.info(
                "Setting up %s on %s..." % (module.__name__, module.App.BASE)
            )
            app.mount(module.App.BASE, module.App.create())
            if hasattr(module.App, 'run'):
                logging.info(
                    "Setting up %s backend..." % (module.__name__)
                )
                module.App.run(workers=system.server.workers)
        logging.info("*** Done setting up apps.")

        # Serve the Web-App
        app.run(
            host=system.http.interface,
            port=system.http.port,
            server=system.http.adapter,
            debug=system.http.debug,
        )
    else:

        logging.error('unknown command "%s"', command)
        sys.exit(-1)
