import os
import sys
import logging
import yaml
import getpass
from subprocess import Popen, PIPE
from datetime import datetime


class Settings:
    """
    A general class used to set path options for the runtime.
    It is used to access attributes dinamically through its object.

    ...

    Attributes
    ----------
    ROOT_DIR : str
        a formatted string specifying the path to the root's project. 
        It's determined by the main class' file location.
    CONFIG_PATH : str
        a formatted string specifying the path to the project's conf directory.
        Used to locate stored files like .yaml, .cfg, .ini, etc.
    LOG_DIR : str
        a formatted string specifying the path to the project's log directory.
        Stores the log file.

    Methods
    -------
    says(sound=None)
        Prints the animals name and what sound it makes
    """
    def __init__(self, root_file=__file__):
        self.ROOT_DIR=os.path.dirname(os.path.abspath(root_file))
        self.CONFIG_PATH=os.path.join(os.path.abspath(os.path.join(self.ROOT_DIR, os.pardir)), 'conf')
        self.PG_CONFIG_PATH=os.path.join(os.path.abspath(os.path.join(self.ROOT_DIR, os.pardir)), 'conf', 'postgres.yaml')
        self.LOG_DIR=os.path.join(os.path.abspath(os.path.join(self.ROOT_DIR, os.pardir)), 'logs')


class LogStream:
    """
    A intermediate implementation of logging framework used to add logging handlers.
    It helps creating diferent log streams for diferent classes or keeps everything together.

    ...

    Attributes
    ----------
    settings : Settings object
        A general class used to set path options for the runtime.
    LOG_DIR : str
        a formatted string specifying the path to the project's log directory.
        Stores the log file.

    Methods
    -------
    log_stream(self, origin, _log_dir=settings.LOG_DIR)
        Sets a logger instance with a loaded handler by caller class.
    """
    settings=Settings()

    def __init__(self, settings=settings):

        self.log_dir=settings.LOG_DIR

    def log_stream(self, origin, _log_dir=settings.LOG_DIR):
        log=logging.getLogger(origin)
        log.setLevel(logging.DEBUG)

        if not os.path.isdir(_log_dir):

            os.makedirs(_log_dir)

        handler=logging.FileHandler(os.path.join(_log_dir, 'All_simple_etl_to_local.log'))
        handler.setLevel(logging.DEBUG)
        formatter=logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        if log.handlers:

            log.debug('Logging Handler para {} encontrado!'.format(origin))

        else:

            log.addHandler(handler)
            log.debug('Novo logging Handler para {} adicionado!'.format(origin))

        return log


class Configuration:
    """
    Specific class do load config files to a python object.

    ...

    Attributes
    ----------
    config : dict
        a dict containing all config info declared on the .yaml file.
    _logstream : logger object
        a cofigurable logger object to be loaded with the caller class parameters.
    log : logger instance
        a formatted string specifying the path to the project's log directory.

    Methods
    -------
    load_config(settings=settings)
        loads .yaml file content as dict.
    """
    settings=Settings()

    def load_config(self, settings=settings):
        _logstream = LogStream()
        log = _logstream.log_stream(origin=__class__.__name__)
        try:

            with open(settings.PG_CONFIG_PATH, 'rb') as yml:

                config = yaml.safe_load(yml)

        except IOError as e:

            log.error('Carregamento de arquivo yaml falhou. Confira em ' +
                      settings.PG_CONFIG_PATH,
                      exec_info=True)

            raise(e)

        log.info('Arquivo de configuration carregado.')

        return config