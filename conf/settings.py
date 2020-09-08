import os
import sys
import logging
import yaml
import getpass
from subprocess import Popen, PIPE
from datetime import datetime


class Settings:
    """
    A general class used to set configuration options for the runtime.
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
        self.LOG_DIR=os.path.join(os.path.abspath(os.path.join(self.ROOT_DIR, os.pardir)), 'logs')

class LogStream:
        """
    A intermediate implementation of logging framework used to add logging handlers.
    It helps creating diferent log streams for diferent classes or keeps everything together.

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
    settings=Settings()

    def __init__(self, specify_log_path=False, settings=settings):
        self.specify_log_path=specify_log_path
        self.log_dir=settings.LOG_DIR

    def log_stream(self, origin, _log_dir=settings.LOG_DIR):
        log=logging.getLogger(origin)
        log.setLevel(logging.DEBUG)

        if not os.path.isdir(_log_dir):

            os.makedirs(_log_dir)

        handler=logging.FileHandler(os.path.join(_log_dir, '{}_simple_etl_to_local.log'.format(origin, )))
        handler.setLevel(logging.DEBUG)
        formatter=logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        if log.handlers:
            
            log.info('Logging Handler para {} encontrado!'.format(origin))

        else: 

            log.addHandler(handler)
            log.info('Novo logging Handler para {} adicionado!'.format(origin))

        return log