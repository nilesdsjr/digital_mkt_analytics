import os
import logging
import yaml


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
    CVV_CONFIG_PATH : yaml
        a config file to load external parameters.
    Methods
    -------
    
    """
    def __init__(self):

        self.ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
        self.LOCAL_PATHS = {
            'CONFIG_PATH': os.path.join(os.path.abspath(os.path.join(self.ROOT_DIR, os.pardir)), 'resources', 'conf'),
            'YAML_CONFIG_PATH': os.path.join(os.path.abspath(os.path.join(self.ROOT_DIR, os.pardir)),'resources', 'conf', 'sel.yaml'),
            'LOG_DIR': os.path.join(os.path.abspath(os.path.join(self.ROOT_DIR, os.pardir)), 'logs'),
            'DATA_DIR': os.path.join(os.path.abspath(os.path.join(self.ROOT_DIR, os.pardir)), 'data')
        }


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
    def __init__(self):
        pass

    def log_stream(self, origin, _log_dir=settings.LOCAL_PATHS['LOG_DIR']):

        log=logging.getLogger(origin)
        log.setLevel(logging.INFO)
        if not os.path.isdir(_log_dir):
            os.makedirs(_log_dir)
        handler=logging.FileHandler(os.path.join(_log_dir, 'All_base_reclamacao.log'))
        handler.setLevel(logging.INFO)
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
    def __init__(self):
        _logstream = LogStream()
        self.log = _logstream.log_stream(origin=__class__.__name__)

    def load_config(self, fl_config_path=settings.LOCAL_PATHS['YAML_CONFIG_PATH']):

        try:

            with open(fl_config_path, 'rb') as yml:
                config = yaml.safe_load(yml)
        except IOError as e:
            self.log.error('yaml file load has failed. Check it at {}'.format(
                      fl_config_path), exc_info=True)
            raise(e)
        self.log.debug('yaml file loaded and configurations found.')
        return config
