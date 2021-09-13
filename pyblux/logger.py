#!/bin/python
# -*- coding: utf-8 -*-
import os, logging
from datetime import datetime, timedelta
from urllib.parse import urlparse
import time 
 
 

class Logger:
    """
    This class connects to a local database session using the db `connnection` library.
    """

    def __init__(self, logname:str, filename:str, level=logging.INFO, console:bool=True):
        """
        Args:
            logname (str): Logger Name.
            filename (str): log file path 
            level (str): Logger Level (DEBUG, INFO, WARNING, ERROR)
            console (cool): print to console
        """
        self._logname = logname
        self._filename = filename
        self._level = level
        self._console = console
    
    def logger(self, verbose:bool=False): 
        directory = os.path.dirname(self._filename)
        if verbose:
            print("File Name -->",self._filename)
            print("Directory --> ",directory)
        if not os.path.exists(directory):
            if verbose:
                print("Directory --> ",directory)
            os.makedirs(directory)
        logger = logging.getLogger(self._logname)
        handler = logging.FileHandler(self._filename)
        formatter = logging.Formatter('%(asctime)s-%(levelname)s-%(module)s: line:%(lineno)s  %(message)s')
        handler.setFormatter(formatter)
        logger.setLevel(self._level)
        logger.addHandler(handler)
        if verbose:
            print("logger.addHandler --> ",logger)
        if self._console:
            console = logging.StreamHandler()
            console.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s-%(levelname)s-%(module)s: line:%(lineno)s  %(message)s')
            console.setFormatter(formatter)
            logger.addHandler(console)
        return logger
 