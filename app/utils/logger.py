import logging
import inspect


class Logger:
    def __init__(self, log_file='log.log'):
        self.logger = logging.getLogger('StatLogger')
        self.logger.setLevel(logging.INFO)
        self.log_file = log_file
        self.setup_logger()

    def setup_logger(self):
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s | %(filename)s | %(funcName)s')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def log_info(self, message):
        # Get the current stack frame and extract the calling function name
        caller_function = inspect.stack()[1].function
        self.logger.info(f'Function {caller_function} | {message}')

    def log_warning(self, message):
        caller_function = inspect.stack()[1].function
        self.logger.warning(f'Function {caller_function} | {message}')

    def log_error(self, message):
        caller_function = inspect.stack()[1].function
        self.logger.error(f'Function {caller_function} | {message}')
