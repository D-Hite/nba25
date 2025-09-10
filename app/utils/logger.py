import logging
import inspect


class Logger:
    def __init__(self, log_file='logs/log.log', sql_log_file='logs/sql.log'):
        self.logger = logging.getLogger('StatLogger')
        self.logger.setLevel(logging.INFO)
        self.log_file = log_file

        self.sql_logger = logging.getLogger('SQLLogger')
        self.sql_logger.setLevel(logging.INFO)
        self.sql_log_file = sql_log_file

        self.setup_logger()
        self.setup_sql_logger()

    def setup_logger(self):
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s | %(filename)s | %(funcName)s')
        file_handler.setFormatter(formatter)
        # Prevent adding multiple handlers if already set
        if not self.logger.handlers:
            self.logger.addHandler(file_handler)

    def setup_sql_logger(self):
        file_handler = logging.FileHandler(self.sql_log_file)
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s | SQL | %(message)s')
        file_handler.setFormatter(formatter)
        if not self.sql_logger.handlers:
            self.sql_logger.addHandler(file_handler)

    def log_info(self, message):
        caller_function = inspect.stack()[1].function
        self.logger.info(f'Function {caller_function} | {message}')

    def log_warning(self, message):
        caller_function = inspect.stack()[1].function
        self.logger.warning(f'Function {caller_function} | {message}')

    def log_error(self, message):
        caller_function = inspect.stack()[1].function
        self.logger.error(f'Function {caller_function} | {message}')

    def log_sql(self, message):
        self.sql_logger.info(message)
