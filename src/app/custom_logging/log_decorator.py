import logging
import functools
import inspect
import traceback


# Automatically print error logs with stacktrace
# Logger is available inside class as cls._logger


class LogDecorator(object):
    def __call__(self, fn):
        @functools.wraps(fn)
        def decorated(*args, **kwargs):
            try:
                self.logger = logging.getLogger(fn.__name__)
                self.logger.debug(f"Inside {fn.__name__} - {args}")
                result = fn(*args, **kwargs)
                return result
            except Exception as e:
                self.logger.error(f'Error occurred in function {fn.__name__}')
                self.logger.error(f'Exception {traceback.format_exc()}')
                result = None
            return result
        return decorated


def log_decorate_class(cls):
    for name, method in inspect.getmembers(cls, inspect.ismethod):
        setattr(cls, name, LogDecorator().__call__(method))
    setattr(cls,'_logger', logging.getLogger(cls.__name__))
    return cls
