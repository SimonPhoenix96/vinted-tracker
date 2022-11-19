import os

# celery config is in a non-standard location & can be updated dynamically now
os.environ['CELERY_CONFIG_MODULE'] = 'vinted-tracker.config.celeryconfig'

# try:
    # Python 3.8+
    # import importlib.metadata as importlib_metadata
# except ImportError:
    # <Python 3.7 and lower
    # import importlib_metadata
