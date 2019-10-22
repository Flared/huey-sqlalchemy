__author__ = "Israël Hallé"
__license__ = "LGPL-3.0"
__version__ = "2.1.3"

from functools import partial

from huey import Huey

from .storage import SQLAlchemyStorage

SQLAlchemyHuey = partial(Huey, storage_class=SQLAlchemyStorage)
