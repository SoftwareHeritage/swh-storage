from os import path
import swh.storage


SQL_DIR = path.join(path.dirname(swh.storage.__file__), 'sql')
