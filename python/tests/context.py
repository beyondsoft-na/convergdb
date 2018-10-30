import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

os.environ["LOCK_TABLE"] = 'tmp'
os.environ["LOCK_ID"] = 'tmp'

import convergdb