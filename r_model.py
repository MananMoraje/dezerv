import os
try:
    import rpy2.robjects as robjects

except ImportError or ModuleNotFoundError:
    os.system('pip install rpy2')
    import rpy2.robjects as robjects

import sys

r = robjects.r
r.source('model.r')

r_func = robjects.globalenv['sum_columns']

r_func(sys.argv[1], sys.argv[2], sys.argv[3])
