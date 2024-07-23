from flask import Flask, request, render_template, send_file
import rpy2.robjects as robjects
from rpy2.robjects.conversion import localconverter
from rpy2.robjects import pandas2ri

app = Flask(__name__)
r = robjects.r
r.source('model.r')

with localconverter(robjects.default_converter + pandas2ri.converter):
    r_func = robjects.globalenv['sum_columns']

@app.route('/')
def home():

    with localconverter(robjects.default_converter + pandas2ri.converter):
        r_func('data files/data.xlsx', 'data files/data2.xlsx', 'data files/out2.xlsx')

    return render_template("""
    
        function probably ran
    """)

if __name__ == '__main__':

    app.run(debug=True, threaded=False, host='0.0.0.0')
