import os
from datetime import datetime
import shutil
import sqlite3
from rpy2.robjects.conversion import localconverter
from rpy2.robjects import pandas2ri

# import json

# Docker didn't always import/install libraries properly
try:
    from flask import Flask, request, render_template, send_file

except Exception as e:
    print(e)
    os.system('pip install rpy2')
    os.system('pip install flask')

    from flask import Flask, request, render_template, send_file

try:
    import rpy2.robjects as robjects

except ImportError or ModuleNotFoundError:
    os.system('pip install rpy2')
    import rpy2.robjects as robjects

print('Imports complete')

r = robjects.r
r.source('model.r')

with localconverter(robjects.default_converter + pandas2ri.converter):
    r_func = robjects.globalenv['sum_columns']
print('R integration complete')

app = Flask(__name__)

# Useful to save as a variable
output_dir = 'output/'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

"""
try:
    with open('db.json', 'r') as db_file:
        db = json.load(db_file)
        db_file.close()

except FileNotFoundError or json.decoder.JSONDecodeError:
    with open('db.json', 'w') as db_file:
        db = []
        json.dump(db, db_file)
        db_file.close()
"""

# Set up db
sqliteConnection = sqlite3.connect('entries.db', check_same_thread=False)
cursor = sqliteConnection.cursor()

print('SQL connection complete')

# Make sure the table exists in the db
def table_exists(table_name):
    global cursor

    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    result = cursor.fetchone()
    return result is not None

if not table_exists('Entries'):
    cursor.execute("""
        CREATE TABLE Entries (
            input_1 TEXT,
            input_2 TEXT,
            output TEXT,
            uid INTEGER
        )
    """)
    sqliteConnection.commit()
    cursor.close()
    print(f"Table created.")


"""
@app.route('/by_name', methods=['GET', 'POST'])
def name():
    global input_file_1, input_file_2

    if request.method == "POST":
        input_file_1 = request.form.get("input1")
        input_file_2 = request.form.get("input2")
        output_file = request.form.get('output')
        try:
            r_func(f'data files/{input_file_1}', f'data files/{input_file_2}', f'data files/{output_file}')

        except:
            pass

    return render_template('name.html')
"""


@app.route('/', methods=['GET', 'POST'])
def upload():
    global cursor
    cursor = sqliteConnection.cursor()
    cursor.execute('SELECT * FROM Entries')
    n = cursor.fetchall()

    if request.method == 'GET':
        search = request.args.get('search', '')

        if search is not None:
            print('not none')
            if search.strip() != '':
                search = search.strip()
                search = search.lower()
                print(search)

                l = []
                for row in n:
                    for col in row[0:3]:
                        if search in str(col):
                            l.append(row)
                            print('row added')

                n = list(set(l))
    return render_template('upload.html', tbl=n)


@app.route('/download/<filename>')
def download(filename):
    # Ensure this path matches where your files are stored
    print(filename)
    return send_file(output_dir + filename, as_attachment=True)

@app.route('/success', methods=['POST'])
def success():
    global cursor

    if request.method == 'POST':
        print('POST request made to /success')
        # Used to name files to prevent repeated names
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save uploaded files
        f = request.files['input1']
        f.save(f'uploads/{f.filename.replace(".xlsx", "")}_{timestamp}.xlsx')
        input_file_1 = f'uploads/{f.filename.replace(".xlsx", "")}_{timestamp}.xlsx'

        f = request.files['input2']
        f.save(f'uploads/{f.filename.replace(".xlsx", "")}_{timestamp}.xlsx')
        input_file_2 = f'uploads/{f.filename.replace(".xlsx", "")}_{timestamp}.xlsx'

        # Generate output file path
        shutil.copy('data files/out.xlsx', f'{output_dir}/output_20240723_122139.xlsx')
        print('Copied output template')

        # Copy and rename the base output file because the R program didn't properly format xlsx files it made
        old_file = os.path.join(output_dir, "output_20240723_122139.xlsx")
        output_file = os.path.join(output_dir, f"output_{timestamp}.xlsx")
        os.rename(old_file, output_file)
        print('Renamed output file')

        print(input_file_1)
        print(input_file_2)
        print(output_file)

        # Save the model in a seperate file because for some reason keeping the code in this file caused a
        # NotImplementedError that I couldn't fix.
        # os.system(f'python r_model.py {input_file_1} {input_file_2} {output_file}')
        # db.append([input_file_1, input_file_2, output_file, len(db)])
        print('Calling r_func')
        with localconverter(robjects.default_converter + pandas2ri.converter):
            r_func(input_file_1, input_file_2, output_file)

        # Save the entry to entries.db
        cursor = sqliteConnection.cursor()
        cursor.execute('SELECT * FROM Entries')
        n = len(cursor.fetchall())
        cursor.execute(f'INSERT INTO Entries (input_1, input_2, output, uid) VALUES (?, ?, ?, ?)',
                       (input_file_1, input_file_2, output_file, n))
        sqliteConnection.commit()
        cursor.close()

        # Automatically download output file

        return send_file(output_file, as_attachment=True)

        """
        with open('db.json', 'w') as db_file:
            json.dump(db, db_file, indent=4)
            db_file.close()
        """

    return render_template('success.html')


if __name__ == '__main__':

    app.run(debug=True, threaded=False, host='0.0.0.0')
