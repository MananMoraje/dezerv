import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from PIL import Image
import os

data_file = 'macro_master_jun24.csv'
data = pd.read_csv(data_file)

numeric_cols = data.select_dtypes(include=np.number).columns
graph_paths = []

for col in numeric_cols:
    mean = data[col].mean()
    std_dev = data[col].std()

    plt.figure(figsize=(10, 6))
    plt.plot(data.index, data[col], label=col, color='blue')
    plt.axhline(y=mean, color='black', linestyle='--', label='Mean')

    plt.axhline(y=mean + std_dev, color='green', linestyle='--', label='+1 Std Dev')
    plt.axhline(y=mean + 2 * std_dev, color='orange', linestyle='--', label='+2 Std Dev')
    plt.axhline(y=mean + 3 * std_dev, color='red', linestyle='--', label='+3 Std Dev')

    plt.axhline(y=mean - std_dev, color='green', linestyle='--', label='-1 Std Dev')
    plt.axhline(y=mean - 2 * std_dev, color='orange', linestyle='--', label='-2 Std Dev')
    plt.axhline(y=mean - 3 * std_dev, color='red', linestyle='--', label='-3 Std Dev')

    plt.xlabel('Index')
    plt.ylabel('Value')
    plt.title(f'Standard Deviations for {col}')
    plt.legend()

    safe_col_name = col.replace("/", "_").replace(" ", "_")
    plt.savefig(f'{safe_col_name}_std_dev_plot.png')
    graph_paths.append(f'{safe_col_name}_std_dev_plot.png')

graphs = [Image.open(f) for f in graph_paths]

pdf_path = "output.pdf"

graphs[0].save(
    pdf_path, "PDF", resolution=100.0, save_all=True, append_images=graphs[1:]
)

path = os.getcwd()
files = os.listdir(path)

for file in files:
    if file.endswith(".png"):
        os.remove(os.path.join(path, file))
