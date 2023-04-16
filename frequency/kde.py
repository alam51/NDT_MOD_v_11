import pandas as pd
import matplotlib.pyplot as plt

file_path = r'K:\AKS\AKS Off vs On Feb March 2023.xlsx'
df = pd.read_excel(file_path, sheet_name='Freq Duration', usecols='A:B')
df.plot.kde(bw_method=.03)
df.head()
plt.show()
