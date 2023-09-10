import numpy as np
import pandas as pd

arr = np.array([[1, 1], [5, 10], [3, 100], [4, 100]])
df = pd.DataFrame(arr, columns=['a', 'b'])
# print(arr)
print(df)
df1 = df.quantile([.1, .5], interpolation='lower')
print(df1)
