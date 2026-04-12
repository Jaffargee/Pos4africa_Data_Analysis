import pandas as pd

df = pd.read_excel('./Excels/DSR.xlsx')

gby_sid = df.groupby('Sale Id')

print(gby_sid.query('Sale Id = S1 Edit S1Clone'))