import pandas as pd


file = pd.read_excel("tour_occ_ninat.xlsx")
file.rename(columns = file.loc[7], inplace=True)
file = file.loc[8:]
file = file.replace(':',None)
print(file.isnull().sum().sort_values(ascending=False))

print(file['2007'])

# Next steps
#Pivot the table
# Get the mean of the table
# Replace every missing value