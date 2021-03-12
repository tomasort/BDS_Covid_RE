import pandas as pd

bronx_df = pd.read_excel('/Users/tomasortega/Desktop/BDS_project/data/real_estate/rollingsales_bronx.xls')
brook_df = pd.read_excel('/Users/tomasortega/Desktop/BDS_project/data/real_estate/rollingsales_brooklyn.xls')
man_df = pd.read_excel('/Users/tomasortega/Desktop/BDS_project/data/real_estate/rollingsales_manhattan.xls')
queens_df = pd.read_excel('/Users/tomasortega/Desktop/BDS_project/data/real_estate/rollingsales_queens.xls')
staten_df = pd.read_excel('/Users/tomasortega/Desktop/BDS_project/data/real_estate/rollingsales_statenisland.xls')

bronx_df = bronx_df.drop([0, 1, 2])
brook_df = brook_df.drop([0, 1, 2])
man_df = man_df.drop([0, 1, 2])
queens_df = queens_df.drop([0, 1, 2])
staten_df = staten_df.drop([0, 1, 2])
if __name__ == '__main__':
    bronx_df.to_csv('rollingsales_bronx.csv')
    brook_df.to_csv('rollingsales_brooklyn.csv')
    man_df.to_csv('rollingsales_manhattan.csv')
    queens_df.to_csv('rollingsales_queens.csv')
    staten_df.to_csv('rollingsales_statenisland.csv')
