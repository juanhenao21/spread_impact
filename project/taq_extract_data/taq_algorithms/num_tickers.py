# Chech number of tickers

import subprocess
import os

os.chdir('../../taq_data/hdf5_daily_data_2008/')
files = os.listdir()

f_ticks = set(map(lambda x: x.split('_')[1], files))
print(len(f_ticks))

tickers = []

for f_tick in f_ticks:
    num = int(subprocess.check_output(f"find . -iname '*_{f_tick}_'* | wc -l", shell=True))
    if (num != 506):
        tickers.append(f_tick)

print(set(tickers))

# 'MSI', 'LO', 'DPS', 'ABI', 'BRL', 'L', 'FTR', 'PM', 'BUD', 'WM', 'AW', 'SNI', 'DISCA', 'MWW', 'LIFE', 'V', 'WB'