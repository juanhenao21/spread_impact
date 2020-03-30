# Chech number of tickers

import subprocess
import os

os.chdir('../../taq_data/hdf5_daily_data_2008/')
files = os.listdir()

f_ticks = set(map(lambda x: x.split('_')[1], files))

tickers = []

for f_tick in f_ticks:
    num = int(subprocess.check_output(f"find . -iname '*_{f_tick}_'* | wc -l", shell=True))
    if (num != 506):
        tickers.append(f_tick)

print(set(tickers))