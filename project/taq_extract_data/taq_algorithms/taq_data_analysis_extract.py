'''TAQ data analysis module.

The functions in the module extract the data from binary format to CSV files.


This script requires the following modules:
    * itertools.product
    * multiprocessing
    * numpy
    * os
    * pandas
    * pickle
    * subprocess
    * taq_data_tools_extract

The module contains the following functions:
    * taq_build_from_scratch - extract data to daily CSV files.
    * taq_data_extract - extracts the data for every day in a year.
    * taq_daily_data_extract - parallelize the taq_data_extract function.
    * taq_midpoint_trade_data - computes the midpoint price of every trade.
    * taq_midpoint_physical_data - computes the midpoint price of every second.
    * taq_trade_signs_trade_data - computes the trade signs of every trade.
    * taq_trade_signs_physical_data - computes the trade signs of every second.
    * main - the main function of the script.

.. moduleauthor:: Juan Camilo Henao Londono <www.github.com/juanhenao21>
'''

# ----------------------------------------------------------------------------
# Modules

from itertools import product as iprod
import multiprocessing as mp
import numpy as np
import os
import pandas as pd
import pickle
import subprocess

import taq_data_tools_extract

# -----------------------------------------------------------------------------


def taq_build_from_scratch(tickers, year):
    """Extracts data to year CSV files.

    The original data must be decompressed. The function runs a script in
    C++ to decompress and then extract and filter the data for a year in CSV
    files.

    :param tickers: list of the string abbreviation of the stocks to be
     analyzed (i.e. ['AAPL', 'MSFT']).
    :param year: string of the year to be analyzed (i.e '2016').
    :return: None -- The function saves the data in a file and does not return
     a value.
    """

    tickers_rm = tickers[:]

    # Check if there are extracted files from the list of stocks
    for ticker in tickers:
        if(os.path.isfile(
            f'../../taq_data/csv_year_data_{year}/{ticker}_{year}_NASDAQ'
            + f'_quotes.csv')
           and os.path.isfile(
                f'../../taq_data/csv_year_data_{year}/{ticker}_{year}_NASDAQ'
                + f'_trades.csv')):

            print(f'The ticker {ticker} has already the trades and quotes '
                  + f'csv files')
            tickers_rm.remove(ticker)

    if (len(tickers_rm)):
        # Compile and run the C++ script to decompress
        os.chdir(f'../../taq_data/decompress_original_data_{year}/'
                 + f'armadillo-3.920.3/')
        subprocess.call('rm CMakeCache.txt', shell=True)
        subprocess.call('cmake .', shell=True)
        subprocess.call('make', shell=True)
        os.chdir('../')
        abs_path = os.path.abspath('.')
        os.system(
            'g++ main.cpp -std=c++11 -lboost_date_time -lz '
            + f'-I {abs_path}/armadillo-3.920.3/include -o decompress.out')
        os.system(f'mv decompress.out ../original_year_data_{year}/')
        os.chdir(f'../original_year_data_{year}')

        print('Extracting quotes')
        # Parallel computing
        with mp.Pool(processes=mp.cpu_count()) as pool:
            pool.starmap(taq_data_tools_extract.taq_decompress,
                         iprod(tickers_rm, [year], ['quotes']))
        print('Extracting trades')
        # Parallel computing
        with mp.Pool(processes=mp.cpu_count()) as pool:
            pool.starmap(taq_data_tools_extract.taq_decompress,
                         iprod(tickers_rm, [year], ['trades']))

        subprocess.call('rm decompress.out', shell=True)
        subprocess.call(f'mkdir ../csv_year_data_{year}/', shell=True)
        subprocess.call(f'mv *.csv ../csv_year_data_{year}/', shell=True)

    else:
        print('All the tickers have trades and quotes csv files')

    return None

# -----------------------------------------------------------------------------


def taq_data_extract(ticker, type, year):
    """Extracts the data for every day in a year.

    Extracts the trades and quotes (TAQ) data for a day from a CSV file with
    the information of a whole year. The time range for each day is from 9:30
    to 16:00, that means, the open market time.

    :param ticker: string of the abbreviation of the stock to be analyzed
     (i.e. 'AAPL').
    :param type: string with the type of the data to be extracted
     (i.e. 'trades' or 'quotes').
    :param year: string of the year to be analyzed (i.e. '2016').
    :return: None -- The function extracts the data and does not return a
     value.
    """

    function_name = taq_data_extract.__name__
    taq_data_tools_extract \
        .taq_function_header_print_data(function_name, ticker, ticker, year,
                                        '', '')

    try:

        df = pd.DataFrame()
        chunksize = 10 ** 7

        date_list = taq_data_tools_extract.taq_bussiness_days(year)

        # Load data
        csv_file = f'../../taq_data/csv_year_data_{year}/{ticker}_{year}' + \
            f'_NASDAQ_{type}.csv'

        df_type = {'quotes': {
                        'Date': 'str',
                        'Time': 'int',
                        'Bid': 'int',
                        'Ask': 'int',
                        'Vol_Bid': 'int',
                        'Vol_Ask': 'int',
                        'Mode': 'int',
                        'Cond': 'str',
                    },
                   'trades': {
                        'Date': 'str',
                        'Time': 'int',
                        'Ask': 'int',
                        'Vol_Ask': 'int',
                        'Mode': 'int',
                        'Corr': 'int',
                        'Cond': 'str',
                    }}

        col_names = {'quotes': ['Date', 'Time', 'Bid', 'Ask', 'Vol_Bid',
                                'Vol_Ask', 'Mode', 'Cond'],
                     'trades': ['Date', 'Time', 'Ask', 'Vol_Ask', 'Mode',
                                'Corr', 'Cond']}

        # Save data
        if (not os.path.isdir(f'../../taq_data/hdf5_daily_data_{year}/')):

            try:
                os.mkdir(f'../../taq_data/hdf5_daily_data_{year}/')
                print('Folder to save data created')

            except FileExistsError:
                print('Folder exists. The folder was not created')

        for chunk in pd.read_csv(csv_file, chunksize=chunksize, sep='\s+',
                                 names=col_names[type], dtype=df_type[type],
                                 na_filter=False, low_memory=False):

            chunk['Date'] = pd.to_datetime(chunk['Date'], format='%Y-%m-%d')
            chunk.set_index('Date', inplace=True)
            if (type == 'quotes'):
                chunk.drop(['Mode', 'Cond'], axis=1, inplace=True)
            else:
                chunk.drop(['Mode', 'Corr', 'Cond'], axis=1, inplace=True)

            for date in date_list:
                day = chunk.index.isin([date])
                df = chunk.loc[day & (chunk['Time'] >= 34200)
                               & (chunk['Time'] < 57600)]

                if not df.empty:
                    df.to_hdf(f'../../taq_data/hdf5_daily_data_{year}/taq_'
                              + f'{ticker}_{type}_{date}.h5', key=type,
                              format='table', append=True)

        print('Data Saved')
        print()

        return None

    except FileNotFoundError as e:
        print('No data')
        print(e)
        print()
        return None

# ----------------------------------------------------------------------------


def taq_daily_data_extract(tickers, year):
    """ Extracts data to daily CSV files.

    Extract and filter the data for every day of a year in HDF5 files.

    :param tickers: list of the string abbreviation of the stocks to be
     analyzed (i.e. ['AAPL', 'MSFT']).
    :param year: string of the year to be analyzed (i.e '2016').
    :return: None -- The function saves the data in a file and does not return
     a value.
    """

    # Extract daily data
    print('Extracting daily data')
    # Parallel computing
    with mp.Pool(processes=mp.cpu_count()) as pool:
        pool.starmap(taq_data_extract, iprod(tickers, ['quotes'], [year]))
    # Parallel computing
    with mp.Pool(processes=mp.cpu_count()) as pool:
        pool.starmap(taq_data_extract, iprod(tickers, ['trades'], [year]))

    return None

# ----------------------------------------------------------------------------


def taq_midpoint_trade_data(ticker, date):
    """Computes the midpoint price of every trade.

    Using the daily TAQ data computes the midpoint price of every trade in a
    day. For further calculations, the function returns the values for the time
    range from 9h40 to 15h50.

    :param ticker: string of the abbreviation of the stock to be analyzed
     (i.e. 'AAPL').
    :param date: string with the date of the data to be extracted
     (i.e. '2008-01-02').
    :return: tuple -- The function returns a tuple with numpy arrays.
    """

    date_sep = date.split('-')

    year = date_sep[0]
    month = date_sep[1]
    day = date_sep[2]

    try:
        # Load data
        # The module is used in other folders, so it is necessary to use
        # absolute paths instead of relative paths
        # Obtain the absolute path of the current file and split it
        abs_path = os.path.abspath(__file__).split('/')
        # Take the path from the start to the project folder
        root_path = '/'.join(abs_path[:abs_path.index('project') + 1])
        data_quotes_trade = pd.read_hdf(root_path
                                        + f'/taq_data/hdf5_daily_data_{year}/'
                                        + f'taq_{ticker}_quotes_{date}.h5',
                                        key='/quotes')

        time_q = data_quotes_trade['Time'].to_numpy()
        bid_q = data_quotes_trade['Bid'].to_numpy()
        ask_q = data_quotes_trade['Ask'].to_numpy()

        # Some files are corrupted, so there are some zero values that does not
        # have sense
        condition = ask_q != 0
        time_q = time_q[condition]
        bid_q = bid_q[condition]
        ask_q = ask_q[condition]

        midpoint = (bid_q + ask_q) / 2
        spread = ask_q - bid_q

        return (time_q, midpoint, spread)

    except FileNotFoundError as e:
        print('No data')
        print(e)
        print()
        return None

# ----------------------------------------------------------------------------


def taq_midpoint_physical_data(ticker, date):
    """Computes the midpoint price of every second.

    Using the taq_midpoint_trade_data function computes the midpoint price of
    every second. To fill the time spaces when nothing happens I replicate the
    last value calculated until a change in the price happens.

    :param ticker: string of the abbreviation of the stock to be analyzed
     (i.e. 'AAPL').
    :param date: string with the date of the data to be extracted
     (i.e. '2008-01-02').
    :return: numpy array.
    """

    date_sep = date.split('-')

    year = date_sep[0]
    month = date_sep[1]
    day = date_sep[2]

    function_name = taq_midpoint_physical_data.__name__
    taq_data_tools_extract \
        .taq_function_header_print_data(function_name, ticker, ticker, year,
                                        month, day)

    try:
        # Calculate the values of the midpoint price for all the events
        time_q, midpoint_trade, spread = taq_midpoint_trade_data(ticker, date)

        # 34800 s = 9h40 - 57000 s = 15h50
        # Reproducing the paper time values. In the results the time interval
        # for the midpoint is [34800, 56999]
        full_time = np.array(range(34800, 57000))
        midpoint = 0. * full_time

        # Select the last midpoint price of every second. If there is no
        # midpoint price in a second, takes the value of the previous second
        for t_idx, t_val in enumerate(full_time):

            condition = time_q == t_val
            if (np.sum(condition)):
                midpoint[t_idx] = midpoint_trade[condition][-1]

            else:
                midpoint[t_idx] = midpoint[t_idx - 1]

        # Prevent zero values in dates when the first seconds does not have a
        # midpoint price value
        t_pos = 34800
        while (not np.sum(time_q == t_pos)):
            t_pos -= 1
        m_pos = 0
        condition_2 = time_q == t_pos
        while (not midpoint[m_pos]):
            midpoint[m_pos] = midpoint_trade[condition_2][-1]
            m_pos += 1

        assert not np.sum(midpoint == 0)

        # Use the spread only in market time
        s_cond = (time_q >= 34800) * (time_q < 57000)
        spread_mt = spread[s_cond]

        # Saving data
        if (not os.path.isdir(f'../../taq_data/extract_data_{year}'
                              + f'/{function_name}/')):

            try:
                os.mkdir(f'../../taq_data/extract_data_{year}/'
                         + f'{function_name}/')
                print('Folder to save data created')

            except FileExistsError:
                print('Folder exists. The folder was not created')

        pickle.dump(midpoint / 10000,
                    open(f'../../taq_data/extract_data_{year}/'
                         + f'{function_name}/{function_name}_midpoint_'
                         + f'{year}{month}{day}_{ticker}.pickle', 'wb'))
        pickle.dump(spread_mt / 10000,
                    open(f'../../taq_data/extract_data_{year}/'
                         + f'{function_name}/{function_name}_spread_'
                         + f'{year}{month}{day}_{ticker}.pickle', 'wb'))
        pickle.dump(full_time,
                    open(f'../../taq_data/extract_data_{year}/'
                         + f'{function_name}/{function_name}_time.pickle',
                         'wb'))

        print('Data saved')
        print()

        return (full_time, midpoint, spread_mt)

    except TypeError as e:
        return None

# ----------------------------------------------------------------------------


def taq_trade_signs_trade_data(ticker, date):
    """Computes the trade signs of every trade.

    Using the daily TAQ data computes the trade signs of every trade in a day.
    The trade signs are computed using Eq. 1 of the
    `paper
    <https://link.springer.com/content/pdf/10.1140/epjb/e2016-60818-y.pdf>`_.
    As the trades signs are not directly given by the TAQ data, they must be
    inferred by the trades prices.
    For further calculations, the function returns the values for the time
    range from 9h40 to 15h50.

    :param ticker: string of the abbreviation of the stock to be analyzed
        (i.e. 'AAPL').
    :param date: string with the date of the data to be extracted
     (i.e. '2008-01-02').
    :return: tuple -- The function returns a tuple with numpy arrays.
    """

    date_sep = date.split('-')

    year = date_sep[0]
    month = date_sep[1]
    day = date_sep[2]

    try:
        # Load data
        # The module is used in other folders, so it is necessary to use
        # absolute paths instead of relative paths
        # Obtain the absolut path of the current file and split it
        abs_path = os.path.abspath(__file__).split('/')
        # Take the path from the start to the project folder
        root_path = '/'.join(abs_path[:abs_path.index('project') + 1])
        data_trades_trade = pd.read_hdf(root_path
                                        + f'/taq_data/hdf5_daily_data_{year}/'
                                        + f'taq_{ticker}_trades_{date}.h5',
                                        key='/trades')

        time_t = data_trades_trade['Time'].to_numpy()
        ask_t = data_trades_trade['Ask'].to_numpy()

        # All the trades must have a price different to zero
        assert not np.sum(ask_t == 0)

        # Trades identified using equation (1)
        identified_trades = np.zeros(len(time_t))
        identified_trades[-1] = 1

        # Implementation of equation (1). Sign of the price change between
        # consecutive trades

        for t_idx in range(len(time_t)):

            diff = ask_t[t_idx] - ask_t[t_idx - 1]

            if (diff):
                identified_trades[t_idx] = np.sign(diff)

            else:
                identified_trades[t_idx] = identified_trades[t_idx - 1]

        # All the identified trades must be different to zero
        assert not np.sum(identified_trades == 0)

        return (time_t, ask_t, identified_trades)

    except FileNotFoundError as e:
        print('No data')
        print(e)
        print()
        return None

# ----------------------------------------------------------------------------


def taq_trade_signs_physical_data(ticker, date):
    """Computes the trade signs of every second.

    Using the taq_trade_signs_trade_data function computes the trade signs of
    every second.
    The trade signs are computed using Eq. 2 of the
    `paper
    <https://link.springer.com/content/pdf/10.1140/epjb/e2016-60818-y.pdf>`_.
    As the trades signs are not directly given by the TAQ data, they must be
    inferred by the trades prices.
    For further calculations, the function returns the values for the time
    range from 9h40 to 15h50.
    To fill the time spaces when nothing happens I added zeros indicating that
    there were neither a buy nor a sell.

    :param ticker: string of the abbreviation of the stock to be analyzed
     (i.e. 'AAPL').
    :param date: string with the date of the data to be extracted
     (i.e. '2008-01-02').
    :return: tuple -- The function returns a tuple with numpy arrays.
    """

    date_sep = date.split('-')

    year = date_sep[0]
    month = date_sep[1]
    day = date_sep[2]

    function_name = taq_trade_signs_physical_data.__name__
    taq_data_tools_extract \
        .taq_function_header_print_data(function_name, ticker, ticker, year,
                                        month, day)

    try:
        # Calculate the values of the trade signs for all the events
        (time_t, ask_t,
         identified_trades) = taq_trade_signs_trade_data(ticker, date)

        # Reproducing the paper time values. In her results the time interval
        # for the trade signs is [34801, 57000]
        full_time = np.array(range(34801, 57001))
        trade_signs = 0. * full_time
        price_signs = 0. * full_time

        # Implementation of Eq. 2. Trade sign in each second
        for t_idx, t_val in enumerate(full_time):

            condition = (time_t >= t_val) * (time_t < t_val + 1)
            trades_same_t_exp = identified_trades[condition]
            sign_exp = int(np.sign(np.sum(trades_same_t_exp)))
            trade_signs[t_idx] = sign_exp

            if (np.sum(condition)):
                price_signs[t_idx] = ask_t[condition][-1]

        # Saving data
        taq_data_tools_extract \
            .taq_save_data(function_name,
                           (full_time, price_signs, trade_signs), ticker,
                           ticker, year, month, day)

        return (full_time, price_signs, trade_signs)

    except TypeError as e:
        return None

# ----------------------------------------------------------------------------


def main():
    """The main function of the script.

    The main function is used to test the functions in the script.

    :return: None.
    """

    pass

    return None

# ----------------------------------------------------------------------------


if __name__ == "__main__":
    main()
