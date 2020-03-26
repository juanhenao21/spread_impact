'''TAQ data main module.

The functions in the module run the complete extraction, analysis and plot of
the TAQ data.

This script requires the following modules:
    * itertools.product
    * multiprocessing
    * os
    * pandas
    * taq_data_analysis_extract
    * taq_data_tools_extract

The module contains the following functions:
    * taq_data_plot_generator - generates all the analysis and plots from the
      TAQ data.
    * main - the main function of the script.

.. moduleauthor:: Juan Camilo Henao Londono <www.github.com/juanhenao21>
'''

# -----------------------------------------------------------------------------
# Modules

from itertools import product as iprod
import multiprocessing as mp
import os
import pandas as pd
import pickle

import taq_data_analysis_extract
import taq_data_tools_extract

# -----------------------------------------------------------------------------


def taq_data_plot_generator(tickers, year):
    """Generates all the analysis and plots from the TAQ data.

    :param tickers: list of the string abbreviation of the stocks to be
     analyzed (i.e. ['AAPL', 'MSFT']).
    :param year: string of the year to be analyzed (i.e '2016').
    :return: None -- The function saves the data in a file and does not return
     a value.
    """

    date_list = taq_data_tools_extract.taq_bussiness_days(year)

    # Parallel computing
    with mp.Pool(processes=mp.cpu_count()) as pool:
        # Basic functions
        pool.starmap(taq_data_analysis_extract
                     .taq_midpoint_physical_data,
                     iprod(tickers, date_list))
    # Parallel computing
    with mp.Pool(processes=mp.cpu_count()) as pool:
        # Basic functions
        pool.starmap(taq_data_analysis_extract
                     .taq_trade_signs_physical_data,
                     iprod(tickers, date_list))

    return None

# -----------------------------------------------------------------------------


def main():
    """The main function of the script.

    The main function extract, analyze and plot the data.

    :return: None.
    """

    # Tickers and days to analyze
    # year, tickers = taq_data_tools_extract.taq_initial_data()
    # To be used when run in server
    year = '2008'
    tickers = taq_data_tools_extract.taq_get_tickers_data(year)

    # Basic folders
    taq_data_tools_extract.taq_start_folders(year)

    # Run analysis
    # Use the following function if you have all the C++ modules
    # taq_data_analysis_extract.taq_build_from_scratch(tickers, year)
    # Use this function if you have the year csv files of the stocks
    # taq_data_analysis_extract.taq_daily_data_extract(tickers, year)

    # Analysis and plot
    taq_data_plot_generator(tickers, year)

    print('Ay vamos!!')

    return None

# -----------------------------------------------------------------------------


if __name__ == '__main__':
    main()
