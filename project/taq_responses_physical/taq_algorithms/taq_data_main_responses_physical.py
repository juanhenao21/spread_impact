'''TAQ data main module.

The functions in the module run the complete extraction, analysis and plot of
the TAQ data.

This script requires the following modules:
    * itertools.product
    * multiprocessing
    * os
    * pandas
    * pickle
    * subprocess
    * taq_data_analysis_responses_physical
    * taq_data_plot_responses_physical
    * taq_data_tools_responses_physical

The module contains the following functions:
    * taq_build_from_scratch - extract data to daily CSV files.
    * taq_daily_data_extract - parallelize the taq_data_extract function.
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

import taq_data_analysis_responses_physical
import taq_data_plot_responses_physical
import taq_data_tools_responses_physical

# -----------------------------------------------------------------------------


def taq_data_plot_generator(tickers, year):
    """Generates all the analysis and plots from the TAQ data.

    :param tickers: list of the string abbreviation of the stocks to be
     analyzed (i.e. ['AAPL', 'MSFT']).
    :param year: string of the year to be analyzed (i.e '2016').
    :return: None -- The function saves the data in a file and does not return
     a value.
    """

    date_list = taq_data_tools_responses_physical.taq_bussiness_days(year)

    # Specific functions
    # Self-response
    for ticker in tickers:

        taq_data_analysis_responses_physical \
            .taq_self_response_year_responses_physical_data(ticker, year)

    ticker_prod = iprod(tickers, tickers)
    # ticker_prod = [('AAPL', 'MSFT'), ('MSFT', 'AAPL'),
    #                ('GS', 'JPM'), ('JPM', 'GS'),
    #                ('CVX', 'XOM'), ('XOM', 'CVX'),
    #                ('GOOG', 'MA'), ('MA', 'GOOG'),
    #                ('CME', 'GS'), ('GS', 'CME'),
    #                ('RIG', 'APA'), ('APA', 'RIG')]

    # Cross-response
    # for ticks in ticker_prod:

    #     taq_data_analysis_responses_physical \
    #         .taq_cross_response_year_responses_physical_data(ticks[0],
    #                                                          ticks[1], year)

    # Parallel computing
    with mp.Pool(processes=mp.cpu_count()) as pool:

        # Plot
        pool.starmap(taq_data_plot_responses_physical
                     .taq_self_response_year_avg_responses_physical_plot,
                     iprod(tickers, [year]))
        # pool.starmap(taq_data_plot_responses_physical
        #              .taq_cross_response_year_avg_responses_physical_plot,
        #              iprod(tickers, tickers, [year]))

    return None

# -----------------------------------------------------------------------------


def main():
    """The main function of the script.

    The main function extract, analyze and plot the data.

    :return: None.
    """

    # Tickers and days to analyze
    year, tickers = taq_data_tools_responses_physical.taq_initial_data()
    # To be used when run in server
    year = '2008'
    tickers = ['MSFT', 'AAPL', 'AMZN', 'GOOG', 'JPM', 'JNJ', 'V', 'PG', 'T',
               'MA',
               'MU', 'BIIB', 'BLK', 'PNC', 'AMD', 'MS', 'MMC', 'CSX', 'TGT',
               'AMAT',
               'EQR', 'F', 'MCK', 'PEG', 'VLO', 'PAYX', 'BLL', 'A', 'FE',
               'PPG',
               'KEY', 'CAH', 'K', 'DOV', 'CINF', 'OMC', 'HES', 'AKAM', 'FCX',
               'IP',
               'ETFC', 'AVY', 'WYNN', 'WU', 'HAS', 'PKI', 'TAP', 'APA', 'TXT',
               'CHRW']

    # Basic folders
    taq_data_tools_responses_physical.taq_start_folders(year)

    # Run analysis
    # Analysis and plot
    taq_data_plot_generator(tickers, year)

    print('Ay vamos!!')

    return None

# -----------------------------------------------------------------------------


if __name__ == '__main__':
    main()
