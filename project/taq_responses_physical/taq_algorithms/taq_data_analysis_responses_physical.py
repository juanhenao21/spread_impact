'''TAQ data analysis module.

The functions in the module analyze the data from the NASDAQ stock market,
computing the self- and cross-response functions and the trade sign self- and
cross-correlator functions. This module reproduces the sections 3.1 and 3.2 of
the `paper
<https://link.springer.com/content/pdf/10.1140/epjb/e2016-60818-y.pdf>`_.


This script requires the following modules:
    * itertools.product
    * multiprocessing
    * numpy
    * os
    * pandas
    * pickle
    * taq_data_tools_responses_physical

The module contains the following functions:
    * taq_self_response_day_responses_physical_data - computes the self
      response of a day.
    * taq_self_response_year_responses_physical_data - computes the self
      response of a year.
    * taq_cross_response_day_responses_physical_data - computes the cross
      response of a day.
    * taq_cross_response_year_responses_physical_data - computes the cross
      response of a year.
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

import taq_data_tools_responses_physical

__tau__ = 10000

# ----------------------------------------------------------------------------


def taq_self_response_day_responses_physical_data(ticker, date):
    """Computes the self-response of a day.

    Using the midpoint price and trade signs of a ticker computes the self-
    response during different time lags (:math:`\\tau`) for a day.

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
        midpoint = pickle.load(open(
                f'../../taq_data/extract_data_{year}/taq_midpoint'
                + f'_physical_data/taq_midpoint_physical_data_midpoint'
                + f'_{year}{month}{day}_{ticker}.pickle', 'rb'))
        _, _, trade_sign = pickle.load(open(
                f'../../taq_data/extract_data_{year}/taq_trade'
                + f'_signs_physical_data/taq_trade_signs_physical_data'
                + f'_{year}{month}{day}_{ticker}.pickle', 'rb'))

        assert len(midpoint) == len(trade_sign)

        # Array of the average of each tau. 10^3 s is used in the paper
        self_response_tau = np.zeros(__tau__)
        num = np.zeros(__tau__)

        # Calculating the midpoint price return and the self response function

        # Depending on the tau value
        for tau_idx in range(__tau__):

            trade_sign_tau = trade_sign[:-tau_idx - 1]
            trade_sign_no_0_len = len(trade_sign_tau[trade_sign_tau != 0])
            num[tau_idx] = trade_sign_no_0_len
            # Obtain the midpoint price return. Displace the numerator tau
            # values to the right and compute the return

            # Midpoint price returns
            log_return_sec = (midpoint[tau_idx + 1:]
                              - midpoint[:-tau_idx - 1]) \
                / midpoint[:-tau_idx - 1]

            # Obtain the self response value
            if (trade_sign_no_0_len != 0):
                product = log_return_sec * trade_sign_tau
                self_response_tau[tau_idx] = np.sum(product)

        return (self_response_tau, num)

    except FileNotFoundError as e:
        print('No data')
        print(e)
        print()
        zeros = np.zeros(__tau__)
        return (zeros, zeros)

# ----------------------------------------------------------------------------


def taq_self_response_year_responses_physical_data(ticker, year):
    """Computes the self-response of a year.

    Using the taq_self_response_day_responses_physical_data function computes
    the self-response function for a year.

    :param ticker: string of the abbreviation of stock to be analyzed
     (i.e. 'AAPL').
    :param year: string of the year to be analyzed (i.e '2016').
    :return: tuple -- The function returns a tuple with numpy arrays.
    """

    function_name = taq_self_response_year_responses_physical_data.__name__
    taq_data_tools_responses_physical \
        .taq_function_header_print_data(function_name, ticker, ticker, year,
                                        '', '')

    dates = taq_data_tools_responses_physical.taq_bussiness_days(year)

    self_values = []
    args_prod = iprod([ticker], dates)

    # Parallel computation of the self-responses. Every result is appended to
    # a list
    with mp.Pool(processes=mp.cpu_count()) as pool:
        self_values.append(pool.starmap(
            taq_self_response_day_responses_physical_data, args_prod))

    # To obtain the total self-response, I sum over all the self-response
    # values and all the amount of trades (averaging values)
    self_v_final = np.sum(self_values[0], axis=0)

    self_response_val = self_v_final[0] / self_v_final[1]
    self_response_avg = self_v_final[1]

    # Saving data
    taq_data_tools_responses_physical \
        .taq_save_data(function_name, self_response_val, ticker, ticker, year,
                       '', '')

    return (self_response_val, self_response_avg)

# ----------------------------------------------------------------------------


def taq_cross_response_day_responses_physical_data(ticker_i, ticker_j, date):
    """Computes the cross-response of a day.

    Using the midpoint price of ticker i and trade signs of ticker j computes
    the cross-response during different time lags (:math:`\\tau`) for a day.

    :param ticker_i: string of the abbreviation of the stock to be analyzed
     (i.e. 'AAPL').
    :param ticker_j: string of the abbreviation of the stock to be analyzed
     (i.e. 'AAPL').
    :param date: string with the date of the data to be extracted
     (i.e. '2008-01-02').
    :return: tuple -- The function returns a tuple with numpy arrays.
    """

    date_sep = date.split('-')

    year = date_sep[0]
    month = date_sep[1]
    day = date_sep[2]

    if (ticker_i == ticker_j):

        # Self-response
        return None

    else:
        try:
            # Load data
            midpoint_i = pickle.load(open(
                    f'../../taq_data/extract_data_{year}/taq'
                    + f'_midpoint_physical_data/taq_midpoint_physical_data'
                    + f'_midpoint_{year}{month}{day}_{ticker_i}.pickle', 'rb'))
            _, _, trade_sign_j = pickle.load(open(
                    f'../../taq_data/extract_data_{year}/taq_trade_'
                    + f'signs_physical_data/taq_trade_signs_physical_data'
                    + f'_{year}{month}{day}_{ticker_j}.pickle', 'rb'))

            assert len(midpoint_i) == len(trade_sign_j)

            # Array of the average of each tau. 10^3 s is used in the paper
            cross_response_tau = np.zeros(__tau__)
            num = np.zeros(__tau__)

            # Calculating the midpoint return and the cross response function

            # Depending on the tau value
            for tau_idx in range(__tau__):

                trade_sign_tau = 1 * trade_sign_j[:-tau_idx - 1]
                trade_sign_no_0_len = len(trade_sign_tau[trade_sign_tau != 0])
                num[tau_idx] = trade_sign_no_0_len
                # Obtain the midpoint price return. Displace the numerator tau
                # values to the right and compute the return

                # Midpoint price returns
                log_return_i_sec = (midpoint_i[tau_idx + 1:]
                                    - midpoint_i[:-tau_idx - 1]) \
                    / midpoint_i[:-tau_idx - 1]

                # Obtain the cross response value
                if (trade_sign_no_0_len != 0):
                    product = log_return_i_sec * trade_sign_tau
                    cross_response_tau[tau_idx] = np.sum(product)

            return (cross_response_tau, num)

        except FileNotFoundError as e:
            print('No data')
            print(e)
            print()
            zeros = np.zeros(__tau__)
            return (zeros, zeros)

# ----------------------------------------------------------------------------


def taq_cross_response_year_responses_physical_data(ticker_i, ticker_j, year):
    """Computes the cross-response of a year.

    Using the taq_cross_response_day_responses_physical_data function computes
    the cross-response function for a year.

    :param ticker_i: string of the abbreviation of the stock to be analyzed
     (i.e. 'AAPL').
    :param ticker_j: string of the abbreviation of the stock to be analyzed
     (i.e. 'AAPL').
    :param year: string of the year to be analyzed (i.e '2016').
    :return: tuple -- The function returns a tuple with numpy arrays.
    """

    if (ticker_i == ticker_j):

        # Self-response
        return None

    else:
        function_name = taq_cross_response_year_responses_physical_data \
            .__name__
        taq_data_tools_responses_physical \
            .taq_function_header_print_data(function_name, ticker_i, ticker_j,
                                            year, '', '')

        dates = taq_data_tools_responses_physical.taq_bussiness_days(year)

        cross_values = []
        args_prod = iprod([ticker_i], [ticker_j], dates)

        # Parallel computation of the cross-responses. Every result is appended
        # to a list
        with mp.Pool(processes=mp.cpu_count()) as pool:
            cross_values.append(pool.starmap(
                taq_cross_response_day_responses_physical_data, args_prod))

        # To obtain the total cross-response, I sum over all the cross-response
        # values and all the amount of trades (averaging values)
        cross_v_final = np.sum(cross_values[0], axis=0)

        cross_response_val = cross_v_final[0] / cross_v_final[1]
        cross_response_avg = cross_v_final[1]

        # Saving data
        taq_data_tools_responses_physical \
            .taq_save_data(function_name, cross_response_val, ticker_i,
                           ticker_j, year, '', '')

        return (cross_response_val, cross_response_avg)

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
