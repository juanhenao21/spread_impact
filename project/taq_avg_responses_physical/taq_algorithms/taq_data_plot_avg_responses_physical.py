'''TAQ data plot module.

The functions in the module plot the data obtained in the
taq_data_analysis_avg_responses_physical module.

This script requires the following modules:
    * matplotlib
    * numpy
    * pickle
    * taq_data_tools_avg_responses_physical

The module contains the following functions:
    * taq_self_response_year_avg_plot - plots the self-response average for a
      year.
    * taq_cross_response_year_avg_plot - plots the cross-response average for a
      year.
    * main - the main function of the script.

.. moduleauthor:: Juan Camilo Henao Londono <www.github.com/juanhenao21>
'''

# ----------------------------------------------------------------------------
# Modules

from matplotlib import pyplot as plt
import numpy as np
import pickle

import taq_data_tools_avg_responses_physical

# ----------------------------------------------------------------------------


def taq_self_response_year_avg_responses_physical_plot(year):
    """Plots the self-response average for a year.

    :param ticker: string of the abbreviation of the stock to be analyzed
     (i.e. 'AAPL').
    :param year: string of the year to be analyzed (i.e '2008').
    :return: None -- The function saves the plot in a file and does not return
     a value.
    """

    try:
        function_name = taq_self_response_year_avg_responses_physical_plot \
            .__name__
        taq_data_tools_avg_responses_physical \
            .taq_function_header_print_plot(function_name,'', '', year, '', '')

        # Load data
        responses = pickle.load(open(
                        f'../../taq_data/avg_responses_physical_data_{year}/taq'
                        + f'_self_response_year_avg_responses_physical_data/taq'
                        + f'_self_response_year_avg_responses_physical_data_{year}'
                        + f'_.pickle', 'rb'))

        figure = plt.figure(figsize=(16, 9))

        for r_idx, r_val in enumerate(responses):
            plt.semilogx(r_val, linewidth=5, label=f'Group {r_idx + 1}')

        plt.legend(loc='best', fontsize=25)
        plt.title('Self-response', fontsize=40)
        plt.xlabel(r'$\tau \, [s]$', fontsize=35)
        plt.ylabel(r'$R_{ii}(\tau)$', fontsize=35)
        plt.xticks(fontsize=25)
        plt.yticks(fontsize=25)
        plt.xlim(1, 10000)
        # plt.ylim(13 * 10 ** -5, 16 * 10 ** -5)
        plt.ticklabel_format(style='sci', axis='y', scilimits=(0, 0))
        plt.grid(True)
        plt.tight_layout()

        # Plotting
        taq_data_tools_avg_responses_physical \
            .taq_save_plot(function_name, figure, '', '', year, '')

        return None

    except FileNotFoundError as e:
        print('No data')
        print(e)
        print()
        return None

# ----------------------------------------------------------------------------


def main():
    """The main function of the script.

    The main function is used to test the functions in the script.

    :return: None.
    """

    pass

    return None

# -----------------------------------------------------------------------------


if __name__ == '__main__':
    main()
