import csv
import argparse
import pandas as pd
import numpy as np
from tqdm.notebook import tqdm
import yfinance as yf
import yahoo_fin.stock_info as si


def get_ticker_info(ticker):
    info_keys = [
        'symbol',
        'shortName',
        'longName',
        'financialCurrency',
        'sector',
        'currency',
        'industry',
        'country',
        'market'
    ]
    return {key: ticker.info.get(key, None) for key in info_keys}


def parse_single_ticker(stock_name, period_months=24):
    ticker = yf.Ticker(stock_name)
    columns_drop = ['Volume', 'Dividends', 'Stock Splits']
    tmp = ticker.history(period=f'{period_months}mo').reset_index().drop(columns=columns_drop)
    for key, value in get_ticker_info(ticker).items():
        tmp[key] = value
    return tmp


def write_data_to_csv(data: pd.DataFrame, csv_name='snp500.csv', create_csv=False):
    MODE = 'w' if create_csv else 'a'
    with open(csv_name, MODE, newline='') as csvfile:
        writer = csv.writer(
            csvfile,
            delimiter=',',
            # quotechar='|', quoting=csv.QUOTE_MINIMAL
        )
        if create_csv:
            writer.writerow(data.T.index.tolist())
        for _, row in data.T.to_dict().items():
            writer.writerow(list(row.values()))


def parse_all_tickers(stock_names_list: list, csv_name='snp500.csv', limit=float('inf')):
    if not stock_names_list:
        return
    if limit < float('inf'):
        stock_names_list = np.random.choice(stock_names_list, size=limit)
    ticker_data = parse_single_ticker(stock_names_list[0])
    write_data_to_csv(ticker_data, csv_name=csv_name, create_csv=True)
    for stock_name in tqdm(stock_names_list[1:]):
        ticker_data = parse_single_ticker(stock_name)
        write_data_to_csv(ticker_data, csv_name=csv_name)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-csv_name', dest='csv_name', type=str, default='snp500.csv', help='Csv table name to same parsed stock data.')
    parser.add_argument('-limit', dest='limit',  type=int, default=float('inf'), help='Number of stocks to be parsed')
    args = parser.parse_args()
    snp500_stock_names = si.tickers_sp500()
    parse_all_tickers(snp500_stock_names, csv_name=args.csv_name, limit=args.limit)

