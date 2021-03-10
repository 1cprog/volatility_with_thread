# -*- coding: utf-8 -*-


# Задача: вычислить 3 тикера с максимальной и 3 тикера с минимальной волатильностью в МНОГОПОТОЧНОМ стиле
#
# Бумаги с нулевой волатильностью вывести отдельно.
# Результаты вывести на консоль в виде:
#   Максимальная волатильность:
#       ТИКЕР1 - ХХХ.ХХ %
#       ТИКЕР2 - ХХХ.ХХ %
#       ТИКЕР3 - ХХХ.ХХ %
#   Минимальная волатильность:
#       ТИКЕР4 - ХХХ.ХХ %
#       ТИКЕР5 - ХХХ.ХХ %
#       ТИКЕР6 - ХХХ.ХХ %
#   Нулевая волатильность:
#       ТИКЕР7, ТИКЕР8, ТИКЕР9, ТИКЕР10, ТИКЕР11, ТИКЕР12
# Волатильности указывать в порядке убывания. Тикеры с нулевой волатильностью упорядочить по имени.
#
from threading import Thread
from queue import Queue, Empty
from os import path, walk
from decimal import Decimal
from warnings import warn
from python_snippets.utils import time_track


def file_parser(filename, result):

    def file_reader():
        with open(filename, 'r', encoding='utf-8') as input_file:
            for string in input_file:
                yield string
    try:
        get_file_data = file_reader()
        next(get_file_data, None)

        secid, price_min, price_max = None, None, None
        for line in get_file_data:
            try:
                secid, trade_time, price, quantity = line.split(',')
            except ValueError as err:
                print(err, err.args)
                continue

            try:
                price = Decimal(price)
            except ValueError as err:
                print(err, err.args)

            if price_max is None:
                price_min, price_max = Decimal(price), Decimal(price)

            price = Decimal(price)
            if price_max < price:
                price_max = price
            if price_min > price:
                price_min = price

        if price_min is None:
            warn(f'No data in file {filename}')
        else:
            half_sum = (price_min + price_max) / 2
            if half_sum:
                volatility = ((price_max - price_min) / half_sum) * 100
            else:
                volatility = 0
            result.put({'ticker_name': secid, 'volatility': volatility})
    except IOError as err:
        print(err, err.args)


class FileCrawler:

    def __init__(self, directory):
        self.directory = directory
        self.volatility_list = []
        self.zero_volatility_list = []
        self.threads = []

    def run(self):
        if path.exists(self.directory):
            for root, _, filelist in walk(self.directory):
                queue_tasks = Queue()
                for file in filelist:
                    task = Thread(target=file_parser, args=(path.join(root, file), queue_tasks))
                    task.start()
                    self.threads.append(task)
                    while True:
                        try:
                            result = queue_tasks.get(timeout=0.00001)
                            if result['volatility']:
                                self.volatility_list.append(result)
                            else:
                                self.zero_volatility_list.append(result['ticker_name'])
                        except Empty:
                            if not any(task.is_alive() for task in self.threads):
                                break
                for process in self.threads:
                    process.join()
            volatility_list = sorted(self.volatility_list, key=lambda val: val['volatility'], reverse=True)
            print('Максимальная волатильность:')
            for res in volatility_list[:3]:
                print(f'\t{res["ticker_name"]} - {round(res["volatility"], 2)} %')
            print('Минимальная волатильность:')
            for res in volatility_list[-3:]:
                print(f'\t{res["ticker_name"]} - {round(res["volatility"], 2)} %')
            print('Нулевая волатильность:')
            print(f'\t{self.zero_volatility_list}')


operation = FileCrawler('./trades')
operation.run()
