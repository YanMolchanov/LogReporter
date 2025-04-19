import datetime
import re
import os.path
import threading
import time
import logging
from collections import Counter, defaultdict
from itertools import islice
from queue import Queue
from argparse import ArgumentParser


pattern: str = r"000 (.+) django\.request: .+ (\/.+\/)" #  Паттерн regex
headers: tuple = ("HANDLER", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL") #  Заголовки
col_amt: int = len(headers)  #  Количество столбцов отчета
f_col_w: int = 20  #  Ширина первого столбца отчета
col_w: int = 10  #  Ширина остальных столбцов отчета
slice_size: int = 1000  #  Количество строк, на которое разбиваем загружаемый массив для поочередной обработки
main_queue: Queue = Queue() #  Главная очередь для сбора результатов работы потоков


def draw_report(rows: list,
                total_r: int,
                total_api_r: int) -> str:
    #print(rows)
    row: str = "{:>" + str(f_col_w) + "}" + ("{:>" + str(col_w) + "}") * (col_amt - 1)
    rows: list = [headers] + rows
    body_str: str = "\n".join(row.format(*r) for r in rows)
    report: str = (f"{'-' * ((col_amt - 1) * col_w + f_col_w)}\n"
              f"\tTotal requests: {total_r}\n"
              f"\tAPI requests: {total_api_r}\n"
              f"{'-' * ((col_amt - 1) * col_w + f_col_w)}\n"
              f"{body_str}")
    return report


def process_slice(next_n_lines: str) -> (Counter, int):
    total_r = len(re.findall(pattern=r'\d{4}-', string=next_n_lines))
    result = re.findall(pattern=pattern, string=next_n_lines)
    #result = [(x[0], x[2]) for x in result]
    data = Counter(result)
    return data, total_r


def read(filename: str,
         queue: Queue) -> None:
    with open(filename) as f:
        x: int = 0
        while True:
            x += 1
            next_n_lines = list(islice(f, slice_size))
            if not next_n_lines:
                logging.debug(f"Документ {filename} проанализирован!")
                break
            else:
                data, total_r_new = process_slice("".join(next_n_lines))
                queue.put((data, total_r_new))
                main_queue.put(queue)


def read_all(filenames: set[str]) -> str:
    api_r: Counter = Counter()
    total_r: int = 0
    total_api_r: int = 0
    queue: Queue = Queue()
    finished = object()
    threads = [threading.Thread(target=read, args=(filename, queue)) for filename in filenames]
    for thread in threads:
        thread.start()
    logging.debug(f'Запуск потоков в кол-ве {len(filenames)} шт.')
    for t in threads:
        t.join()
    logging.debug(f'Все потоки завершили свою работу.')
    main_queue.put(finished)
    for output in iter(main_queue.get, finished):
        data, total_r_new = output.get()
        api_r.update(data)
        total_api_r += data.total()
        total_r = total_r + total_r_new
    dd = defaultdict(int, api_r)
    handlers = list(set(x[1] for x in api_r.keys()))
    handlers.sort()
    rows = []
    for hs in handlers:
        row = [hs]
        for hd in headers[1:]:
            row.append(dd[(hd, hs)])
        rows.append(row)
    return draw_report(rows, total_r, total_api_r)


def main():
    start_time = time.time()
    parser = ArgumentParser()
    parser.add_argument('files',
                        nargs='+')
    parser.add_argument("--debug",
                        help="Set log level to DEBUG")
    parser.add_argument("--report",
                        help="Report name",
                        default=f"report {str(datetime.datetime.now().strftime(f"%Y-%m-%d %H%M%S"))}")
    args = parser.parse_args()
    if args.debug is not None:
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)
    files = set([f for f in args.files if os.path.isfile(f)])
    for d in set(args.files).difference(set(files)):
        logging.warning(f"Файл \'{d}\' не существует")
    result = read_all(files)
    print(result)
    with open("reports/" + args.report, 'w') as rep:
        rep.write(result)
    logging.debug("\n--- finished in %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    main()



