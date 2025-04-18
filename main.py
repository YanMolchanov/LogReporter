import re
from collections import Counter, defaultdict
from itertools import islice
from queue import Queue
import threading
import time
import logging


'''
Паттерн для regex, ожидаются запросы типа GET и POST
Если в логах ожидаются другие типы запросов, добавьте их в группу (GET|POST)
'''
pattern: str = r"000 (.+) django\.request: (GET|POST) (\/.+\/)"
'''
Заголовки - значения на основе которых формируется отчет.
Именно по ним происходит парсинг логов
'''
headers: tuple = ("HANDLER", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
col_amt: int = len(headers)  #  Количество столбцов отчета
f_col_w: int = 20  #  Ширина первого столбца отчета
col_w: int = 10  #  Ширина остальных столбцов отчета

slice_size: int = 1000  #  количество строк, на которое разбиваем загружаемый массив для поочередной обработки
main_queue = Queue()

root = logging.getLogger()
root.setLevel(logging.DEBUG)

def draw_report(rows, total_r: int, total_api_r: int) -> str:
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
    total_r = len(re.findall(pattern=r'\n', string=next_n_lines))
    result = re.findall(pattern=pattern, string=next_n_lines)
    result = [(x[0], x[2]) for x in result]
    data = Counter(result)
    return data, total_r


def read_mono(filename):
    api_r: Counter = Counter()
    total_r: int = 0
    total_api_r: int = 0
    with open(filename) as f:
        x = 0
        while True:
            x += 1
            next_n_lines = list(islice(f, slice_size))
            if not next_n_lines:
                logging.warning(f"Документ {filename} проанализирован!")
                break
            else:
                data, total_r_new = process_slice("".join(next_n_lines))
                api_r.update(data)
                total_api_r += data.total()
                total_r = total_r + total_r_new
                logging.warning(f"Chunk # {x} (lines {(x - 1)*slice_size} - {x*slice_size}) SUCCESS!")
    dd = defaultdict(int, api_r)
    handlers = list(set(x[1] for x in api_r.keys()))
    handlers.sort()
    rows = []
    for hs in handlers:
        row = [hs]
        for hd in headers:
            row.append(dd[(hd, hs)])
        rows.append(row)
    return draw_report(rows, total_r, total_api_r)


def read(filename: str, queue: Queue) -> None:
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
    queue = Queue()
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
        for hd in headers:
            row.append(dd[(hd, hs)])
        rows.append(row)
    return draw_report(rows, total_r, total_api_r)


if __name__ == '__main__':
    start_time = time.time()
    files = {'logs/app_test.log', 'logs/app1.log', 'logs/app0.log', 'logs/app3.log', 'logs/app2.log',}
    result = read_all(files)
    print(result)
    logging.debug("\n--- finished in %s seconds ---" % (time.time() - start_time))



