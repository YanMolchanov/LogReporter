import re
from collections import Counter, defaultdict
from itertools import islice
from queue import Queue
from threading import Thread
import time


'''
Паттерн для regex, ожидаются запросы типа GET и POST
Если в логах ожидаются другие типы запросов, добавьте их в группу (GET|POST)
'''
pattern: str = r"000 (.+) django\.request: (GET|POST) (\/.+\/)"

fcol_w: int = 20  # Ширина первого столбца отчета
col_w: int = 10  # Ширина остальных столбцов отчета
headers: list[str] = ["HANDLER", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
col_amt: int = len(headers)  # Количество столбцов отчета
chunksize: int = 1000  #  количество строк, на которое разбиваем загружаемый массив для поочередной загрузки
skiprows: int = 0 * chunksize


def draw_report(rows, total_r, total_api_r):
    row: str = "{:>" + str(fcol_w) + "}" + ("{:>" + str(col_w) + "}") * (col_amt - 1)
    rows: list = [headers] + rows
    body_str: str = "\n".join(row.format(*r) for r in rows)
    report: str = (f"{'-' * ((col_amt - 1) * col_w + fcol_w)}\n"
              f"\tTotal requests: {total_r}\n"
              f"\tAPI requests: {total_api_r}\n"
              f"{'-' * ((col_amt - 1) * col_w + fcol_w)}\n"
              f"{body_str}")
    return report

def process_chunk(chunk):
    #with open("logs/app_test.log", "r") as f:
    #    f.readlines()
    #    f = f.read()
    total_r: int = len(re.findall(pattern=r'\n', string=chunk))
    result = re.findall(pattern=pattern, string=chunk)
    result = [(x[0], x[2]) for x in result]
    data = Counter(result)
    return data, total_r

def process(queue, chunk):
    queue.put(process_chunk(chunk))

def read(filename):
    worker_queue = Queue()
    finished = object()
    api_r, total_r, total_api_r = Counter(), 0, 0
    with open(filename) as f:
        x = 0
        while True:
            x += 1
            next_n_lines = list(islice(f, chunksize))
            if not next_n_lines:
                print(f"Документ {filename} проанализирован!")
                break
            else:
                queue = Queue()
                Thread(target=process, args=(queue, "".join(next_n_lines))).start()
                worker_queue.put(queue)
                #print(f"Chunk # {x} (lines {(x - 1) * chunksize} - {x * chunksize}) SUCCESS!")
    worker_queue.put(finished)
    for output in iter(worker_queue.get, finished):
        data, total_r_new = output.get()
        #print(data)
        api_r.update(data)
        total_api_r += data.total()
        total_r = total_r + total_r_new
        #print(f"Chunk # {x} (lines {(x - 1) * chunksize} - {x * chunksize}) SUCCESS!")
    dd = defaultdict(int, api_r)
    handlers = list(set(x[1] for x in api_r.keys()))
    handlers.sort()
    rows = []
    #print(rows)
    for hs in handlers:
        row = [hs]
        for hd in headers:
            row.append(dd[(hd, hs)])
        rows.append(row)
    return draw_report(rows, total_r, total_api_r)






if __name__ == '__main__':
    start_time = time.time()
    result = read('logs/app_test.log')
    print(result)
    print("\n--- finished in %s seconds ---" % (time.time() - start_time))


'''
2025-03-28 12:44:46,000 INFO django.request: GET /api/v1/reviews/ 204 OK [192.168.1.59]
2025-03-28 12:21:51,000 INFO django.request: GET /admin/dashboard/ 200 OK [192.168.1.68]
2025-03-28 12:40:47,000 CRITICAL django.core.management: DatabaseError: Deadlock detected
2025-03-28 12:25:45,000 DEBUG django.db.backends: (0.41) SELECT * FROM 'products' WHERE id = 4;
2025-03-28 12:03:09,000 DEBUG django.db.backends: (0.19) SELECT * FROM 'users' WHERE id = 32;
2025-03-28 12:05:13,000 INFO django.request: GET /api/v1/reviews/ 201 OK [192.168.1.97]
2025-03-28 12:11:57,000 ERROR django.request: Internal Server Error: /admin/dashboard/ [192.168.1.29] - ValueError: Invalid input data
2025-03-28 12:37:43,000 INFO django.request: GET /api/v1/users/ 204 OK [192.168.1.36]
2025-03-28 12:01:42,000 WARNING django.security: IntegrityError: duplicate key value violates unique constraint
2025-03-28 12:09:16,000 INFO django.request: GET /api/v1/cart/ 204 OK [192.168.1.93]
2025-03-28 12:04:09,000 INFO django.request: GET /api/v1/products/ 204 OK [192.168.1.44]
2025-03-28 12:25:37,000 INFO django.request: GET /api/v1/support/ 204 OK [192.168.1.35]
2025-03-28 12:49:16,000 WARNING django.security: SuspiciousOperation: Invalid HTTP_HOST header

'''



'''
Total requests: 1000

HANDLER               	DEBUG  	INFO   	WARNING	ERROR  	CRITICAL  
/admin/dashboard/     	20     		72     	19     		14     		18  	 
/api/v1/auth/login/   	23     		78     	14     		15     		18  	 
/api/v1/orders/       	26     		77     	12     		19     		22  	 
/api/v1/payments/     	26     		69     	14     		18     		15  	 
/api/v1/products/     	23     		70     	11     		18     		18  	 
/api/v1/shipping/     	60     		128    	26     		32     		25  	 
                        178    		494    	96     		116    		116
'''

