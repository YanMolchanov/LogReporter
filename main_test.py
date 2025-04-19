from main import *

def test_draw_report():
    test_tows = [['/admin/dashboard/', 0, 6, 0, 2, 0], ['/admin/login/', 0, 5, 0, 1, 0]]
    result = draw_report(test_tows, 5, 5)
    assert '/admin/dashboard/' in result
    assert '/api/v1/orders/' not in result
    assert 'Total requests: 5' in result


def test_process_slice():
    slice = '''
    2025-03-28 12:44:46,000 INFO django.request: GET /api/v1/reviews/ 204 OK [192.168.1.59]
    2025-03-28 12:18:25,000 ERROR django.request: Internal Server Error: /admin/dashboard/ [192.168.1.90] - DatabaseError: Deadlock detected
    2025-03-28 12:45:52,000 DEBUG django.db.backends: (0.32) SELECT * FROM 'shipping' WHERE id = 51;
    '''
    result = process_slice(slice)
    assert type(result[0]) is Counter
    assert dict(result[0])['INFO', '/api/v1/reviews/'] == 1
    assert type(result[1]) is int
    assert result[1] == 3


def test_read_all():
    result = read_all({'logs/app0.log'})
    assert "Total requests: 3" in result
    assert "API requests: 2" in result
    assert "/admin/dashboard/" in result
    assert '/api/v1/orders/' not in result

