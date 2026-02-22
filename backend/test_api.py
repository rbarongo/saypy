import json
import urllib.request

BASE = 'http://127.0.0.1:8000'

def post(path, data):
    url = BASE + path
    b = json.dumps(data).encode('utf-8')
    req = urllib.request.Request(url, data=b, headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(req) as resp:
        return resp.read().decode('utf-8')

if __name__ == '__main__':
    rows = [{"collection_code":"import","s2":"2024-02-14T00:00:00","s3":1,"s4":"Tester"}]
    print('POST /members_collections/validate')
    print(post('/members_collections/validate', rows))
    print('\nPOST /members_collections/bulk')
    try:
        print(post('/members_collections/bulk', rows))
    except Exception as e:
        import urllib.error
        if isinstance(e, urllib.error.HTTPError):
            print('HTTPError:', e.code)
            try:
                print(e.read().decode())
            except Exception:
                pass
        else:
            print('Error:', e)
    print('\nGET /reports/members_collections')
    import urllib.request
    with urllib.request.urlopen(BASE + '/reports/members_collections') as r:
        print(r.read().decode())
