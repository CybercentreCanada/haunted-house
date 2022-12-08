from pprint import pprint
import requests
import requests.auth
import time


def main():
    query = {"Or": [{"And": [{"String": "pending_timers"}, {"String": "pending_batch"}]}, {"String": "get_bucket_range"}]}

    yara = """
rule TestSig {
    strings:
        $a = "pending_timers"
        $b = "pending_batch"
        $c = "get_bucket_range"

    condition:
        ($a and $b) or $c
}"""
    headers = {"Authorization": "Bearer password"}
    res = requests.post("http://localhost:8080/search/", json={
        'access': [],
        'query': query,
        'yara_signature': yara,
        'start_date': None,
        'end_date': None
    }, headers=headers)
    if res.status_code != 200:
        print(res.status_code)
        print(res.text)
        return

    res = res.json()
    pprint(res)
    code = res['code']

    while True:
        time.sleep(10)
        res = requests.get("http://localhost:8080/search/" + code, headers=headers).json()
        pprint(res)
        if res['finished']:
            break


if __name__ == '__main__':
    main()