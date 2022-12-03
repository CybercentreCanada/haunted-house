from pprint import pprint
import requests


if __name__ == '__main__':

    query = {"Or":[{"And":[{"String":"pending_timers"},{"String":"pending_batch"}]},{"String":"get_bucket_range"}]}

    yara = """
rule TestSig {
    strings:
        $a = "pending_timers"
        $b = "pending_batch"
        $c = "get_bucket_range"

    condition:
        ($a and $b) or $c
}"""

    res = requests.put("http://localhost/search", json={
        'access': [],
        'query': query,
        'yara_signature': yara,
        'start_date': None,
        'end_date': None
    })
    res = res.json()
    pprint(res)
    code = res['code']

    while True:
        res = requests.get("http://localhost")

