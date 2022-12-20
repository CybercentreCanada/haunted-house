from hauntedhouse.client import query_from_yara


def test_simple_yara():
    query = query_from_yara("""
        rule TestSig {
            strings:
                $a = "pending_timers"
                $b = "pending_batch"
                $c = "get_bucket_range"

            condition:
                ($a and $b) or $c
        }
    """)

    assert query == {"Or": [
        {"And": [{"String": "pending_timers"}, {"String": "pending_batch"}]},
        {"String": "get_bucket_range"}
    ]}

