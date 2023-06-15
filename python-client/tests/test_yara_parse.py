from hauntedhouse.client import query_from_yara, CouldNotParseRule
import codecs
import pytest


def hex(data: str):
    return '{' + str(codecs.encode(data.encode(), 'hex'))[2:-1] + '}'


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

    assert query == f"(({hex('pending_timers')} & {hex('pending_batch')}) | {hex('get_bucket_range')})"


def test_yara_error():
    query = query_from_yara("""
        rule TestSig {
            strings:
                $a = /pending.*timers/
                $b = /pending.*batch/
                $c = /get_bucket.*range/

            condition:
                ($a and $b) or $c
        }
    """)

    assert query == f"((({hex('pending')} & {hex('timers')}) & ({hex('pending')} & {hex('batch')})) " \
                    f"| ({hex('get_bucket')} & {hex('range')}))"


def test_unsupported_condition_logic():
    with pytest.raises(CouldNotParseRule):
        query_from_yara("""
            rule TestSig {
                strings:
                    $a = /pending.*timers/
                    $b = /pending.*batch/
                    $c = /get_bucket.*range/

                condition:
                    for 1 i in (1..2):(
                        @b[0] > 10
                    )
                    or ($a and $b) or $c
            }
        """)
