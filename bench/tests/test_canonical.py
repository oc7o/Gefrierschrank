from gsbench.canonical import canonical_hash, raw_hash


def test_key_order_whitespace_and_escapes_are_normalized():
    assert canonical_hash('{"b": 1, "a": "\\u00e9"}') == canonical_hash('{"a":"é","b":1}')


def test_numbers_compare_by_exact_decimal_value():
    assert canonical_hash('{"x":1.0}') == canonical_hash('{"x":1}') == canonical_hash('{"x":1E+0}')
    assert canonical_hash('{"x":100}') == canonical_hash('{"x":1e2}')
    assert canonical_hash('{"x":-0.0}') == canonical_hash('{"x":0}')
    assert canonical_hash('{"x":9007199254740993}') != canonical_hash('{"x":9007199254740992}')
    assert canonical_hash('{"x":0.1000000000000000055511}') != canonical_hash('{"x":0.1}')


def test_numbers_beyond_28_significant_digits_stay_exact():
    assert canonical_hash('{"x":11111111111111111111111111111}') != canonical_hash('{"x":11111111111111111111111111112}')
    assert canonical_hash('{"x":0.1000000000000000055511151231257827}') \
        != canonical_hash('{"x":0.1000000000000000055511151231257828}')
    assert canonical_hash('{"x":1234567890123456789012345678900}') == canonical_hash('{"x":1.2345678901234567890123456789E+30}')


def test_number_is_not_its_string():
    assert canonical_hash('{"x":5}') != canonical_hash('{"x":"5"}')


def test_array_order_matters():
    assert canonical_hash('{"x":[1,2]}') != canonical_hash('{"x":[2,1]}')


def test_raw_hash_is_byte_exact():
    assert raw_hash('{"a":1}') != raw_hash('{"a": 1}')
    assert raw_hash("é") == raw_hash("é".encode())
