import json

from gsbench.pit import parse_response


def test_source_span_is_exact_including_whitespace():
    sources = [' {"a" : 1}\n', '{"b":"}\\"]{"}', '{\n  "c": [1, {"d": null}],\n  "e": "é🧊"\n}', '{}']
    hits = ",".join('{"_index":"i","_id":%s,"_score":null,%s"_source":%s,"sort":[%d,1]}'
                    % (json.dumps(f"id{k}"), '"_routing":"r",' if k % 2 else "", s, k)
                    for k, s in enumerate(sources))
    text = '{"pit_id":"p2","took":1,"timed_out":false,"_shards":{"total":1,"failed":0},' \
           '"hits":{"max_score":null,"hits":[' + hits + ']}}'
    pit_id, parsed = parse_response(text)
    assert pit_id == "p2"
    assert [h.source for h in parsed] == sources
    assert [h.id for h in parsed] == [f"id{k}" for k in range(4)]
    assert [h.routing for h in parsed] == [None, "r", None, "r"]
    assert parsed[-1].sort == [3, 1]


def test_empty_hits():
    assert parse_response('{"pit_id":"x","hits":{"max_score":null,"hits":[]}}') == ("x", [])
