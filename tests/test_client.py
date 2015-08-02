# -*- coding:utf-8 -*-

from plumbca import Plumbca


def test_client_basic():
    p = Plumbca()
    rv = p.ping()
    assert rv == 'SERVER OK'

    rv = p.wping()
    assert rv == 'WORKER OK'

    coll = 'test_minute'
    taglist = ['google', 'facebook', 'apple']
    val = {'tech': 1}
    p.ensure_collection(coll, expire=7200)
    for tag in taglist:
        for ts in range(128, 256):
            for i in range(5):
                rv = p.store(coll, ts, tag, val)
                assert rv == 'Store OK'

    rv = p.dump()
    assert rv == 'DUMP OK'

    for tag in taglist:
        rv = p.query(coll, 100, 300, tag)
        assert len(rv) == 128
        assert rv[0][0] == '128:{}'.format(tag)
        assert rv[0][1] == {'tech': 5}

    rv = p.fetch(coll, tagging=taglist[0])
    assert len(rv) == 128
    for i, r in enumerate(rv):
        assert rv[i][0] == '%s:%s' % (128 + i, taglist[0])
        assert rv[i][1] == {'tech': 5}

    rv = p.fetch(coll)
    assert len(rv) == 128 * len(taglist[1:])
    for i, r in enumerate(rv):
        assert rv[i][1] == {'tech': 5}

    rv = p.fetch(coll)
    assert len(rv) == 0

    for tag in taglist:
        rv = p.query(coll, 100, 300, tag)
        assert len(rv) == 0