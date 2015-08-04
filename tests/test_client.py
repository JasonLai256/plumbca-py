# -*- coding:utf-8 -*-

from pyplumbca import Plumbca


def test_client_basic():
    p = Plumbca()
    rv = p.ping()
    assert rv == 'SERVER OK'

    rv = p.wping()
    assert rv == 'WORKER OK'

    # 测试 pyplumbca.store 方法
    coll = 'test_minute'
    taglist = ['google', 'facebook', 'apple']
    val = {'tech': 1, 'info': 2, 'china': 10}
    p.ensure_collection(coll, expire=7200)
    for tag in taglist:
        for ts in range(128, 256):
            for i in range(5):
                rv = p.store(coll, ts, tag, val)
                assert rv == 'Store OK'

    # 测试 pyplumbca.dump 方法
    rv = p.dump()
    assert rv == 'DUMP OK'

    # 测试 pyplumbca.query 方法
    for tag in taglist:
        rv = p.query(coll, 100, 300, tag)
        assert len(rv) == 128
        assert rv[0][0] == '128:{}'.format(tag)
        assert rv[0][1] == {'tech': 5, 'info': 10, 'china': 50}

    # 测试 pyplumbca.fetch 方法
    rv = p.fetch(coll, tagging=taglist[0])
    assert len(rv) == 128
    for i, r in enumerate(rv):
        assert rv[i][0] == '%s:%s' % (128 + i, taglist[0])
        assert rv[i][1] == {'tech': 5, 'info': 10, 'china': 50}

    rv = p.fetch(coll)
    assert len(rv) == 128 * len(taglist[1:])
    for i, r in enumerate(rv):
        assert rv[i][1] == {'tech': 5, 'info': 10, 'china': 50}

    rv = p.fetch(coll)
    assert len(rv) == 0

    for tag in taglist:
        rv = p.query(coll, 100, 300, tag)
        assert len(rv) == 0
