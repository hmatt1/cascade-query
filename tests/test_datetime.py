import datetime
import tempfile
from cascade import Engine


def test_datetime_serde():
    eng = Engine()

    dt = datetime.datetime(2023, 1, 1, 12, 0, tzinfo=datetime.timezone.utc)
    d = datetime.date(2023, 1, 1)
    t = datetime.time(12, 0)
    td = datetime.timedelta(days=1, seconds=2)
    tz = datetime.timezone(datetime.timedelta(hours=5), "EST")

    @eng.query
    def return_dt():
        return dt

    @eng.query
    def return_d():
        return d

    @eng.query
    def return_t():
        return t

    @eng.query
    def return_td():
        return td

    @eng.query
    def return_tz():
        return tz

    assert return_dt() == dt
    assert return_d() == d
    assert return_t() == t
    assert return_td() == td
    assert return_tz() == tz

    # second call should hit cache
    assert return_dt() == dt
    assert return_d() == d
    assert return_t() == t
    assert return_td() == td
    assert return_tz() == tz


def test_datetime_canonical():
    cdir = tempfile.mkdtemp()
    eng = Engine(cache_dir=cdir)

    dt = datetime.datetime(2023, 1, 1, 12, 0, tzinfo=datetime.timezone.utc)
    d = datetime.date(2023, 1, 1)
    t = datetime.time(12, 0)
    td = datetime.timedelta(days=1, seconds=2)
    tz = datetime.timezone(datetime.timedelta(hours=5), "EST")

    @eng.query
    def return_dt():
        return dt

    @eng.query
    def return_d():
        return d

    @eng.query
    def return_t():
        return t

    @eng.query
    def return_td():
        return td

    @eng.query
    def return_tz():
        return tz

    assert return_dt() == dt
    assert return_d() == d
    assert return_t() == t
    assert return_td() == td
    assert return_tz() == tz
    eng.shutdown()

    # Reload from disk
    eng2 = Engine(cache_dir=cdir)

    @eng2.query
    def return_dt():
        return dt

    @eng2.query
    def return_d():
        return d

    @eng2.query
    def return_t():
        return t

    @eng2.query
    def return_td():
        return td

    @eng2.query
    def return_tz():
        return tz

    assert return_dt() == dt
    assert return_d() == d
    assert return_t() == t
    assert return_td() == td
    assert return_tz() == tz
    eng2.shutdown()
