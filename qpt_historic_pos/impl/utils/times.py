
from datetime import datetime, timedelta, tzinfo
import pytz
import os
import signal
import time
import threading

USecPerSecond = 1e6
USecPerDay = 24 * 60 * 60 * USecPerSecond


ChicagoTimeZone = pytz.timezone('America/Chicago')
NewYorkTimeZone = pytz.timezone('America/New_York')
LATimeZone = pytz.timezone('America/Los_Angeles')
HongKongTimeZone = pytz.timezone('Asia/Hong_Kong')
TokyoTimeZone = pytz.timezone('Asia/Tokyo')

UtcTimeZone = pytz.UTC

EPOCH = datetime(1970, 1, 1, tzinfo=UtcTimeZone)


def parse_ISO8601(dt_str):
    dt = datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S.%fZ')
    return dt.replace(tzinfo=pytz.utc)


def to_micro_sec(t, timezone=ChicagoTimeZone) -> int:
    """
    :param t: can be <br> string in one of the format[ "%Y-%m-%d %H:%M:%S.%f" , "%Y-%m-%d %H:%M:%S" ]
                    <br> datetime object with timezone
                    <br> integer time stamp in micro second precision
    :param timezone: when the first parameter is string, apply this timezone
    :return: integer timestamp in microsecond
    """
    if isinstance(t, int):
        #           2009                    2100
        if t < 1230768000000000 or t > 4102444800000000:
            err_msg = 'micro second time outside range ' + str(t)
            raise RuntimeError(err_msg)
        return t
    elif isinstance(t, str):
        ot = None
        try:
            ot = datetime.strptime(t, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            try:
                ot = datetime.strptime(t, '%Y-%m-%d %H:%M:%S')
                ot = timezone.localize(ot)
            except ValueError:
                raise ValueError(t + " does not match format '%H:%M:%S.%f' or %H:%M:%S'")
        return to_micro_sec(ot)
    elif isinstance(t, datetime):
        return int(t.timestamp() * 1000000.0)
    else:
        err_msg = 'unknown time parameter[' + str(t) + '] type ' + str(type(t))
        raise TypeError(err_msg)


class SFThread(threading.Thread):

    def __init__(self, seconds):
        super(SFThread, self).__init__(target=self.run)
        self._stop_event = threading.Event()
        self.ttl = seconds

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

    def run(self):
        start_ts = datetime.now()
        while not self.stopped():
            tnow = datetime.now()
            if (tnow - start_ts).total_seconds() > self.ttl:
                os.kill(os.getpid(), signal.SIGINT)
            time.sleep(10)

    def cancel(self):
        self.stop()
        time.sleep(2)
        self.join()
