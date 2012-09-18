from CassLock import CassLock
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
import threading
import time


class FakeCassLock(CassLock):
    ''' just swap out the get_my_id() method so that it
        can be tested on the same box 
    '''
    def __init__(self, cf, lockname):
        #super(FakeCassLock, self).__init__(cf,lockname)
        CassLock.__init__(self,cf,lockname)

    def get_my_id(self):
        return 'fake_id' + threading.current_thread().name

class MyTestThread(threading.Thread):
    def run(self):
        pool = ConnectionPool('yytest', ['localhost:9160'])
        cf = ColumnFamily(pool, 'testcf')

        c = FakeCassLock(cf, 'testlock')

        print("acquiring from " + threading.current_thread().name ,  time.time())
        c.acquire_lock()
        print("got lock by " + threading.current_thread().name ,  time.time())

        # critical stuff
        time.sleep(5)

        print("releasing from " + threading.current_thread().name ,  time.time())
        c.release_lock()
        print("released from " + threading.current_thread().name ,  time.time())

        print( "result of lockng and releasing:" )
        print cf.get('testlock')


t1 = MyTestThread()
t2 = MyTestThread()

#t1.run()

t1.start()
time.sleep(2)
t2.start()
