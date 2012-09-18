import time
from pycassa.cassandra.ttypes import (ConsistencyLevel)
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from threading import *
import threading
from multiprocessing import *
import random
import md5

import traceback
import socket

# static vars:
# if tried 100 sleeps  to get the lock and still can't get it, give up
MAX_LOCK_TRY = 10000
class CassLock:
    '''
        global lock implemented on top of Cassandra,
        using the bakery algo

        each lock is represented by a row in Cassandra, which each contender having 
        a supercolumn in the row. Each superColumn roughly implements the "NUMBER[J]"
        in the bakerly algorithm description as on wikipedia

        here we use a simplified version, instead of taking the MAX() operation in the algorithm,
        we just throw a random number. This means that the algorithm is no longer fair,
        i.e. you might randomly override some customer who have entered the bakery earlier than you.
        but for most applications this is not a big deal. Actually java notify() does not guarantee
        a queuing behavior either.

        we utilize Lamport logical clock (number sequence) in writing and deleting
        because otherwise, the order of operations within the same originator 
        may be completely changed, if they all fall within the same millisecond.
        strictly speaking, we do not need a Lamport logical clock, and a simple number sequence per
        column would suffice,  since each contender writes
        only his own column. But having a logical clock provides better insight into the sequence
        of operations for debugging.
        
        to make sure the order between contendors are right, we truthfully implement
        the Lamport logical clock

        note that for the Lamport clock to work, we need to avoid deletion, since after
        a deletion is recorded, Cassandra won't return the latest timestamp to us anymore,
        but we need the timestamp for logical clock.


        another feature is that to prevent a contender from grabbing clock and then go 
        dead, we add a heartbeat to the clock, and remove it if it was old.


        we might use this class in a multi-thread, multi-process environment. as a measure to 
        avoid going to cassandra excessively, we first synchronize locally using 
        python multiprocessing and threading package
    '''



    def __init__(self, cf, lockname):
        self.cf = cf
        self.lockname = lockname
        # determine starting timestamp
        try:
            all_tickets = self.cf.get(self.lockname, include_timestamp = True, read_consistency_level=ConsistencyLevel.QUORUM)
            self.ts = self.get_next_ts(all_tickets)
        except Exception as e:
            self.ts = 0
        random.seed()


    # one number higher than all ts seen
    def get_next_ts(self,all_tickets):
        max_ts = 0
        for participant_id in all_tickets:
            column = all_tickets[participant_id]
            #print "real column:" 
            #print column
            column_value = column
            ticket, ts = column_value['ticket']
            if (ts > max_ts):
                max_ts = ts
            return max_ts + 1 

                
    # return a md5 hash of my hostname
    # but for testing, we just do any random string for a given thread
    def get_my_id(self):
        return socket.gethostname()

    # create a random ticket between 1 and 1000,000  1mil is considered large enough so we'd never 
    # have collisions
    def get_random_ticket(self):
        m = md5.new()
        m.update(str(random.randint(1, 1000000)))
        return m.hexdigest()
        
        
    # if called in a multi-threaded environment, this should be synchronized
    # actually just the timestamp increment
    # actually we could have used hostname + processId + threadId , but
    # that is possibly slow, so we try to save accesses to the cross-machine synchronization

    # raises exception if tried too many times
    def acquire_lock(self):
        myticket = self.get_random_ticket()
        myid = self.get_my_id()
        column_value = { 'ticket':myticket, 'live_ts': str(int(round(time.time()))) }
        

        self.cf.insert(self.lockname, { myid : column_value}, timestamp=self.ts, write_consistency_level=ConsistencyLevel.QUORUM)
        tries = 0
        while(True):
            try:
                all_tickets = self.cf.get(self.lockname, include_timestamp = True, read_consistency_level=ConsistencyLevel.QUORUM)
                #print(" fetching existing tickets: " + threading.current_thread().name , all_tickets)
                self.ts = self.get_next_ts(all_tickets)
            except Exception as e:
                traceback.print_exc();
                #print(" exception: " + str(e)  + threading.current_thread().name , all_tickets)
                self.ts = 0
                all_tickets = []

            should_sleep = False
            for participant_id  in all_tickets:
                column_value  = all_tickets[participant_id]
                ticket, useless_ts= column_value['ticket']
                live_ts, useless_ts = column_value['live_ts'] # this is real clock, not logical
                real_clock = int(round(time.time()))
                if (int(live_ts)< real_clock - 60000 ):
                    #print "too old" + str(real_clock)
                    # older than 1 minute, dead lock, ignore
                    # TODO: in the future we could potentially purge such old locks
                    continue

                if (myticket == ticket):
                    continue
                if (ticket != '0' and ( ticket < myticket or participant_id < myid)):
                    #print( "checking---- " + threading.current_thread().name ,  ticket, myticket, participant_id, myid)
                    should_sleep = True
                    break
            if (not should_sleep ):
                break #now good to proceed to critical section

            tries += 1
            if ( tries > MAX_LOCK_TRY ):
                raise Exception("took too long to acquire lock, give up ")

            time.sleep(0.01) # 10 msec

        # now the lock is acquired


    def release_lock(self):
        myid = self.get_my_id()
        column_value = { 'ticket':'0', 'live_ts': str(int(round(time.time()))) }
        

        self.cf.insert(self.lockname, { myid : column_value}, timestamp=self.ts, write_consistency_level=ConsistencyLevel.QUORUM)
