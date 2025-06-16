from mpi4py import MPI
import time
import random

REQ_JAR = 1
REQ_JAM = 2
ACK = 3
RELEASE_JAR = 4
RELEASE_JAM = 5

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

num_grandmas = (size + 1) // 2
num_students = size // 2

INIT_JARS = 4
INIT_JAMS = 0


class Babcia:
    def __init__(self, rank):
        self.rank = rank
        self.queue = []
        self.clock = 0
        self.available_jars = INIT_JARS
        self.grandma_ranks = list(range(num_grandmas))

    def increment_clock(self):
        self.clock += 1

    def update_clock(self, received):
        self.clock = max(self.clock, received) + 1

    def broadcast(self, msg_type):
        for dest in self.grandma_ranks:
            if dest != self.rank:
                comm.send((msg_type, self.clock, self.rank), dest=dest)
        for dest in range(num_grandmas, size):
            if msg_type == RELEASE_JAM:
                comm.send((msg_type, self.clock, self.rank), dest=dest)

    def receive_all(self):
        while comm.Iprobe(source=MPI.ANY_SOURCE):
            msg_type, msg_clock, sender_id = comm.recv(source=MPI.ANY_SOURCE)
            if msg_type != ACK:
                self.process_message(msg_type, msg_clock, sender_id)

    def collect_acks(self, expected, ack_counter):
        while ack_counter < expected:
            if comm.Iprobe(source=MPI.ANY_SOURCE):
                msg_type, msg_clock, sender_id = comm.recv()
                if msg_type == ACK:
                    self.update_clock(msg_clock)
                    ack_counter += 1
                else:
                    self.process_message(msg_type, msg_clock, sender_id)
            else:
                time.sleep(0.01)

    def process_message(self, msg_type, msg_clock, sender_id):
        if msg_type == REQ_JAR:
            self.update_clock(msg_clock)
            self.queue.append((msg_clock, sender_id))
            self.queue.sort()
            comm.send((ACK, self.clock, self.rank), dest=sender_id)
        elif msg_type == RELEASE_JAR:
            self.available_jars += 1
        elif msg_type == RELEASE_JAM:
            self.update_clock(msg_clock)
            self.available_jars -= 1
            self.queue = [item for item in self.queue if item[1] != sender_id]

    def run(self):
        comm.Barrier()
        while True:
            sleep_time = random.uniform(5, 10)
            end_time = time.time() + sleep_time
            while time.time() < end_time:
                self.receive_all()
                time.sleep(0.05)

            queue_print = [(clock, pid + 1) for clock, pid in self.queue]
            print(f"[Babcia {self.rank + 1} o zegarze {self.clock}] chce sloik ", f'lista=[{queue_print}]',
                  flush=True)

            self.increment_clock()
            self.queue.append((self.clock, self.rank))
            self.queue.sort()
            self.broadcast(REQ_JAR)

            ack_counter = 0
            self.collect_acks(len(self.grandma_ranks) - 1, ack_counter)

            while True:
                self.receive_all()
                position = [pid for _, pid in self.queue].index(self.rank)
                if position < self.available_jars:
                    self.available_jars -= 1
                    break
                time.sleep(0.1)

            print(f"[Babcia {self.rank + 1} o zegarze {self.clock}] WCHODZI do sekcji krytycznej", flush=True)
            time.sleep(random.uniform(1, 5))
            self.increment_clock()
            self.broadcast(RELEASE_JAM)
            print(
                f"[Babcia {self.rank + 1} o zegarze {self.clock}] WYCHODZI z sekcji krytycznej, przekazuje dzem -> dostepny dzem u studentek +1",
                flush=True)
            self.queue = [x for x in self.queue if x[1] != self.rank]


class Studentka:
    def __init__(self, rank):
        self.rank = rank
        self.queue = []
        self.clock = 0
        self.available_jams = INIT_JAMS
        self.student_ranks = list(range(num_grandmas, size))

    def increment_clock(self):
        self.clock += 1

    def update_clock(self, received):
        self.clock = max(self.clock, received) + 1

    def broadcast(self, msg_type):
        for dest in self.student_ranks:
            if dest != self.rank:
                comm.send((msg_type, self.clock, self.rank), dest=dest)
        for dest in range(num_grandmas):
            if msg_type == RELEASE_JAR:
                comm.send((msg_type, self.clock, self.rank), dest=dest)

    def receive_all(self):
        while comm.Iprobe(source=MPI.ANY_SOURCE):
            msg_type, msg_clock, sender_id = comm.recv(source=MPI.ANY_SOURCE)
            if msg_type != ACK:
                self.process_message(msg_type, msg_clock, sender_id)

    def collect_acks(self, expected, ack_counter):
        while ack_counter < expected:
            if comm.Iprobe(source=MPI.ANY_SOURCE):
                msg_type, msg_clock, sender_id = comm.recv()
                if msg_type == ACK:
                    self.update_clock(msg_clock)
                    ack_counter += 1
                else:
                    self.process_message(msg_type, msg_clock, sender_id)
            else:
                time.sleep(0.01)

    def process_message(self, msg_type, msg_clock, sender_id):
        if msg_type == REQ_JAM:
            self.update_clock(msg_clock)
            self.queue.append((msg_clock, sender_id))
            self.queue.sort()
            comm.send((ACK, self.clock, self.rank), dest=sender_id)
        elif msg_type == RELEASE_JAM:
            self.available_jams += 1
        elif msg_type == RELEASE_JAR:
            self.update_clock(msg_clock)
            self.available_jams -= 1
            self.queue = [item for item in self.queue if item[1] != sender_id]

    def run(self):
        comm.Barrier()
        while True:
            sleep_time = random.uniform(0, 2)
            end_time = time.time() + sleep_time
            while time.time() < end_time:
                self.receive_all()
                time.sleep(0.05)

            queue_print = [(clock, pid - num_grandmas + 1) for clock, pid in self.queue]
            print(f"[Studentka {self.rank - num_grandmas + 1} o zegarze {self.clock}] chce konfiture, f'{queue_print}",
                  f'k=[{self.available_jams}]',
                  flush=True)

            self.increment_clock()
            self.queue.append((self.clock, self.rank))
            self.queue.sort()
            self.broadcast(REQ_JAM)

            ack_counter = 0
            self.collect_acks(len(self.student_ranks) - 1, ack_counter)

            while True:
                self.receive_all()
                position = [pid for _, pid in self.queue].index(self.rank)
                if position < self.available_jams:
                    self.available_jams -= 1
                    break
                time.sleep(0.1)

            print(f"[Studentka {self.rank - num_grandmas + 1} o zegarze {self.clock}] WCHODZI do sekcji krytycznej",
                  flush=True)
            time.sleep(random.uniform(1, 5))
            self.increment_clock()
            self.broadcast(RELEASE_JAR)
            print(
                f"[Studentka {self.rank - num_grandmas + 1} o zegarze {self.clock}] WYCHODZI z sekcji krytycznej oddaje pusty sloik -> dostepne sloiki u babc +1",
                flush=True)
            self.queue = [x for x in self.queue if x[1] != self.rank]


if rank < num_grandmas:
    Babcia(rank).run()
else:
    Studentka(rank).run()
