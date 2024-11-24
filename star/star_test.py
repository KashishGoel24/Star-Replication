import random
import threading
from typing import Optional
import unittest
import os
import time

from star.star_cluster import StarCluster, StarClient
from core.logger import client_logger
from core.logger import set_client_logfile, remove_client_logfile

class TestSTAR(unittest.TestCase):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.star = StarCluster()

  def setUp(self) -> None:
    self.star.start_all()

  def tearDown(self) -> None:
    self.star.stop_all()

  def test_basic(self) -> None:
    logfile_name = f"logs/{self._testMethodName}.log"
    if os.path.exists(logfile_name):
      os.remove(logfile_name)
    file_sink_id = set_client_logfile(logfile_name)

    try:
      client1 = self.star.connect(1)
      client2 = self.star.connect(2)
      client1.set("key", 2)
      logger_c1 = client_logger.bind(server_name="Client 1")
      logger_c2 = client_logger.bind(server_name="Client 2")
      logger_c1.info(f"Set key = {2}")
      status, val = client2.get("key")
      self.assertTrue(status, msg=val)
      logger_c2.info(f"Get key = {val}")
    finally:
      remove_client_logfile(file_sink_id)

  def test_gen_history(self) -> None:
    logfile_name = f"logs/{self._testMethodName}.log"
    if os.path.exists(logfile_name):
      os.remove(logfile_name)
    file_sink_id = set_client_logfile(logfile_name)

    def setter(c: StarClient, iters: int, name: str) -> None:
      logger = client_logger.bind(server_name=name)
      for i in range(iters):
        logger.info(f"Setting key = {i}")
        c.set("key", f"{i}")
        logger.info(f"Set key = {i}")

    def getter(c: StarClient, iters: int, name: str) -> None:
      logger = client_logger.bind(server_name=name)
      for i in range(iters):
        logger.info(f"Getting key")
        status, val = c.get("key")
        self.assertTrue(status, msg=val)
        logger.info(f"Get key = {val}")

    try:
      client1 = self.star.connect(1)
      client2 = self.star.connect(2)
      time.sleep(10)
      s = threading.Thread(target=setter, args=(client1, 10, "worker_0"))
      g = threading.Thread(target=getter, args=(client2, 10, "worker_1"))

      s.start()
      g.start()
      s.join()
      g.join()
      client3 = self.star.connect(3)
      status, val = client3.get("key")
      self.assertTrue(status, msg=val)
      print(f"Get key from client 3= {val}")
    finally:
      remove_client_logfile(file_sink_id)

  def test_throughput(self) -> None:
    # Redirect the logs to a file
    logfile_name = f"logs/{self._testMethodName}.log"
    if os.path.exists(logfile_name):
        os.remove(logfile_name)
    file_sink_id = set_client_logfile(logfile_name)

    self.test_duration = 10 # seconds
    # Counters to keep track of operations
    self.total_sets = 0
    self.total_gets = 0
    self.lock = threading.Lock()

    def setter(c: StarClient, name: str) -> None:
      logger = client_logger.bind(server_name=name)
      for i in range(10):
        logger.info(f"Setting key = {i}")
        c.set("key", f"{i}")
        logger.info(f"Set key = {i}")
        # print(f"SETTER ITERATION {i} DONE")
      with self.lock:
        self.total_sets += 10

    def getter(c: StarClient, name: str) -> None:
      logger_instance = client_logger.bind(server_name=name)
      start_time = time.time()
      gets = 0
      while True:
        logger_instance.info(f"Getting key")
        status, val = c.get("key")
        self.assertTrue(status, msg=val)
        logger_instance.info(f"Get key = {val}")
        gets += 1
        # Check if the time window has elapsed
        if time.time() - start_time >= self.test_duration:
          break
      with self.lock:
        self.total_gets += gets

    try:
      # Connect clients
      # client1 = self.star.connect(1)
      num_getters = 3
      clients = [self.star.connect(i) for i in range(num_getters)]

      num_setters = 3
      clients_setter = [self.star.connect(j+num_getters) for j in range(num_setters)]

      # Set the initial value
      # client1.set("key", "0")

      # start num_setters threads for setters
      setter_threads = [
          threading.Thread(target=setter, args=(clients_setter[i], f"worker_{i+1}"))
          for i in range(num_setters)
      ]

      # Start num_getters threads for getter
      getter_threads = [
          threading.Thread(target=getter, args=(clients[i], f"worker_{i+1+num_getters}"))
          for i in range(num_getters)
      ]

      # Record the start time
      start_time = time.time()

      # Start the threads
      for s_thread in setter_threads:
        s_thread.start()
      for g_thread in getter_threads:
          g_thread.start()

      # Let the threads run for test_duration
      while time.time() - start_time < self.test_duration:
          time.sleep(1)

      # Join the threads
      for s_thread in setter_threads:
          s_thread.join()
      for g_thread in getter_threads:
          g_thread.join()

      # Calculate the throughput
      total_time = time.time() - start_time
      writes_per_second = self.total_sets / total_time
      reads_per_second = self.total_gets / total_time

      print(f"Total number of sets: {self.total_sets}")
      print(f"Total number of gets : {self.total_gets}")
      print(f"Write throughput: {writes_per_second:.2f} writes/s")
      print(f"Read throughput: {reads_per_second:.2f} reads/s")

    finally:
      remove_client_logfile(file_sink_id)

  def test_1(self) -> None:
    # Redirect the logs to a file
    logfile_name = f"logs/{self._testMethodName}.log"
    if os.path.exists(logfile_name):
        os.remove(logfile_name)
    file_sink_id = set_client_logfile(logfile_name)

    self.test_duration = 10 # seconds
    # Counters to keep track of operations
    self.total_sets = 0
    self.total_gets = 0
    self.lock = threading.Lock()

    def setter(c: StarClient, name: str) -> None:
      logger_instance = client_logger.bind(server_name=name)
      start_time = time.time()
      sets = 0
      while True:
        with self.lock:
          logger_instance.info(f"Setting key = {sets}")
          c.set("key", f"{sets}")
          logger_instance.info(f"Set key = {sets}")
        sets += 1
        # Check if the time window has elapsed
        if time.time() - start_time >= self.test_duration:
          break
        time.sleep(0.01)
      with self.lock:
        self.total_sets += sets

    def getter(c: StarClient, name: str) -> None:
      logger_instance = client_logger.bind(server_name=name)
      start_time = time.time()
      gets = 0
      while True:
        logger_instance.info(f"Getting key")
        status, val = c.get("key")
        self.assertTrue(status, msg=val)
        logger_instance.info(f"Get key = {val}")
        gets += 1
        # Check if the time window has elapsed
        if time.time() - start_time >= self.test_duration:
          break
      with self.lock:
        self.total_gets += gets

    try:
      # Connect clients
      # client1 = self.star.connect(1)
      num_getters = 3
      clients = [self.star.connect(i) for i in range(num_getters)]

      num_setters = 3
      clients_setter = [self.star.connect(j+num_getters) for j in range(num_setters)]

      # Set the initial value
      # client1.set("key", "0")

      # start num_setters threads for setters
      setter_threads = [
          threading.Thread(target=setter, args=(clients_setter[i], f"worker_{i+1}"))
          for i in range(num_setters)
      ]

      # Start num_getters threads for getter
      getter_threads = [
          threading.Thread(target=getter, args=(clients[i], f"worker_{i+1+num_getters}"))
          for i in range(num_getters)
      ]

      # Record the start time
      start_time = time.time()

      # Start the threads
      for s_thread in setter_threads:
        s_thread.start()
      for g_thread in getter_threads:
          g_thread.start()

      # Let the threads run for test_duration
      while time.time() - start_time < self.test_duration:
          time.sleep(1)

      # Join the threads
      for s_thread in setter_threads:
          s_thread.join()
      for g_thread in getter_threads:
          g_thread.join()

      # Calculate the throughput
      total_time = time.time() - start_time
      writes_per_second = self.total_sets / total_time
      reads_per_second = self.total_gets / total_time

      print(f"Total number of sets: {self.total_sets}")
      print(f"Total number of gets : {self.total_gets}")
      print(f"Write throughput: {writes_per_second:.2f} writes/s")
      print(f"Read throughput: {reads_per_second:.2f} reads/s")

    finally:
      remove_client_logfile(file_sink_id)


  # testing the case when we are sending the request to leader and simultaneously reading from it and another server
  def test_leader_write_read(self):
    logfile_name = f"logs/{self._testMethodName}.log"
    if os.path.exists(logfile_name):
        os.remove(logfile_name)
    file_sink_id = set_client_logfile(logfile_name)

    # Initialize counters
    self.test_duration = 2 # seconds
    self.total_sets = 0
    self.total_gets = 0
    self.lock = threading.Lock()

    leader_server = 3

    def setter(client: StarClient, worker_name: str, server_number: Optional[int] = None):
      logger_instance = client_logger.bind(server_name=worker_name)
      sets = 0
      start_time = time.time()
      while True:
        with self.lock:
          logger_instance.info(f"Setting key = {sets}")
          client.set("key", str(sets), server_number)
          logger_instance.info(f"Set key = {sets}")
        sets += 1
        
        if time.time() - start_time >= self.test_duration:
          break
        time.sleep(0.01)
      with self.lock:
        self.total_sets += sets

    def getter(client: StarClient, worker_name: str, server_number: Optional[int] = None):
      logger_instance = client_logger.bind(server_name=worker_name)
      gets = 0
      start_time = time.time()
      while True:
        logger_instance.info(f"Getting key")
        status, val = client.get("key", server_number)
        self.assertTrue(status, msg=val)
        logger_instance.info(f"Get key = {val}")
        gets += 1
        if time.time() - start_time >= self.test_duration:
          break
      with self.lock:
        self.total_gets += gets

    try:
      # Create 1 client for setting values to the leader server
      client1 = self.star.connect(1)
      # Create 2 clients for getting values from the leader server and another server
      client2 = self.star.connect(2) 
      client3 = self.star.connect(3)

      # Create and start threads
      setter_thread = threading.Thread(target=setter, args=(client1, "worker_1", leader_server))
      getter_thread1 = threading.Thread(target=getter, args=(client2, "worker_2", leader_server))
      getter_thread2 = threading.Thread(target=getter, args=(client3, "worker_3"))

      start_time = time.time()

      setter_thread.start()
      getter_thread1.start() 
      getter_thread2.start()

      # Let threads run for test duration
      while time.time() - start_time < self.test_duration:
        time.sleep(1)

      # Join threads
      setter_thread.join()
      getter_thread1.join()
      getter_thread2.join()

      # Calculate throughput
      total_time = time.time() - start_time
      writes_per_second = self.total_sets / total_time
      reads_per_second = self.total_gets / total_time

      print(f"Total number of sets: {self.total_sets}")
      print(f"Total number of gets: {self.total_gets}")
      print(f"Write throughput: {writes_per_second:.2f} writes/s")
      print(f"Read throughput: {reads_per_second:.2f} reads/s")

    finally:
      remove_client_logfile(file_sink_id)


# test the case when we are writing to two non-leader servers and continuously reading from the leader server
  def test_non_leader_write_leader_read(self):
    logfile_name = f"logs/{self._testMethodName}.log"
    if os.path.exists(logfile_name):
        os.remove(logfile_name)
    file_sink_id = set_client_logfile(logfile_name)

    # Initialize counters
    self.test_duration = 2 # seconds
    self.total_sets = 0
    self.total_gets = 0
    self.lock = threading.Lock()

    leader_server = 3
    first_non_leader = 1
    second_non_leader = 4

    def setter(client: StarClient, worker_name: str, server_number: Optional[int] = None):
      logger_instance = client_logger.bind(server_name=worker_name)
      sets = 0
      start_time = time.time()
      while True:
        with self.lock:
          logger_instance.info(f"Setting key = {sets}")
          client.set("key", str(sets), server_number)
          logger_instance.info(f"Set key = {sets}")
        sets += 1
        
        if time.time() - start_time >= self.test_duration:
          break
        time.sleep(0.01)
      with self.lock:
        self.total_sets += sets

    def getter(client: StarClient, worker_name: str, server_number: Optional[int] = None):
      logger_instance = client_logger.bind(server_name=worker_name)
      gets = 0
      start_time = time.time()
      while True:
        logger_instance.info(f"Getting key")
        status, val = client.get("key", server_number)
        self.assertTrue(status, msg=val)
        logger_instance.info(f"Get key = {val}")
        gets += 1
        if time.time() - start_time >= self.test_duration:
          break
      with self.lock:
        self.total_gets += gets

    try:
      # Create 1 client for setting values to non two leader servers
      client1 = self.star.connect(1)
      client2 = self.star.connect(2) 
      # Create 2 clients for getting values from the leader server
      client3 = self.star.connect(3)

      # Create and start threads
      setter_thread1 = threading.Thread(target=setter, args=(client1, "worker_1", first_non_leader))
      setter_thread2 = threading.Thread(target=getter, args=(client2, "worker_2", second_non_leader))
      getter_thread = threading.Thread(target=getter, args=(client3, "worker_3", leader_server))

      start_time = time.time()

      setter_thread1.start()
      setter_thread2.start() 
      getter_thread.start()

      # Let threads run for test duration
      while time.time() - start_time < self.test_duration:
        time.sleep(1)

      # Join threads
      setter_thread1.join()
      setter_thread2.join()
      getter_thread.join()

      # Calculate throughput
      total_time = time.time() - start_time
      writes_per_second = self.total_sets / total_time
      reads_per_second = self.total_gets / total_time

      print(f"Total number of sets: {self.total_sets}")
      print(f"Total number of gets: {self.total_gets}")
      print(f"Write throughput: {writes_per_second:.2f} writes/s")
      print(f"Read throughput: {reads_per_second:.2f} reads/s")

    finally:
      remove_client_logfile(file_sink_id)

# test the case when we are writing to a non-leader server and constantly reading from that non-leader server
  def test_non_leader_write_non_leader_read(self):
    logfile_name = f"logs/{self._testMethodName}.log"
    if os.path.exists(logfile_name):
        os.remove(logfile_name)
    file_sink_id = set_client_logfile(logfile_name)

    # Initialize counters
    self.test_duration = 2 # seconds
    self.total_sets = 0
    self.total_gets = 0
    self.lock = threading.Lock()

    non_leader = 1

    def setter(client: StarClient, worker_name: str, server_number: Optional[int] = None):
      logger_instance = client_logger.bind(server_name=worker_name)
      sets = 0
      start_time = time.time()
      while True:
        with self.lock:
          logger_instance.info(f"Setting key = {sets}")
          client.set("key", str(sets), server_number)
          logger_instance.info(f"Set key = {sets}")
        sets += 1
        
        if time.time() - start_time >= self.test_duration:
          break
        time.sleep(0.01)
      with self.lock:
        self.total_sets += sets

    def getter(client: StarClient, worker_name: str, server_number: Optional[int] = None):
      logger_instance = client_logger.bind(server_name=worker_name)
      gets = 0
      start_time = time.time()
      while True:
        logger_instance.info(f"Getting key")
        status, val = client.get("key", server_number)
        self.assertTrue(status, msg=val)
        logger_instance.info(f"Get key = {val}")
        gets += 1
        if time.time() - start_time >= self.test_duration:
          break
      with self.lock:
        self.total_gets += gets

    try:
      # Create 1 non leader client for setting values
      client1 = self.star.connect(1)
      # Create 1 non leader client for reading values
      client2 = self.star.connect(2) 
      
      # Create and start threads
      setter_thread = threading.Thread(target=setter, args=(client1, "worker_1", non_leader))
      getter_thread = threading.Thread(target=getter, args=(client2, "worker_2", non_leader))

      start_time = time.time()  

      setter_thread.start()
      getter_thread.start()

      # Let threads run for test duration
      while time.time() - start_time < self.test_duration:
        time.sleep(1)

      # Join threads
      setter_thread.join()
      getter_thread.join()

      # Calculate throughput
      total_time = time.time() - start_time
      writes_per_second = self.total_sets / total_time
      reads_per_second = self.total_gets / total_time

      print(f"Total number of sets: {self.total_sets}")
      print(f"Total number of gets: {self.total_gets}")
      print(f"Write throughput: {writes_per_second:.2f} writes/s")
      print(f"Read throughput: {reads_per_second:.2f} reads/s")

    finally:
      remove_client_logfile(file_sink_id)

# test the case when we are constantly writing to all servers and then constantly reading from a leader and a non-leader server
  def test_all_write_leader_non_leader_read(self):
    logfile_name = f"logs/{self._testMethodName}.log"
    if os.path.exists(logfile_name):
        os.remove(logfile_name)
    file_sink_id = set_client_logfile(logfile_name)

    # Initialize counters
    self.test_duration = 2 # seconds
    self.total_sets = 0
    self.total_gets = 0
    self.lock = threading.Lock()

    leader_server = 3

    def setter(client: StarClient, worker_name: str, server_number: Optional[int] = None):
      logger_instance = client_logger.bind(server_name=worker_name)
      sets = 0
      start_time = time.time()
      while True:
        with self.lock:
          logger_instance.info(f"Setting key = {sets}")
          client.set("key", str(sets), server_number)
          logger_instance.info(f"Set key = {sets}")
        sets += 1
        
        if time.time() - start_time >= self.test_duration:
          break
        time.sleep(0.01)
      with self.lock:
        self.total_sets += sets

    def getter(client: StarClient, worker_name: str, server_number: Optional[int] = None):
      logger_instance = client_logger.bind(server_name=worker_name)
      gets = 0
      start_time = time.time()
      while True:
        logger_instance.info(f"Getting key")
        status, val = client.get("key", server_number)
        self.assertTrue(status, msg=val)
        logger_instance.info(f"Get key = {val}")
        gets += 1
        if time.time() - start_time >= self.test_duration:
          break
      with self.lock:
        self.total_gets += gets

    try:
      # Create setting clients
      clients = [self.star.connect(i) for i in range(5)]

      # Create 2 getting clients
      client5 = self.star.connect(5)
      client6 = self.star.connect(6)


      # Create setter threads 
      setter_threads = [
        threading.Thread(target=setter, args=(clients[i], f"worker_{i+1}", i))
        for i in range(5)
      ]

      # Create getter threads
      leader_getter_thread = threading.Thread(target=getter, args=(client5, "worker_6", leader_server))
      nonleader_getter_thread = threading.Thread(target=getter, args=(client6, "worker_7"))

      start_time = time.time()

      for s_thread in setter_threads:
        s_thread.start()
      leader_getter_thread.start()
      nonleader_getter_thread.start()

      # Let threads run for test duration
      while time.time() - start_time < self.test_duration:
        time.sleep(1)

      # Join threads
      for s_thread in setter_threads:
        s_thread.join()
      leader_getter_thread.join()
      nonleader_getter_thread.join()

      # Calculate throughput
      total_time = time.time() - start_time
      writes_per_second = self.total_sets / total_time
      reads_per_second = self.total_gets / total_time

      print(f"Total number of sets: {self.total_sets}")
      print(f"Total number of gets: {self.total_gets}")
      print(f"Write throughput: {writes_per_second:.2f} writes/s")
      print(f"Read throughput: {reads_per_second:.2f} reads/s")

    finally:
      remove_client_logfile(file_sink_id)

# test linearisability on multiple key values
  def test_multiple_keys(self) -> None:
      # Redirect the logs to a file
      logfile_name = f"logs/{self._testMethodName}.log"
      if os.path.exists(logfile_name):
          os.remove(logfile_name)
      file_sink_id = set_client_logfile(logfile_name)

      self.test_duration = 10 # seconds
      # Counters to keep track of operations
      self.total_sets = 0
      self.total_gets = 0
      self.lock = threading.Lock()
      key_vals = ['key1', 'key2', 'key3']

      def setter(c: StarClient, name: str, key: str) -> None:
        logger_instance = client_logger.bind(server_name=name)
        start_time = time.time()
        sets = 0
        while True:
          with self.lock:
            logger_instance.info(f"Setting {key} = {sets}")
            c.set(f"{key}", f"{sets}")
            logger_instance.info(f"Set {key} = {sets}")
          sets += 1
          # Check if the time window has elapsed
          if time.time() - start_time >= self.test_duration:
            break
          time.sleep(0.01)
        with self.lock:
          self.total_sets += sets

      def getter(c: StarClient, name: str) -> None:
        logger_instance = client_logger.bind(server_name=name)
        start_time = time.time()
        gets = 0
        while True:
          key = random.choice(key_vals)
          logger_instance.info(f"Getting {key}")
          status, val = c.get(f"{key}")
          self.assertTrue(status, msg=val)
          logger_instance.info(f"Get {key} = {val}")
          gets += 1
          # Check if the time window has elapsed
          if time.time() - start_time >= self.test_duration:
            break
        with self.lock:
          self.total_gets += gets

      try:
        # Connect clients
        # client1 = self.star.connect(1)
        num_getters = 3
        clients = [self.star.connect(i) for i in range(num_getters)]

        num_setters = 3
        clients_setter = [self.star.connect(j+num_getters) for j in range(num_setters)]

        # Set the initial value
        # client1.set("key", "0")

        # start num_setters threads for setters
        setter_threads = [
            threading.Thread(target=setter, args=(clients_setter[i], f"worker_{i+1}", f"key{i+1}"))
            for i in range(num_setters)
        ]

        # Start num_getters threads for getter
        getter_threads = [
            threading.Thread(target=getter, args=(clients[i], f"worker_{i+1+num_getters}"))
            for i in range(num_getters)
        ]

        # Record the start time
        start_time = time.time()

        # Start the threads
        for s_thread in setter_threads:
          s_thread.start()
        for g_thread in getter_threads:
            g_thread.start()

        # Let the threads run for test_duration
        while time.time() - start_time < self.test_duration:
            time.sleep(1)

        # Join the threads
        for s_thread in setter_threads:
            s_thread.join()
        for g_thread in getter_threads:
            g_thread.join()

        # Calculate the throughput
        total_time = time.time() - start_time
        writes_per_second = self.total_sets / total_time
        reads_per_second = self.total_gets / total_time

        print(f"Total number of sets: {self.total_sets}")
        print(f"Total number of gets : {self.total_gets}")
        print(f"Write throughput: {writes_per_second:.2f} writes/s")
        print(f"Read throughput: {reads_per_second:.2f} reads/s")

      finally:
        remove_client_logfile(file_sink_id)



if __name__ == "__main__":
  unittest.main()
