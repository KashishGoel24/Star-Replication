import threading
import unittest
import os
import time
import random

from craq.craq_cluster import CraqCluster, CraqClient
from core.logger import client_logger
from core.logger import set_client_logfile, remove_client_logfile

class TestCRAQ(unittest.TestCase):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.craq = CraqCluster()

  def setUp(self) -> None:
    self.craq.start_all()

  def tearDown(self) -> None:
    self.craq.stop_all()


  def test_gen_history(self) -> None:
    # Redirect the logs to a file
    logfile_name = f"logs/{self._testMethodName}.log"
    if os.path.exists(logfile_name):
      os.remove(logfile_name)
    file_sink_id = set_client_logfile(logfile_name)

    def setter(c: CraqClient, iters: int, name: str) -> None:
      logger = client_logger.bind(server_name=name)
      for i in range(iters):
        logger.info(f"Setting key = {i}")
        c.set("key", f"{i}")
        logger.info(f"Set key = {i}")

    def getter(c: CraqClient, iters: int, name: str) -> None:
      logger = client_logger.bind(server_name=name)
      for _ in range(iters):
        logger.info(f"Getting key")
        status, val = c.get("key")
        self.assertTrue(status, msg=val)
        logger.info(f"Get key = {val}")

    try:
      client1 = self.craq.connect()
      client2 = self.craq.connect()

      client1.set("key", "0")
      s = threading.Thread(target=setter, args=(client1, 10, "worker_0"))
      g = threading.Thread(target=getter, args=(client2, 10, "worker_1"))
      s.start()
      g.start()
      s.join()
      g.join()
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

    def setter(c: CraqClient, name: str) -> None:
      logger_instance = client_logger.bind(server_name=name)
      start_time = time.time()
      sets = 0
      while True:
        logger_instance.info(f"Setting key = {self.total_sets}")
        c.set("key", f"{self.total_sets}")
        logger_instance.info(f"Set key = {self.total_sets}")
        sets += 1
        # Check if the time window has elapsed
        if time.time() - start_time >= self.test_duration:
          break
        time.sleep(0.01)
      with self.lock:
        self.total_sets += sets

    def getter(c: CraqClient, name: str) -> None:
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
      client1 = self.craq.connect()
      num_getters = 8
      clients = [self.craq.connect() for _ in range(num_getters)]

      # Set the initial value
      client1.set("key", "0")

      # Start num_getters threads for getter
      getter_threads = [
          threading.Thread(target=getter, args=(clients[i], f"worker_{i+1}"))
          for i in range(num_getters)
      ]

      # Record the start time
      start_time = time.time()

      # Start the threads
      for g_thread in getter_threads:
          g_thread.start()

      # Let the threads run for test_duration
      while time.time() - start_time < self.test_duration:
          time.sleep(1)

      # Join the threads
      for g_thread in getter_threads:
          g_thread.join()

      # Calculate the throughput
      total_time = time.time() - start_time
      writes_per_second = self.total_sets / total_time
      reads_per_second = self.total_gets / total_time

      print(f"Write throughput: {writes_per_second:.2f} writes/s")
      print(f"Read throughput: {reads_per_second:.2f} reads/s")

    finally:
      remove_client_logfile(file_sink_id)

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

    def setter(c: CraqClient, name: str, key: str) -> None:
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

    def getter(c: CraqClient, name: str) -> None:
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
      # client1 = self.craq.connect(1)
      num_getters = 4
      clients = [self.craq.connect() for i in range(num_getters)]

      num_setters = 8
      clients_setter = [self.craq.connect() for j in range(num_setters)]

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
