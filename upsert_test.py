import unittest
import os
import struct
import mmap
import operator
from collections import namedtuple

import upsert

class fd2rows(unittest.TestCase):
  def test_empty(self):
    os.makedirs("./.test/upsert/fd2rows/empty", exist_ok=True)
    d = os.open("./.test/upsert/fd2rows/empty", os.O_DIRECTORY)

    fd = os.open("empty.dat", os.O_CREAT | os.O_TRUNC | os.O_RDWR, 0o644, dir_fd=d)
    a = upsert.fd2rows(fd)
    self.assertEqual(None, next(a, None))
    os.close(fd)

    os.close(d)
  def test_human5(self):
    os.makedirs("./.test/upsert/fd2rows/human5", exist_ok=True)
    d = os.open("./.test/upsert/fd2rows/human5", os.O_DIRECTORY)

    fd = os.open("human5.dat", os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o644, dir_fd=d)
    s = struct.Struct("<16sbbHfd")
    h = namedtuple("Human", [
      "name",
      "flag",
      "age",
      "height",
      "weight",
      "updated",
    ])
    with os.fdopen(fd, "wb") as f:
      f.write(s.pack(*h(
        name=b"0123456789abcdef",
        flag=0,
        age=34,
        height=1750,
        weight=65.125,
        updated=1591755942.4352214,
      )))
      f.write(s.pack(*h(
        name=b"0123456789abcdef",
        flag=0,
        age=35,
        height=1750,
        weight=65.375,
        updated=1598455942.4352214,
      )))
      pass
    fd = os.open("human5.dat", os.O_RDONLY, dir_fd=d)
    a = upsert.fd2rows(fd, s=s)
    self.assertEqual(next(a), h(
      name=b"0123456789abcdef",
      flag=0,
      age=34,
      height=1750,
      weight=65.125,
      updated=1591755942.4352214,
    ))
    self.assertEqual(next(a), h(
      name=b"0123456789abcdef",
      flag=0,
      age=35,
      height=1750,
      weight=65.375,
      updated=1598455942.4352214,
    ))
    self.assertEqual(None, next(a, None))
    os.close(fd)

    os.close(d)
  pass

class map2iter(unittest.TestCase):
  def test_empty(self): self.assertEqual(None, next(upsert.map2iter(), None))
  def test_human7(self):
    m = bytearray(bytes(128*3))
    s = struct.Struct("<64sBBHhBBqqqQQQd")
    h = namedtuple("Human", [
      "buffer",
      "offset",
      "name",
      "flag",
      "age",
      "height",
      "year",
      "month",
      "day",
      "legs",
      "arms",
      "fingers",
      "weight",
      "width",
      "depth",
      "updated",
    ])

    s.pack_into(*h(
      buffer=m,
      offset=128*0,
      name=bytes(64),
      flag=0,
      age=33,
      height=1750,
      year=1986,
      month=8,
      day=23,
      legs=2,
      arms=2,
      fingers=0x0a,
      weight=65536,
      width=65536,
      depth=32768,
      updated=1591755942.125,
    ))
    s.pack_into(*h(
      buffer=m,
      offset=128*1,
      name=bytes(64),
      flag=0,
      age=34,
      height=1750,
      year=1985,
      month=8,
      day=23,
      legs=2,
      arms=2,
      fingers=0x0a,
      weight=65536,
      width=65536,
      depth=32768,
      updated=1591755942.375,
    ))
    s.pack_into(*h(
      buffer=m,
      offset=128*2,
      name=bytes(64),
      flag=0,
      age=35,
      height=1750,
      year=1984,
      month=8,
      day=23,
      legs=2,
      arms=2,
      fingers=0x0a,
      weight=65536,
      width=65536,
      depth=32768,
      updated=1591755942.625,
    ))

    a = upsert.map2iter(m, rowbits=7, s=s)

    self.assertEqual(next(a), (
      bytes(64),
      0,
      33,
      1750,
      1986,
      8,
      23,
      2,
      2,
      0x0a,
      65536,
      65536,
      32768,
      1591755942.125,
    ))
    self.assertEqual(next(a), (
      bytes(64),
      0,
      34,
      1750,
      1985,
      8,
      23,
      2,
      2,
      0x0a,
      65536,
      65536,
      32768,
      1591755942.375,
    ))
    self.assertEqual(next(a), (
      bytes(64),
      0,
      35,
      1750,
      1984,
      8,
      23,
      2,
      2,
      0x0a,
      65536,
      65536,
      32768,
      1591755942.625,
    ))

    self.assertEqual(next(a,None), None)

class names2rows(unittest.TestCase):
  def test_empty(self): self.assertEqual(None, next(upsert.names2rows(),None))
  def test_users(self):
    os.makedirs("./.test/upsert/names2rows/users", exist_ok=True)
    d = os.open("./.test/upsert/names2rows/users", os.O_DIRECTORY)

    u1a = os.open("./user1.a.dat", os.O_CREAT|os.O_TRUNC|os.O_RDWR, 0o644, dir_fd=d)
    u1b = os.open("./user1.b.dat", os.O_CREAT|os.O_TRUNC|os.O_RDWR, 0o644, dir_fd=d)
    u1c = os.open("./user1.c.dat", os.O_CREAT|os.O_TRUNC|os.O_RDWR, 0o644, dir_fd=d)

    for fd in [u1a, u1b, u1c]: os.ftruncate(fd, 32)
    m1a = mmap.mmap(u1a, 32, mmap.MAP_SHARED, mmap.PROT_WRITE | mmap.PROT_READ)
    m1b = mmap.mmap(u1b, 32, mmap.MAP_SHARED, mmap.PROT_WRITE | mmap.PROT_READ)
    m1c = mmap.mmap(u1c, 32, mmap.MAP_SHARED, mmap.PROT_WRITE | mmap.PROT_READ)
    for fd in [u1a, u1b, u1c]: os.close(fd)

    s = struct.Struct("<16sbbhfd")
    h = namedtuple("Human", [
      "name",
      "flag",
      "age",
      "height",
      "weight",
      "updated",
    ])

    m1a[0:32] = s.pack(*h(
      name=b"0123456789abcdef",
      flag=0,
      age=34,
      height=1750,
      weight=65.125,
      updated=1591755942.125,
    ))
    m1b[0:32] = s.pack(*h(
      name=b"0123456789abcdef",
      flag=0,
      age=34,
      height=1750,
      weight=64.125,
      updated=1591755949.125,
    ))
    m1c[0:32] = s.pack(*h(
      name=b"0123456789abcdef",
      flag=0,
      age=34,
      height=1750,
      weight=63.125,
      updated=1591755969.125,
    ))

    for m in [m1a, m1b, m1c]: m.flush()
    for m in [m1a, m1b, m1c]: m.close()

    names = iter([ "user1.a.dat", "user1.b.dat", "user1.c.dat" ])

    a = upsert.names2rows(d, names, s=s, key=operator.itemgetter(0))

    self.assertEqual(next(a), h(
      name=b"0123456789abcdef",
      flag=0,
      age=34,
      height=1750,
      weight=65.125,
      updated=1591755942.125,
    ))
    self.assertEqual(next(a), h(
      name=b"0123456789abcdef",
      flag=0,
      age=34,
      height=1750,
      weight=64.125,
      updated=1591755949.125,
    ))
    self.assertEqual(next(a), h(
      name=b"0123456789abcdef",
      flag=0,
      age=34,
      height=1750,
      weight=63.125,
      updated=1591755969.125,
    ))

    self.assertEqual(next(a,None), None)

    os.close(d)
    pass
