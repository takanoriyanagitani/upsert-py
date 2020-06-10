import os
import mmap
import struct
import operator
from functools import partial
from itertools import islice, groupby

def fd2rows(fd=-1, rowbits=5, s=struct.Struct("<IIIIiiii")):
  size = os.fstat(fd).st_size
  f = filter(lambda _: 0 < size, [ size ])
  maps = [ mmap.mmap(fd, size, mmap.MAP_PRIVATE, mmap.PROT_READ) for _ in f ]
  m = next(iter(maps), bytearray(bytes(0)))
  rowcnt = len(m) >> rowbits
  dbsize = rowcnt << rowbits
  i = s.iter_unpack(m[:dbsize])
  for row in i: yield row
  closes = map(operator.methodcaller("close"), maps)
  closecnt = sum(1 for _ in closes)
  pass

def map2iter(m=bytearray(bytes(0)), rowbits=5, s=struct.Struct("<IIIIiiii")):
  rowcnt = len(m) >> rowbits
  dbsize = rowcnt << rowbits
  return s.iter_unpack(m[:dbsize])

def names2rows(dir_fd=-1, names=iter([]), s=struct.Struct("<IIIIiiii"), key=None, rowbits=5):
  fds = [ os.open(name, os.O_RDONLY, dir_fd=dir_fd) for name in names ]
  mapped = map(lambda fd: (fd, os.fstat(fd)), fds)
  filtered = filter(lambda t: 0 < t[1].st_size, mapped)
  maps = [ mmap.mmap(t[0], t[1].st_size, mmap.MAP_PRIVATE, mmap.PROT_READ) for t in filtered ]
  clsf = map(lambda fd: os.close(fd), fds)
  fcnt = sum(1 for _ in clsf)
  iterators = map(partial(map2iter, rowbits=rowbits, s=s), maps)
  merged = heapq.merge(*iterators, key=key)
  for row in merged: yield merged
  clsm = map(operator.methodcaller("close"), maps)
  mcnt = sum(1 for _ in clsm)
  pass

def upsert(
  old_fd=-1,
  new_fd=-1,
  request_dir=-1,
  request_names=iter([]),
  fmt="<IIIIiiii",
  key=None,
  rowbits=5,
  rdc=None,
  alt=None,
):
  s = struct.Struct(fmt)
  rows_old = fd2rows(old_fd, rowbits, s)
  rows_new = names2rows(request_dir, request_names, s, key, rowbits)
  merged = heapq.merge(*[rows_old, rows_new], key=key)
  grouped = groupby(merged, key=key)
  reduced = map(lambda g: reduce(rdc, g[1], alt), grouped)
  with os.fdopen(fd, "wb") as f:
    w = f.write
    writes = map(partial(tup2write, w=w), reduced)
    wcnt = sum(1 for _ in writes)
    f.flush()
    os.fdatasync(f.fileno())
    return os.dup(f.fileno())
  pass
