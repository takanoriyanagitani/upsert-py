import os
import mmap
import struct
import operator
import heapq
import sys
from functools import partial, reduce
from itertools import islice, groupby, chain

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
  for row in merged: yield row
  clsm = map(operator.methodcaller("close"), maps)
  mcnt = sum(1 for _ in clsm)
  pass

def tup2write(t=tuple(), w=sys.stdout.write, s=struct.Struct("<QQqq"), buf=bytearray(bytes(32))):
  s.pack_into(*tuple(chain(
    (buf, 0),
    t,
  )))
  w(buf)

def sub(
  old_fd=-1,
  new_fd=-1,
  request_dir=-1,
  request_names=iter([]),
  s=struct.Struct("<QQqq"),
  key=None,
  rowbits=5,
  rdc=None,
  alt=None,
  mfunc=None,
):
  rows_old = fd2rows(old_fd, rowbits, s)
  rows_new = names2rows(request_dir, request_names, s, key, rowbits)
  merged = heapq.merge(*[rows_old, rows_new], key=key)
  grouped = groupby(merged, key=key)
  reduced = map(lambda g: reduce(rdc, g[1], alt), grouped)
  mapped  = map(mfunc, reduced)
  buf = bytearray(bytes(1 << rowbits))
  with os.fdopen(new_fd, "wb") as f:
    w = f.write
    writes = map(partial(tup2write, w=w, s=s, buf=buf), mapped)
    wcnt = sum(1 for _ in writes)
    f.flush()
    os.fdatasync(f.fileno())
    return os.dup(f.fileno())
