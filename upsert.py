import os
import mmap
from collections import namedtuple

def new_upsert(rowbits=5, limit=1048575):
  rowsize = (1 << rowbits) & limit
  name = "Upsert_{0:d}".format(rowbits)
  Upsert = namedtuple(name, [
    "upsert_mmap",
    "upsert_fd",
  ])
  def upsert_mmap(m, new_fd=-1, req_dir_fd=-1, requests=iter([])):
    return m
  def upsert_fd(old_fd=-1, new_fd=-1, req_dir_fd=-1, requests=iter([])):
    s = os.fstat(old_fd)
    f = filter(lambda _: 0 < s, [ s ])
    maps = map(lambda _: mmap.mmap(old_fd, s, mmap.MAP_PRIVATE, mmap.PROT_READ), f)
    upserts = map(lambda m: upsert_mmap(m, new_fd, req_dir_fd, requests), maps)
    closes = map(operator.methodcaller("close"), upserts)
    return sum(1 for _ in closes)
  return Upsert(upsert_fd)

