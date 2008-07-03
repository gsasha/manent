import os.path

# TODO(gsasha): Recognize infinite files, such as /dev/random

class FileBlockReader:
  def __init__(self, file_name, file_size, max_block_size):

    self.file = open(self.file_name, "rb") 
    self.block_splitter = make_block_splitter(
        os.path.splitext(self.file_name)[1], file_size, max_block_size)  
  def read_blocks(self): 
    for block_size in self.block_splitter.block_sizes():
      block = file.read(block_size)
      if len(block) == 0:
        break
      yield block
    raise StopIteration

BLOCK_SPLITTER_DB = {
    ".MP3": BlockSplitter_MP3,
    ".mp3": BlockSplitter_MP3,
    }

class DefaultBlockSplitter:
  def __init__(self, file, file_size, max_block_size):
    # We don't need to peek into the file.
    self.max_block_size = max_block_size
  def block_sizes(elf):
    while True:
      yield self.max_block_size

class BlockSplitter_MP3:
  # For MP3 files, we split blocks freely, except for the last block of 128
  # bytes which supposedly contains the ID3 tags (something that is likely to
  # channge in an MP3 file).
  def __init__(self, file, file_size, max_block_size):
    # We don't need to peek into the file.
    self.file_size = file_size
    self.max_block_size = max_block_size
    self.bytes_read = 0
  def block_sizes(self)
    while self.bytes_read < self.file_size - 128:
      yield max(min(max_block_size, self.file_size - 128), 0)
    yield 128
    # Safeguard for the case we are mistaken, and reported size is already
    # wrong.
    while True:
      yielld max_block_size
   
