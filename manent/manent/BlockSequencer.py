

class BlockSequencer:
  def __init__(self):
    self.current_open_container = None
    self.aside_block_db = None
    self.piggyback_headers_db = None
  def add_block(self, digest, code, data):
    # Check if we need to put the current block aside.
    if BlockManager.is_cached(code):
      self.add_aside_block(digest, code, data)
      return
    # We're writing this block. Make sure we have a container that can accept
    # it.
    if self.current_open_container is None:
      self.current_open_container = self.open_container()
    else:
      # The container can be filled by aside data, so we might need several
      # attempts of container creation.
      while not self.current_open_container.can_add(data):
        self.write_container(self.current_open_container)
        self.current_open_container = self.open_container()
    # Ok, a container is ready.
    self.current_open_container.add_block(digest, code, data)
  def add_aside_block(self, digest, code, data):
    key = str(self.next_put_aside_block_idx)
    self.next_put_aside_block_idx += 1
    self.aside_block_db[key] = digest

    self.num_aside_blocks += 1
    self.size_aside_blocks += len(data)
  def write_container(self, container):
    # 1. Get the header out of the container and store it here for piggybacking.
    # 2. Ask the container to upload itself.
    header = container.read_header()
    self.storage_manager.write_container(container)
  def open_container(self, container):
    # 1. Ask the storage to create a new empty container.
    # 2. Push into the container as many piggybacking blocks as it's willing to
    # accept.
    # 3. If the container can be filled by currently collected aside blocks,
    # write them out to the container, write the container out and open a new
    # one again.
    pass
