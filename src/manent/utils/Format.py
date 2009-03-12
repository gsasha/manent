import IntegerEncodings as IE
import cStringIO as StringIO

#
# Value reading/writing
#
def write_int(file, num):
  file.write(IE.binary_encode_int_varlen(num))

def read_int(file):
  return IE.binary_read_int_varlen(file)

def read_ints(file):
  return IE.binary_read_int_varlen_list(file)

def write_ints(file, nums):
  for num in nums:
    file.write(IE.binary_encode_int_varlen(num))

def deserialize_ints(string):
  sio = StringIO.StringIO(string)
  return read_ints(sio)

def serialize_ints(nums):
  sio = StringIO.StringIO()
  write_ints(sio, nums)
  return sio.getvalue()

def write_string(file,str):
  """
  Write a Pascal-encoded string of length of up to 2^16
  """
  file.write(IE.binary_encode_int_varlen(len(str)))
  file.write(str)

def read_string(file):
  """
  The reverse of write_string
  """
  length = IE.binary_read_int_varlen(file)
  return file.read(length)
