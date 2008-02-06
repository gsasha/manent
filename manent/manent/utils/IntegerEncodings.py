#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import string
import cStringIO as StringIO

def str_base( number, radix ):
   """str_base( number, radix ) -- reverse function to int(str,radix) and long(str,radix)"""

   if not 2 <= radix <= 36:
      raise ValueError("radix must be in 2..36")

   abc = string.digits + string.letters

   result = ''

   if number < 0:
      number = -number
      sign = '-'
   else:
      sign = ''

   while True:
      number, rdigit = divmod( number, radix )
      result = abc[rdigit] + result
      if number == 0:
         return sign + result

def ascii_encode_int_varlen(num):
	"""
	Variable-length coding of numbers that will appear in correct order when
	sorted lexicographically.
	"""
	if num < 0:
		prefix = 'a'
		num = -num
	else:
		prefix = 'n'
	s = str_base(num,36)
	return chr(ord(prefix)-1+len(s))+s

def ascii_decode_int_varlen(s):
	"""
	Decoding for the above format.
	"""
	if len(s)==0:
		raise ValueError("empty input")
	if ord(s[0])-ord('n') >= 0:
		sign = 1
		prefix = 'n'
	else:
		sign = -1
		prefix = 'a'

	if ord(s[0])-ord(prefix) != len(s)-2:
		raise ValueError("malformed ascii int encoding of %s: %d != %d" %(s, ord(s[0])-ord(prefix),len(s)-2))
	return sign*int(s[1:],36)

def ascii_read_int_varlen(file):
	"""
	Reading of the above coded number from a file
	"""
	first_byte = file.read(1)
	if len(first_byte) == 0:
		# end of file
		return None
	if ord(first_byte)>=ord('n'):
		prefix = 'n'
		sign = 1
	else:
		prefix = 'a'
		sign = -1
	bytes = file.read(ord(first_byte)-ord(prefix)+1)
	return ascii_decode_int_varlen(first_byte+bytes)

def ascii_read_int_varlen_list(file):
	"""
	Read the whole list of integers until the file ends
	"""
	result = []
	while True:
		num = ascii_read_int_varlen(file)
		if num is None:
			return result
		result.append(num)

def binary_encode_int_varlen(num):
	"""
	Variable-length coding of nonnegative numbers that includes the length symbol, so that
	it is stored quite compactly. The first byte is the number of bytes in the code,
	so a list of such numbers can be read serially from a file.

	The encoding goes as follows:

	For numbers -64<=x<=63, the encoding is exactly one byte, the number itself (MSBit is 0)
	For larger numbers, the first byte specifies the length (the MSBit or the byte is 1 to denote this case).
	The rest of the bytes contain the encoding of the number, from most significant to less significant.

	The mapping is as follows:
	-INF..-64 -> len+encoding(abs)       first byte: 0..63
	-64..63   -> (128+num)               first byte: 64..191
	64..INF   -> (192+len)+encoding(num) first byte: 192..255
	This encoding still preserves lexicographical order of unsigned characters!
	"""
	if num >= -64 and num < 64:
		return chr(num+128)
	if num < 0:
		num = -num
		len_base = 0
	else:
		len_base = 192
	
	bytes = []
	while num != 0:
		bytes.append(chr(num%256))
		num /= 256
	bytes.reverse()
	return chr(len(bytes)+len_base-1)+"".join(bytes)

def binary_decode_int_varlen(s):
	"""
	Decoding for the above format
	"""
	if len(s)==0:
		raise ValueError("empty input")
	l = ord(s[0])
	if l >= 64 and l < 192:
		return l-128
	if l < 64:
		sign = -1
	else:
		sign = 1
		l -= 192
	
	if l != (len(s)-2):
		print "bad string ",
		for ch in s:
			print ord(ch),
		print
		raise ValueError("malformed binary int encoding")
	res = 0
	for byte in s[1:]:
		res *= 256
		res += ord(byte)
	return res*sign

def binary_read_int_varlen(file):
	"""
	Read one integer in the above encoding
	"""
	first_byte = file.read(1)
	if len(first_byte) == 0:
		# End of file
		return None
	l = ord(first_byte)
	if l >= 64 and l < 192:
		return binary_decode_int_varlen(first_byte)
	if l >= 192:
		l -= 192
	bytes = file.read(l+1)
	return binary_decode_int_varlen(first_byte+bytes)

def binary_read_int_varlen_list(file):
	"""
	Read the whole list of integers until the file ends
	"""
	result = []
	while True:
		num = binary_read_int_varlen(file)
		if num is None:
			return result
		result.append(num)

def binary_write_int_varlen_list(data, file):
	for d in data:
		file.write(binary_encode_int_varlen(d))

def binary_decode_int_varlen_list(data):
	"""
	Decode a given string to list of integers
	"""
	stream = StringIO.StringIO(data)
	return binary_read_int_varlen_list(stream)

def binary_encode_int_varlen_list(data):
	stream = StringIO.StringIO()
	binary_write_int_varlen_list(data, stream)
	return stream.getvalue()
