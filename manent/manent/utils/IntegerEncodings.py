def ascii_encode_int_varlen(num):
	"""
	Variable-length coding of numbers that will appear in correct order when
	sorted lexicographically.
	"""
	if num < 0:
		raise ValueError("negative numbers not supported: %s" % str(num))
	s = "%x"%num
	return chr(ord('a')-1+len(s))+s

def ascii_decode_int_varlen(s):
	"""
	Decoding for the above format.
	"""
	if len(s)==0:
		raise ValueError("empty input")
	if ord(s[0])-ord('a') != len(s)-2:
		raise ValueError("malformed ascii int encoding: %d != %d" %(ord(s[0])-ord('a'),len(s)-2))
	return int(s[1:],16)

def ascii_read_int_varlen(file):
	"""
	Reading of the above coded number from a file
	"""
	first_byte = file.read(1)
	if len(first_byte) == 0:
		# end of file
		return None
	bytes = file.read(ord(first_byte)-ord('a')+1)
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

	For numbers 0<=x<=127, the encoding is exactly one byte, the number itself (MSBit is 0)
	For larger numbers, the first byte specifies the length (the MSBit or the byte is 1 to denote this case).
	The rest of the bytes contain the encoding of the number, from most significant to less significant.

	This encoding still preserves lexicographical order!
	"""
	if num < 0:
		raise ValueError("Negative numbers are not supported")
	if num < 128:
		return chr(num)
	bytes = []
	while num != 0:
		bytes.append(chr(num%256))
		num /= 256
	bytes.reverse()
	return chr(len(bytes)+128-1)+"".join(bytes)

def binary_decode_int_varlen(s):
	"""
	Decoding for the above format
	"""
	if len(s)==0:
		raise ValueError("empty input")
	l = ord(s[0])
	if l < 128:
		return l
	l -= 128
	if l != (len(s)-2):
		raise ValueError("malformed binary int encoding")
	res = 0
	for byte in s[1:]:
		res *= 256
		res += ord(byte)
	return res

def binary_read_int_varlen(file):
	"""
	Read one integer in the above encoding
	"""
	first_byte = file.read(1)
	if len(first_byte) == 0:
		# End of file
		return None
	if ord(first_byte) < 128:
		return ord(first_byte)
	bytes = file.read(ord(first_byte)-128+1)
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
