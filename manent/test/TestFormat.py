import unittest
import random
from cStringIO import StringIO

from manent.Format import *

class TestFormat(unittest.TestCase):
	def setUp(self):
		self.nums = range(10000)+range(100000,100100)+range(100000000,100000100)
		random.shuffle(self.nums)

	def test_ascii_encode(self):
		for i in self.nums:
			num = random.randint(0,i)
			self.assertEqual(num, ascii_decode_int_varlen(ascii_encode_int_varlen(num)))

	def test_ascii_stream_encode(self):
		ostream = StringIO()
		for num in self.nums:
			ostream.write(ascii_encode_int_varlen(num))
		istream = StringIO(ostream.getvalue())
		read_nums = ascii_read_int_varlen_list(istream)
		self.assertEqual(self.nums,read_nums)
	
	def test_ascii_bad_input(self):
		# Negative numbers are not supported
		self.assertRaises(ValueError,ascii_encode_int_varlen,-1)
		self.assertRaises(ValueError,ascii_encode_int_varlen,-100)
		# Test badly encoded numbers
		self.assertRaises(ValueError,ascii_decode_int_varlen,"")
		for num in self.nums:
			enc = ascii_encode_int_varlen(num)
			self.assertRaises(ValueError,ascii_decode_int_varlen, enc+"1")
			self.assertRaises(ValueError,ascii_decode_int_varlen, enc[0:-1])
	
	def test_ascii_order(self):
		"""
		Make sure that the encoded numbers preserve the lexicographical ordering
		"""
		for i in self.nums:
			num1 = random.randint(0,i)
			num2 = random.randint(0,i)
			if num1 > num2:
				(num1,num2) = (num2,num1)
			num1_enc = ascii_encode_int_varlen(num1)
			num2_enc = ascii_encode_int_varlen(num1)
			self.assert_(num1_enc <= num2_enc)

	def test_binary_encode(self):
		for i in self.nums:
			num = random.randint(0,i)
			self.assertEqual(num, binary_decode_int_varlen(binary_encode_int_varlen(num)))

	def test_binary_stream_encode(self):
		nums = self.nums
		ostream = StringIO()
		for num in nums:
			ostream.write(binary_encode_int_varlen(num))
		istream = StringIO(ostream.getvalue())
		read_nums = binary_read_int_varlen_list(istream)
		self.assertEqual(nums,read_nums)
	
	def test_binary_bad_input(self):
		# Negative numbers are not supported
		self.assertRaises(ValueError,binary_encode_int_varlen,-1)
		self.assertRaises(ValueError,binary_encode_int_varlen,-100)
		# Test badly encoded numbers
		self.assertRaises(ValueError,binary_decode_int_varlen,"")
		for num in self.nums:
			enc = ascii_encode_int_varlen(num)
			self.assertRaises(ValueError,ascii_decode_int_varlen, enc+"1")
			self.assertRaises(ValueError,ascii_decode_int_varlen, enc[0:-1])
	
	def test_binary_order(self):
		"""
		Make sure that the encoded numbers preserve the lexicographical ordering
		"""
		for i in self.nums:
			num1 = random.randint(0,i)
			num2 = random.randint(0,i)
			if num1 > num2:
				(num1,num2) = (num2,num1)
			num1_enc = binary_encode_int_varlen(num1)
			num2_enc = binary_encode_int_varlen(num1)
			self.assert_(num1_enc <= num2_enc)
