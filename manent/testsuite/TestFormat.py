import random
import cStringIO as StringIO
import unittest

import manent.utils.IntegerEncodings as IE

class TestFormat(unittest.TestCase):
	def setUp(self):
		self.nums = range(-10000,10000) +range(100000,100100)+ range(-100100,-100100)+ range(100000000,100000100)
		random.shuffle(self.nums)

	def test_ascii_encode(self):
		for i in self.nums:
			self.assertEqual(i, IE.ascii_decode_int_varlen(IE.ascii_encode_int_varlen(i)))
			num = random.randint(-abs(i),abs(i))
			self.assertEqual(num, IE.ascii_decode_int_varlen(IE.ascii_encode_int_varlen(num)))

	def test_ascii_stream_encode(self):
		ostream = StringIO.StringIO()
		for num in self.nums:
			ostream.write(IE.ascii_encode_int_varlen(num))
		istream = StringIO.StringIO(ostream.getvalue())
		read_nums = IE.ascii_read_int_varlen_list(istream)
		self.assertEqual(self.nums,read_nums)
	
	def test_ascii_bad_input(self):
		# Test badly encoded numbers
		self.assertRaises(ValueError, IE.ascii_decode_int_varlen, "")
		for num in self.nums:
			enc = IE.ascii_encode_int_varlen(num)
			self.assertRaises(ValueError, IE.ascii_decode_int_varlen, enc+"1")
			self.assertRaises(ValueError, IE.ascii_decode_int_varlen, enc[0:-1])
	
	def test_ascii_order(self):
		"""
		Make sure that the encoded numbers preserve the lexicographical ordering
		"""
		for i in self.nums:
			num1 = random.randint(-abs(i), abs(i))
			num2 = random.randint(-abs(i), abs(i))
			if num1 > num2:
				(num1, num2) = (num2, num1)
			num1_enc = IE.ascii_encode_int_varlen(num1)
			num2_enc = IE.ascii_encode_int_varlen(num1)
			self.assert_(num1_enc <= num2_enc)

	def test_binary_encode(self):
		for i in self.nums:
			self.assertEqual(i, IE.binary_decode_int_varlen(IE.binary_encode_int_varlen(i)))
			num = random.randint(-abs(i), abs(i))
			self.assertEqual(num, IE.binary_decode_int_varlen(IE.binary_encode_int_varlen(num)))
			s = StringIO.StringIO(IE.binary_encode_int_varlen(i))
			res = IE.binary_read_int_varlen(s)
			self.assertEqual(res, i)

	def test_binary_stream_encode(self):
		nums = self.nums
		ostream = StringIO.StringIO()
		for num in nums:
			ostream.write(IE.binary_encode_int_varlen(num))
		istream = StringIO.StringIO(ostream.getvalue())
		read_nums = IE.binary_read_int_varlen_list(istream)
		self.assertEqual(nums,read_nums)
	
	def test_binary_bad_input(self):
		# Test badly encoded numbers
		self.assertRaises(ValueError, IE.binary_decode_int_varlen, "")
		for num in self.nums:
			enc = IE.ascii_encode_int_varlen(num)
			self.assertRaises(ValueError, IE.ascii_decode_int_varlen, enc+"1")
			self.assertRaises(ValueError, IE.ascii_decode_int_varlen, enc[0:-1])
	
	def test_binary_order(self):
		"""
		Make sure that the encoded numbers preserve the lexicographical ordering
		"""
		for i in self.nums:
			num1 = random.randint(-abs(i),abs(i))
			num2 = random.randint(-abs(i),abs(i))
			if num1 > num2:
				(num1,num2) = (num2,num1)
			num1_enc = IE.binary_encode_int_varlen(num1)
			num2_enc = IE.binary_encode_int_varlen(num1)
			self.assert_(num1_enc <= num2_enc)
