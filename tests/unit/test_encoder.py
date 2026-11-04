import unittest
from cassandra.encoder import Encoder
from decimal import Decimal
import datetime
from uuid import UUID
import ipaddress
import types

class TestEncoder(unittest.TestCase):
    def setUp(self):
        self.encoder = Encoder()

    def test_add_or_override_mapping_new_type(self):
        """Test adding a new type to the mapping."""
        def custom_func(val):
            return f"custom_{val}"
        
        # Test adding a completely new type
        self.encoder.add_or_override_mapping(complex, custom_func)
        
        # Verify it's in the mapping
        mapping_types = [item[0] for item in self.encoder._mapping]
        self.assertIn(complex, mapping_types)
        
        # Verify it's at the end
        self.assertEqual(self.encoder._mapping[-1], (complex, custom_func))

    def test_add_or_override_mapping_override_existing(self):
        """Test overriding an existing type in the mapping."""
        def custom_int_func(val):
            return f"int_{val}"
        
        # Override 'int'
        self.encoder.add_or_override_mapping(int, custom_int_func)
        
        # Find the mapping for int
        mapping_item = next((item for item in self.encoder._mapping if item[0] == int), None)
        self.assertIsNotNone(mapping_item)
        self.assertEqual(mapping_item[1], custom_int_func)

    def test_add_or_override_mapping_subclass_priority(self):
        """Test that a subclass is inserted before its parent type."""
        class MyStr(str):
            pass
        
        def my_str_func(val):
            return f"my_str_{val}"
        
        # MyStr is a subclass of str. 
        # str is already in the mapping.
        self.encoder.add_or_override_mapping(MyStr, my_str_func)
        
        mapping = self.encoder._mapping
        # Find indices
        my_str_idx = -1
        str_idx = -1
        for i, (t, _) in enumerate(mapping):
            if t == MyStr:
                my_str_idx = i
            if t == str:
                str_idx = i
        
        self.assertNotEqual(my_str_idx, -1)
        self.assertNotEqual(str_idx, -1)
        # MyStr should be before str
        self.assertLess(my_str_idx, str_idx)

    def test_get_encode_function(self):
        """Test retrieving the correct encoder function."""
        # Base type
        func_str = self.encoder.get_encode_function(str)
        self.assertEqual(func_str, self.encoder.cql_encode_str)
        
        # Subclass type
        class MyStr(str):
            pass
        
        def my_str_func(x):
            return "custom"
            
        self.encoder.add_or_override_mapping(MyStr, my_str_func)
        func_my_str = self.encoder.get_encode_function(MyStr)
        self.assertEqual(func_my_str, my_str_func)

    def test_get_encode_function_not_found(self):
        """Test that getting a function for an unregistered type raises KeyError."""
        with self.assertRaises(KeyError):
            self.encoder.get_encode_function(complex)

    def test_get_encode_function_priority(self):
        """Test that priority is respected for subclasses."""
        class MyInt(int):
            pass
        
        def my_int_func(val):
            return f"my_int_{val}"
        
        self.encoder.add_or_override_mapping(MyInt, my_int_func)
        
        func_my_int = self.encoder.get_encode_function(MyInt)
        self.assertEqual(func_my_int, my_int_func)

        class MySubInt(MyInt):
            pass
        
        func_my_sub_int = self.encoder.get_encode_function(MySubInt)
        self.assertEqual(func_my_sub_int, my_int_func)
