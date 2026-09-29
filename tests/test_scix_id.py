import json
from unittest import TestCase

import SciXPipelineUtils.scix_id as scixid


class TestSciXIDImplementation(TestCase):
    def test_generate_scixid(self):
        test_id = scixid.encode(1000)
        self.assertEqual(test_id, "0000-0000-0Z8A")
        test_int = scixid.decode(test_id)
        self.assertEqual(test_int, 1000)

    def test_generate_scixid_no_checksum(self):
        test_id = scixid.encode(1000, checksum=False)
        self.assertEqual(test_id, "0000-0000-00Z8")
        test_int = scixid.decode(test_id, checksum=False)
        self.assertEqual(test_int, 1000)

    def test_generate_bib_data_hash(self):
        test_bib_data = {
            "id": 1,
            "author": ["Lias, Alberta", "Smith, J."],
            "title": "Test",
            "abstract": ["words"],
            "bibcode": "Test",
        }
        hash = scixid.generate_bib_data_hash(test_bib_data)
        self.assertEqual(hash, "2adce16b8aa67707f4fa2eb34646cdb3")

        test_bib_data = {
            "title": "Test",
            "abstract": ["words"],
        }
        hash2 = scixid.generate_bib_data_hash(test_bib_data)
        self.assertEqual(hash2, "2adce16b8aa67707f4fa2eb34646cdb3")

        self.assertEqual(hash, hash2)

    def test_get_rand_from_hash(self):
        test_bib_data = {
            "id": 1,
            "author": ["Lias, Alberta", "Smith, J."],
            "title": "Test",
            "abstract": ["words"],
        }
        hash = scixid.generate_bib_data_hash(test_bib_data)
        rand_num = scixid.get_rand_from_hash(hash)
        self.assertEqual(rand_num, 2955056064090141)

    def test_scix_id_from_hash(self):
        test_bib_data = {
            "id": 1,
            "author": ["Lias, Alberta", "Smith, J."],
            "title": "Test",
            "abstract": ["words"],
        }
        hash = scixid.generate_bib_data_hash(test_bib_data)
        scix_id = scixid.scix_id_from_hash(hash)
        self.assertEqual(scix_id, "2KZK-EDFV-M0X0")

    def test_generate_scix_id(self):
        test_bib_data = {
            "id": 1,
            "author": ["Lias, Alberta", "Smith, J."],
            "title": "Test",
            "abstract": ["words"],
        }
        scix_id = scixid.generate_scix_id(test_bib_data)
        scix_id_2 = scixid.generate_scix_id(json.dumps(test_bib_data))
        self.assertEqual(scix_id, "3DV1-K5S7-XR8W")
        self.assertEqual(scix_id, scix_id_2)

    def test_generate_scix_id_user_fields(self):
        test_bib_data = {
            "id": 1,
            "author": ["Lias, Alberta", "Smith, J."],
            "title": "Test",
            "abstract": ["words"],
        }
        user_fields = ["id"]
        scix_id = scixid.generate_scix_id(test_bib_data, user_fields=user_fields)
        scix_id_2 = scixid.generate_scix_id(test_bib_data)
        self.assertEqual(scix_id, "44GP-FCA0-SEWD")
        self.assertNotEqual(scix_id, scix_id_2)

    def test_generate_scix_id_special_characters_true(self):
        test_bib_data = {
            "id": 1,
            "author": ["Lias, Alberta", "Smith, J."],
            "title": "Test",
            "abstract": ["words < <lt\\>"],
        }
        scix_id = scixid.generate_scix_id(test_bib_data)
        scix_id_2 = scixid.generate_scix_id(json.dumps(test_bib_data))
        self.assertEqual(scix_id, "3DV1-K5S7-XR8W")
        self.assertEqual(scix_id, scix_id_2)

    def test_generate_scix_id_special_characters_true_comparison(self):
        test_bib_data = {
            "id": 1,
            "author": ["Lias, Alberta", "Smith, J."],
            "title": "Test",
            "abstract": ["words < <lt\\>"],
        }

        test_bib_data_2 = {
            "id": 1,
            "author": ["Lias, Alberta", "Smith, J."],
            "title": "Test",
            "abstract": ["words <"],
        }

        scix_id = scixid.generate_scix_id(test_bib_data)
        scix_id_2 = scixid.generate_scix_id(test_bib_data_2)
        self.assertEqual(scix_id, "3DV1-K5S7-XR8W")
        self.assertEqual(scix_id, scix_id_2)

    def test_generate_scix_id_special_characters_false(self):
        test_bib_data = {
            "id": 1,
            "author": ["Lias, Alberta", "Smith, J."],
            "title": "Test",
            "abstract": ["words < <lt\\>"],
        }
        scix_id = scixid.generate_scix_id(test_bib_data, strip_characters=False)
        scix_id_2 = scixid.generate_scix_id(test_bib_data)
        self.assertEqual(scix_id, "2VAQ-97RH-GCEP")
        self.assertNotEqual(scix_id, scix_id_2)

    def test_generate_scix_id_other(self):
        test_bib_data = {
            "id": 1,
            "author": ["Lias, Alberta", "Smith, J."],
            "title": "Test",
            "abstract": ["words"],
        }
        scix_id = scixid.generate_scix_id(json.dumps(test_bib_data), hash_data_type="other")
        self.assertNotEqual(scix_id, "3DV1-K5S7-XR8W")
        self.assertEqual(scix_id, "4KAA-NYFK-ZK4N")

    def test_two_different_titles(self):
        test_bib_data_one = {
            "id": 1,
            "author": ["McTestFace, Testy", "Templeton, Matthew"],
            "title": "Test One",
            "abstract": ["words"],
        }
        test_bib_data_two = {
            "id": 1,
            "author": ["McTestFace, Testy", "Templeton, Matthew"],
            "title": "Test Two",
            "abstract": ["words"],
        }
        # 22WS-AZXX-BGDQ
        scix_id_one = scixid.generate_scix_id(test_bib_data_one)
        # AKCJ-8JNF-253E
        scix_id_two = scixid.generate_scix_id(test_bib_data_two)
        self.assertNotEqual(scix_id_one, scix_id_two)
