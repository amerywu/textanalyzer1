import json
import os
import time as dt
import unittest

import tools.elasticsearch_management.es_extraction as ex
import tools.utils.envutils as env


class TestSum(unittest.TestCase):

    def test_sum(self):
        print("cwd " + os.getcwd())
        self.assertEqual(sum([1, 2, 3]), 6, "Should be 6")


class TestExtraction(unittest.TestCase):

    def test_extract_confluence(self):
        print("cwd " + os.getcwd())

        env.init()
        print(env.printConf())

        pd = ex.initiate_extraction()
        self.assertTrue(len(pd.columns.values) > 0)


class TestJsonParsing(unittest.TestCase):

    def test_json(self):
        l = ["sssss", "ddddd", "vvvvvv"]
        corpus = []

        for x in range(0, 5):
            row = []
            t = str(dt.time())
            for w in l:
                row.append(t + ":" + w)
            corpus.append(row)

        json_string = json.dumps(corpus)

        print(json_string)
        self.assertTrue(len(json_string) > 0)


if __name__ == '__main__':
    unittest.main()
