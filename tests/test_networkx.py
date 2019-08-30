from unittest import TestCase

from pandas.testing import assert_frame_equal
from gordium import Gordium
from ._util import *

class TestNetworkXBackend(TestCase):

    def test_cc(self):
        edgeframe = get_cc_edgeframe()
        g = Gordium(edgeframe)
        analytics_pred = g.process()
        analytics_true = get_cc_analytics()
        assert_frame_equal(
                analytics_pred[analytics_true.columns],
                analytics_true,
                check_dtype=False)
        return
    
    def test_lone_pair(self):
        n = 2
        edgeframe = get_complete_edgeframe(n)
        g = Gordium(edgeframe)
        analytics_pred = g.process()
        analytics_true = get_complete_analytics(n)
        assert_frame_equal(
                analytics_pred[analytics_true.columns],
                analytics_true,
                check_dtype=False)
        return
    
    def test_less_than_or_equal_1000_degree(self):
        n  = 1000
        edgeframe = get_complete_edgeframe(n)
        g = Gordium(edgeframe)
        analytics_pred = g.process()
        analytics_true = get_complete_analytics(n)
        assert_frame_equal(
                analytics_pred[analytics_true.columns],
                analytics_true,
                check_dtype=False)
        return
    
    def test_greater_than_1000_degree(self):
        n  = 1001
        edgeframe = get_complete_edgeframe(n)
        g = Gordium(edgeframe)
        analytics_pred = g.process()
        analytics_true = get_complete_analytics(n)
        assert_frame_equal(
                analytics_pred[analytics_true.columns],
                analytics_true,
                check_dtype=False)
        return
