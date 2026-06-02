"""Business Screener Deluxe - Screen Implementations"""

from .base import BaseScreen, register_screen, get_screen_class, get_all_screen_classes

# Import all screens to register them
from .screen_01_net_nets import NetNetsScreen
from .screen_02_defensive_bargains import DefensiveBargainsScreen
from .screen_03_asset_plays import AssetPlaysScreen
from .screen_04_revenue_turnarounds import RevenueTurnaroundsScreen
from .screen_05_distressed_stable_earners import DistressedStableEarnersScreen
from .screen_06_garp import GARPScreen
from .screen_07_compressed_fundamentals import CompressedFundamentalsScreen
from .screen_08_special_situations import SpecialSituationsScreen
from .screen_09_holding_companies import HoldingCompanyDiscountsScreen
from .screen_10_sum_of_parts import SumOfPartsScreen
from .screen_11_cannibal_companies import CannibalCompaniesScreen
from .screen_12_wonderful_business import WonderfulBusinessScreen
from .screen_13_crisis_bargains import CrisisBargainsScreen
from .screen_14_cyclicals import CyclicalsScreen
from .screen_15_stalwarts import StalwartsScreen
from .screen_16_industrial_asset_recovery import IndustrialAssetRecoveryScreen

__all__ = [
    'BaseScreen',
    'register_screen',
    'get_screen_class',
    'get_all_screen_classes',
    # Screen implementations
    'NetNetsScreen',
    'DefensiveBargainsScreen',
    'AssetPlaysScreen',
    'RevenueTurnaroundsScreen',
    'DistressedStableEarnersScreen',
    'GARPScreen',
    'CompressedFundamentalsScreen',
    'SpecialSituationsScreen',
    'HoldingCompanyDiscountsScreen',
    'SumOfPartsScreen',
    'CannibalCompaniesScreen',
    'WonderfulBusinessScreen',
    'CrisisBargainsScreen',
    'CyclicalsScreen',
    'StalwartsScreen',
    'IndustrialAssetRecoveryScreen',
]
