LEVERAGE_BOUNDS_V2_START_TIME_MS = 1722018483000
from vali_config import TradePair

def positional_leverage_limit_v1(trade_pair: TradePair) -> int:
    if trade_pair.is_crypto:
        return 20
    elif trade_pair.is_forex or trade_pair.is_indices:
        return 500
    else:
        raise ValueError(f"Unknown trade pair type {trade_pair.trade_pair_id}")

def get_position_leverage_bounds(trade_pair: TradePair, t_ms: int) -> (float, float):
    is_leverage_v2 = t_ms >= LEVERAGE_BOUNDS_V2_START_TIME_MS
    max_position_leverage = trade_pair.max_leverage if is_leverage_v2 else positional_leverage_limit_v1(
        trade_pair)
    min_position_leverage = trade_pair.min_leverage if is_leverage_v2 else 0.001  # clamping from below not needed in v1
    return min_position_leverage, max_position_leverage