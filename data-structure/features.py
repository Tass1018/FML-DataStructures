from logger import logger
import pandas as pd

def get_daily_vol(close, lookback=100):
    """
    Advances in Financial Machine Learning, Snippet 3.1, page 44.

    Daily Volatility Estimates

    Computes the daily volatility at intraday estimation points.

    In practice we want to set profit taking and stop-loss limits that are a function of the risks involved
    in a bet. Otherwise, sometimes we will be aiming too high (tao ≫ sigma_t_i,0), and sometimes too low
    (tao ≪ sigma_t_i,0 ), considering the prevailing volatility. Snippet 3.1 computes the daily volatility
    at intraday estimation points, applying a span of lookback days to an exponentially weighted moving
    standard deviation.

    See the pandas documentation for details on the pandas.Series.ewm function.
    Note: This function is used to compute dynamic thresholds for profit taking and stop loss limits.

    :param close: (pd.Series) Closing prices
    :param lookback: (int) Lookback period to compute volatility
    :return: (pd.Series) Daily volatility value
    """
    if not isinstance(close.index, pd.DatetimeIndex):
        close.index = pd.to_datetime(close.index)

    daily_close = close.resample('D').last()

    # Calculate the daily returns
    daily_returns = daily_close.pct_change()

    # Calculate the exponentially weighted moving standard deviation
    vol = daily_returns.ewm(span=lookback).std()

    return vol


# def getVolatility(close, span0=100):
#     # daily vol, reindexed to close
#     try:
#         df0 = close.index.searchsorted(close.index - pd.Timedelta(days=1))
#         df0 = df0[df0 > 0]
#         df0 = pd.Series(close.index[df0 - 1], index=close.index[close.shape[0] - df0.shape[0]:])
#         df0 = close.loc[df0.index] / close.loc[df0.values].values - 1  # daily returns
#         df0 = df0.ewm(span=span0).std()
#         return df0
#     except Exception as e:
#         logger.value_error(e)
