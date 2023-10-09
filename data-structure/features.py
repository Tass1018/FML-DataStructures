from logger import logger
import pandas as pd


class Volatility:
    @staticmethod
    def getVolatility(close, span0=100):
        # daily vol, reindexed to close
        try:
            df0 = close.index.searchsorted(close.index - pd.Timedelta(days=1))
            df0 = df0[df0 > 0]
            df0 = pd.Series(close.index[df0 - 1], index=close.index[close.shape[0] - df0.shape[0]:])
            df0 = close.loc[df0.index] / close.loc[df0.values].values - 1  # daily returns
            df0 = df0.ewm(span=span0).std()
            return df0
        except Exception as e:
            logger.value_error(e)
