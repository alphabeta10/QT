"""
SuperTrend（超级趋势）策略。

结构说明：
1. 指标层：``deltafq.indicators.supertrend.SuperTrendIndicators``（继承 TechnicalIndicators）；
2. 信号层：``SuperTrendSignalGenerator`` 继承 ``SignalGenerator``，只负责把方向序列
   ``direction`` 转成 {-1, 0, 1} 信号；
3. 策略层：``SuperTrendStrategy`` 继承 ``BaseStrategy``，组装指标与信号，
   可直接交给 ``BacktestEngine`` / ``LiveEngine`` 使用。

两种信号模式：
    state（默认）：多头期间持续输出 1、空头期间持续输出 -1，与 ``ema_signals`` 风格一致；
                   回测引擎在满仓后再次收到 1 时不会重复买入，因此该模式安全。
    flip：只在方向翻转的那根 K 线输出 1 / -1，换手更低、更接近图表上的翻转标记。
"""

from typing import Any

import numpy as np
import pandas as pd

from .base import BaseStrategy
from .signals import SignalGenerator
from ..indicators.supertrend import SuperTrendIndicators


class SuperTrendSignalGenerator(SignalGenerator):
    """把 SuperTrend 的方向序列转换为交易信号，复用父类的日志与信号统计能力。"""

    def supertrend_signals(self, close: pd.Series, trend: pd.Series, method: str = "state") -> pd.Series:
        """
        生成 SuperTrend 交易信号。

        参数:
            close: 收盘价序列，用于对齐索引
            trend: SuperTrend 的方向序列（1 多 / -1 空 / 0 预热）
            method: 'state' 表示持有方向状态；'flip' 表示只在翻转当根触发信号

        返回:
            pd.Series，取值 {-1, 0, 1}
        """
        if method not in ("state", "flip"):
            raise ValueError("method must be 'state' or 'flip'")

        # 对齐到收盘价索引，预热期的 NaN 视为 0（不交易）
        trend = trend.reindex(close.index).fillna(0).astype(int)

        if method == "state":
            # 状态模式：方向即信号，多头持仓、空头空仓
            signals = trend.astype(int)
        else:
            # 翻转模式：仅在前一根方向不同且当前方向非 0 的当根产生信号
            prev = trend.shift(1).fillna(0).astype(int)
            buy = (prev <= 0) & (trend == 1)      # 由空转多：买入
            sell = (prev >= 0) & (trend == -1)    # 由多转空：卖出
            signals = pd.Series(
                np.where(buy, 1, np.where(sell, -1, 0)),
                index=close.index,
                dtype=int,
            )

        # 复用父类的信号计数日志（buy/sell/flat）
        self._log_signal_counts(f"SuperTrend ({method})", signals)
        return signals


class SuperTrendStrategy(BaseStrategy):
    """基于 SuperTrend 的趋势跟踪策略（回测引擎为多头模式：1 买入、-1 清仓）。"""

    def __init__(
        self,
        name: str = "SuperTrend",
        period: int = 10,
        multiplier: float = 3.0,
        atr_method: str = "rma",
        signal_method: str = "state",
        **kwargs: Any,
    ) -> None:
        """
        参数:
            name: 策略名称
            period: ATR 周期
            multiplier: ATR 通道倍数
            atr_method: ATR 平滑方式，'rma'（Wilder，默认）或 'sma'
            signal_method: 信号模式，'state'（默认）或 'flip'
        """
        super().__init__(name=name, **kwargs)
        self.period = period
        self.multiplier = multiplier
        self.atr_method = atr_method
        self.signal_method = signal_method
        # 指标与信号生成器均通过继承扩展，互不修改原有实现
        self.indicators = SuperTrendIndicators()
        self.signals_generator = SuperTrendSignalGenerator()
        # 保存最近一次计算的完整指标结果，便于复现/画图
        self.supertrend_df: pd.DataFrame = pd.DataFrame()

    def generate_signals(self, data: pd.DataFrame) -> pd.Series:
        """由 OHLC 数据计算 SuperTrend 方向，并转换为交易信号。"""
        # 指标层负责 ATR 通道与方向翻转
        self.supertrend_df = self.indicators.supertrend(
            data["High"],
            data["Low"],
            data["Close"],
            period=self.period,
            multiplier=self.multiplier,
            method=self.atr_method,
        )
        # 信号层负责方向 -> 买卖信号
        return self.signals_generator.supertrend_signals(
            data["Close"],
            self.supertrend_df["direction"],
            method=self.signal_method,
        )
