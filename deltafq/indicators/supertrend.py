"""
SuperTrend（超级趋势）指标：ATR 通道 + 方向翻转。

实现说明：
1. 继承 ``TechnicalIndicators``，直接复用框架已有的 ATR/SMA/EMA 等基础指标，
   不修改任何原有代码；
2. ATR 默认使用 Wilder 平滑（RMA），与 TradingView、pandas_ta 及
   ``go/SuperTrendTrend-Following.py`` 的口径一致；
3. 最终上轨/下轨具有路径依赖：在趋势方向内只允许收紧，因此逐 bar 递推；
4. 方向翻转使用「当前收盘价 vs 上一根最终轨道」，严格不使用未来数据。
"""

import numpy as np
import pandas as pd

from .technical import TechnicalIndicators


class SuperTrendIndicators(TechnicalIndicators):
    """在基础技术指标之上，增加的 SuperTrend 指标实现。"""

    def supertrend(
        self,
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        period: int = 10,
        multiplier: float = 3.0,
        method: str = "rma",
    ) -> pd.DataFrame:
        """
        计算 SuperTrend 指标。

        参数:
            high: 最高价序列
            low: 最低价序列
            close: 收盘价序列
            period: ATR 计算周期，默认 10
            multiplier: ATR 通道倍数，默认 3.0
            method: ATR 平滑方式，'rma' 为 Wilder 平滑（默认，与 TA-Lib 一致），
                    'sma' 为简单移动平均

        返回:
            DataFrame（索引与 close 对齐），包含列:
                supertrend: 趋势线（多头取最终下轨，空头取最终上轨）
                direction: 方向，1=多头，-1=空头，0=指标预热期
                upper: 最终上轨
                lower: 最终下轨
                atr: 通道所用的 ATR
        """
        self.logger.info(f"Calculating SuperTrend(period={period}, multiplier={multiplier}, method={method})")

        # 统一转为浮点，避免整数列参与运算时产生精度/类型问题
        high = high.astype(float)
        low = low.astype(float)
        close = close.astype(float)

        # 复用父类的 ATR 实现（Wilder RMA 与 TA-Lib 口径一致）
        atr = self.atr(high, low, close, period=period, method=method)

        # 基础轨道：中价 (H+L)/2 上下各偏移 multiplier 倍 ATR
        hl2 = (high + low) / 2.0
        basic_upper = (hl2 + multiplier * atr).to_numpy(dtype=float)
        basic_lower = (hl2 - multiplier * atr).to_numpy(dtype=float)
        close_arr = close.to_numpy(dtype=float)

        n = len(close)
        final_upper = np.full(n, np.nan, dtype=float)   # 最终上轨
        final_lower = np.full(n, np.nan, dtype=float)   # 最终下轨
        direction = np.zeros(n, dtype=int)              # 趋势方向：0 预热 / 1 多 / -1 空
        line = np.full(n, np.nan, dtype=float)          # 画图用的 SuperTrend 趋势线

        # 找到 ATR 首个有效位置，之前的 K 线属于预热期，不产生任何方向
        valid_positions = np.flatnonzero(atr.notna().to_numpy())
        if valid_positions.size:
            start = int(valid_positions[0])

            # 首根有效 K 线：轨道直接取基础轨道，方向由收盘价相对中轨的位置决定
            final_upper[start] = basic_upper[start]
            final_lower[start] = basic_lower[start]
            direction[start] = 1 if close_arr[start] >= (basic_upper[start] + basic_lower[start]) / 2.0 else -1
            line[start] = final_lower[start] if direction[start] == 1 else final_upper[start]

            for i in range(start + 1, n):
                # 数据缺口（如停牌导致 ATR 为 NaN）：沿用上一根状态，避免整列被污染
                if np.isnan(basic_upper[i]):
                    final_upper[i] = final_upper[i - 1]
                    final_lower[i] = final_lower[i - 1]
                    direction[i] = direction[i - 1]
                    line[i] = final_lower[i] if direction[i] == 1 else final_upper[i]
                    continue

                # 上轨只允许逐步下移（除非价格已向上突破上轨）
                if basic_upper[i] < final_upper[i - 1] or close_arr[i - 1] > final_upper[i - 1]:
                    final_upper[i] = basic_upper[i]
                else:
                    final_upper[i] = final_upper[i - 1]

                # 下轨只允许逐步上移（除非价格已向下跌破下轨）
                if basic_lower[i] > final_lower[i - 1] or close_arr[i - 1] < final_lower[i - 1]:
                    final_lower[i] = basic_lower[i]
                else:
                    final_lower[i] = final_lower[i - 1]

                # 方向翻转：用当前收盘价对比「上一根」最终轨道，避免未来函数
                if close_arr[i] > final_upper[i - 1]:
                    direction[i] = 1
                elif close_arr[i] < final_lower[i - 1]:
                    direction[i] = -1
                else:
                    direction[i] = direction[i - 1]

                # 趋势线：多头显示下轨（支撑），空头显示上轨（压力）
                line[i] = final_lower[i] if direction[i] == 1 else final_upper[i]

        return pd.DataFrame(
            {
                "supertrend": line,
                "direction": direction,
                "upper": final_upper,
                "lower": final_lower,
                "atr": atr.to_numpy(dtype=float),
            },
            index=close.index,
        )
