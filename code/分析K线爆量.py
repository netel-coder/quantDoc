# -*- coding: utf-8 -*-
"""
量化筛选-爆量上涨筛选工具

功能说明：
    1. 筛选符合条件的股票
    2. 获取每只股票近100天前复权日K线数据
    3. 分析并筛选出爆量上涨的日期（成交量>=n倍均值且当日上涨）
    4. 按爆量倍数排序输出结果

作者：Juice
日期：2023-02-27
"""

import jvQuant
import time

# ==================== 配置参数 ====================
TOKEN = "平台token"  # 请替换为你的token

# 筛选条件
QUERY_CONDITION = "近100天涨幅小于50%，基金持股比例大于3%,沪深主板,非ST,流通市值200-2000亿,年利润大于0.5亿"

# 爆量阈值：当日成交量需要达到平均成交量的多少倍
VOLUME_RATIO_THRESHOLD = 3.0

# K线获取参数
KLINE_LIMIT = 100  # 获取近100天K线
KLINE_TYPE = "day"  # 日线
KLINE_FQ = "前复权"  # 前复权


# ==================== 工具函数 ====================

def safe_float(val, default=0.0):
    """
    安全转换为浮点数
    
    Args:
        val: 待转换的值
        default: 转换失败时的默认值
    
    Returns:
        float: 转换后的浮点数
    """
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


# ==================== 主程序 ====================

def main():
    """主函数"""
    
    # 初始化数据库客户端
    db = jvQuant.sql_client.Construct(TOKEN)
    
    # ==================== 第一步：筛选符合条件的股票 ====================
    print("=" * 60)
    print("第一步：筛选符合条件的股票，筛选条件:",QUERY_CONDITION)
    print("=" * 60)
    
    query_result = db.query(
        QUERY_CONDITION,
        page=1,
        sort_type=1,  # 降序
        sort_key="TOTAL_MV"  # 按市值排序
    )
    
    if query_result['code'] != 0:
        print(f"查询失败: {query_result['message']}")
        return
    
    data = query_result['data']
    stock_list = data['list']
    
    print(f"筛选条件: {data['query']}")
    print(f"符合条件的股票数量: {data['count']}")
    print()
    
    # ==================== 第二步：获取K线并分析爆量上涨 ====================
    print("=" * 60)
    print("第二步：获取K线并分析爆量上涨")
    print("=" * 60)
    
    results = []  # 存储爆量上涨记录
    
    for i, stock in enumerate(stock_list):
        code = stock[0]  # 股票代码
        name = stock[1]  # 股票名称
        
        print(f"[{i+1}/{len(stock_list)}] {code} {name}", end=" ")
        
        try:
            # 获取K线数据
            kline_result = db.kline(
                code=code,
                cate='stock',
                fq=KLINE_FQ,
                type=KLINE_TYPE,
                limit=KLINE_LIMIT
            )
            
            if kline_result['code'] != 0:
                print("失败")
                continue
            
            klines = kline_result['data']['list']
            
            # 数据不足则跳过
            if len(klines) < 20:
                print("数据不足")
                continue
            
            # 计算平均成交量
            volumes = [safe_float(k[5]) for k in klines]
            avg_volume = sum(volumes) / len(volumes)
            
            # 遍历每根K线，寻找爆量上涨
            count = 0
            for k in klines:
                # 解析K线数据
                date = k[0]           # 日期
                open_price = safe_float(k[1])   # 开盘价
                close = safe_float(k[2])        # 收盘价
                volume = safe_float(k[5])       # 成交量
                pct_chg = safe_float(k[8])      # 涨跌幅
                turnover = safe_float(k[10])    # 换手率
                
                # 判断是否爆量上涨
                # 条件：收盘价 > 开盘价（上涨） 且 成交量 >= 3倍均值
                if close > open_price and volume >= VOLUME_RATIO_THRESHOLD * avg_volume:
                    volume_ratio = volume / avg_volume
                    results.append({
                        'code': code,
                        'name': name,
                        'date': date,
                        'close': close,
                        'volume_ratio': volume_ratio,
                        'turnover': turnover,
                        'pct_chg': pct_chg
                    })
                    count += 1
            
            print(f"完成(爆量{count}次)")
            
        except Exception as e:
            print(f"错误: {e}")
        
        # 避免请求过于频繁
        time.sleep(0.1)
    
    # ==================== 第三步：输出结果 ====================
    print()
    print("=" * 60)
    print("第三步：输出爆量上涨结果")
    print("=" * 60)
    
    if not results:
        print("没有找到符合条件的爆量上涨记录")
        return
    
    # 按爆量倍数降序排序
    results.sort(key=lambda x: x['volume_ratio'], reverse=True)
    
    # 构建输出内容
    output_lines = []
    output_lines.append(f"共发现 {len(results)} 条爆量上涨记录\n")
    output_lines.append(f"{'序号':<4} {'代码':<8} {'名称':<10} {'日期':<12} {'收盘价':<8} {'爆量倍数':<8} {'换手率%':<8} {'涨幅%':<8}")
    output_lines.append("-" * 90)
    
    for i, r in enumerate(results, 1):
        line = f"{i:<4} {r['code']:<8} {r['name']:<10} {r['date']:<12} {r['close']:<8.2f} {r['volume_ratio']:<8.2f} {r['turnover']:<8.2f} {r['pct_chg']:<8.2f}"
        output_lines.append(line)
    
    output_text = "\n".join(output_lines)
    print(output_text)
    
    # 保存结果到文件
    with open("result.txt", "w", encoding="utf-8") as f:
        f.write(output_text)
    
    print(f"\n结果已保存到 result.txt")


if __name__ == "__main__":
    main()

