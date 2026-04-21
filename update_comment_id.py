import pandas as pd

# 读取CSV文件
csv_path = r'e:\Mediainfo\MediaCrawler\data\douyin\csv\data_cleaned.csv'
df = pd.read_csv(csv_path)

# 获取原始行数
total_rows = len(df)
print(f"总数据行数: {total_rows}")

# 重新编号comment_id，从0001开始递增
# 根据数据量确定位数（最多4位数字，从0001到9999，如果超过则增加位数）
max_width = len(str(total_rows))
df['comment_id'] = [str(i+1).zfill(max_width) for i in range(total_rows)]

print(f"ID位数: {max_width}")
print(f"新ID示例: {df['comment_id'].iloc[0]} ~ {df['comment_id'].iloc[-1]}")

# 保存回CSV文件
df.to_csv(csv_path, index=False, encoding='utf-8')
print(f"✓ 已成功更新 {csv_path}")
print(f"前5行:")
print(df.head())
