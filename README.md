# Event-Contract
Prediction for COIN event contract. Let's play probability with gambling!

## Project structure



## Workflow 
### Offline training and testing



## Update
- 2025.2.23.v0.1
    - fetch_data.py
        - 实现 end_time_to_now 
        - 增加需求：保存的文件需要包含数据的 meta data，比如运行 fetch_data.py 的时间和全部参数，保存形式可以融合到 .npz 里也可以新建一个同名的 .json 文件，看你后续读取怎么样最方便+最符合项目架构设计
    - model.py 
        - 更改模型输入数据，所有 interval 的数据都应当有相同的 #klines 以保证每种跨度的数据能有相同的充分影响？但是对于我们的问题场景，短期特征似乎更重要，我需要两份都尝试，请将模型借口设计的有通用型

- 2025.3.16 v0.2 @decision_tree
    - 初步方案更新: BTC USDT 价格单变量，滑动窗口的决策树，短期预测
        - Biance API -> df files
        - XGBoost

- 2025.3.18 v0.2.1 @decision_tree
    - XGboost.py
