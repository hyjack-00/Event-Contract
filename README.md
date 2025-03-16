# Event-Contract
Prediction for COIN event contract. Let's play probability with gambling!

## Project structure



## Workflow 
### Offline training and testing

1. run `fetch_data.py` to obtain the price data. The program downloads kline data of given time period from the coin trading website API, and saves the data as a *.npz file.

TODO: 

2. 


## Update
- 2025.2.23.v1
    - fetch_data.py
        - 实现 end_time_to_now 
        - 增加需求：保存的文件需要包含数据的 meta data，比如运行 fetch_data.py 的时间和全部参数，保存形式可以融合到 .npz 里也可以新建一个同名的 .json 文件，看你后续读取怎么样最方便+最符合项目架构设计
    - model.py 
        - 更改模型输入数据，所有 interval 的数据都应当有相同的 #klines 以保证每种跨度的数据能有相同的充分影响？但是对于我们的问题场景，短期特征似乎更重要，我需要两份都尝试，请将模型借口设计的有通用型




