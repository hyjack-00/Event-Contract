import torch
import torch.nn as nn
import torch.nn.functional as F
import math

class PositionalEncoding(nn.Module):
    """
    为 Transformer 添加位置编码
    """
    def __init__(self, d_model, dropout=0.1, max_len=5000):
        super(PositionalEncoding, self).__init__()
        self.dropout = nn.Dropout(p=dropout)
        
        pe = torch.zeros(max_len, d_model)  # (max_len, d_model)
        position = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)  # (max_len, 1)
        div_term = torch.exp(torch.arange(0, d_model, 2).float() * (-math.log(10000.0) / d_model))
        pe[:, 0::2] = torch.sin(position * div_term)
        pe[:, 1::2] = torch.cos(position * div_term)
        pe = pe.unsqueeze(1)  # (max_len, 1, d_model)
        self.register_buffer('pe', pe)

    def forward(self, x):
        # x: (seq_len, batch, d_model)
        x = x + self.pe[:x.size(0)]
        return self.dropout(x)

class CNNTransformerBranch(nn.Module):
    """
    单个分支模型：
      - 使用 CNN 提取局部特征（针对 K 线图数据，每个 candle 含有多个特征）
      - 线性投影以匹配 Transformer 的输入维度
      - Transformer 捕捉全局时序依赖
      - 自适应池化将变长序列映射为固定长度表示
    """
    def __init__(self, in_features, cnn_channels, cnn_kernel_size,
                 transformer_d_model, nhead, num_transformer_layers,
                 transformer_dropout=0.1):
        super(CNNTransformerBranch, self).__init__()
        # 构建 CNN 模块，输入形状为 (batch, in_features, seq_len)
        layers = []
        current_channels = in_features
        for out_channels in cnn_channels:
            layers.append(nn.Conv1d(in_channels=current_channels,
                                    out_channels=out_channels,
                                    kernel_size=cnn_kernel_size,
                                    padding=cnn_kernel_size // 2))
            layers.append(nn.ReLU())
            layers.append(nn.BatchNorm1d(out_channels))
            current_channels = out_channels
        self.cnn = nn.Sequential(*layers)
        
        # 将 CNN 输出映射到 Transformer 的输入维度
        self.proj = nn.Linear(current_channels, transformer_d_model)
        
        # Transformer 部分
        self.pos_encoder = PositionalEncoding(transformer_d_model, dropout=transformer_dropout)
        # 注意：TransformerEncoderLayer 默认输入形状为 (seq_len, batch, d_model)
        encoder_layer = nn.TransformerEncoderLayer(d_model=transformer_d_model,
                                                   nhead=nhead,
                                                   dropout=transformer_dropout,
                                                   batch_first=False)
        self.transformer_encoder = nn.TransformerEncoder(encoder_layer, num_layers=num_transformer_layers)
        
        # 自适应平均池化，将时序维度压缩为 1 个输出向量
        self.pool = nn.AdaptiveAvgPool1d(1)

    def forward(self, x):
        """
        x: (batch, seq_len, in_features)
        """
        batch_size, seq_len, _ = x.size()
        # CNN 处理前调整数据维度至 (batch, in_features, seq_len)
        x = x.permute(0, 2, 1)
        x = self.cnn(x)  # (batch, current_channels, seq_len)
        # 调回 (batch, seq_len, channels)
        x = x.permute(0, 2, 1)
        # 线性投影：映射到 transformer_d_model 维度
        x = self.proj(x)  # (batch, seq_len, transformer_d_model)
        # Transformer 要求输入形状 (seq_len, batch, d_model)
        x = x.transpose(0, 1)  # (seq_len, batch, transformer_d_model)
        x = self.pos_encoder(x)
        x = self.transformer_encoder(x)  # (seq_len, batch, transformer_d_model)
        # 转回 (batch, transformer_d_model, seq_len) 以便池化
        x = x.transpose(0, 1).transpose(1, 2)
        x = self.pool(x)  # (batch, transformer_d_model, 1)
        x = x.squeeze(-1)  # (batch, transformer_d_model)
        return x

class MultiBranchModel(nn.Module):
    """
    多分支融合模型：
      - 针对不同时间间隔（例如 "1m", "5m", "15m", "30m", "1h", "2h", "4h", "8h"）建立独立分支
      - 每个分支输入该时间间隔的 K 线数据，经过 CNN+Transformer 处理后输出固定向量
      - 融合各分支输出（Concatenation），再经全连接层输出二分类概率（涨跌）
    """
    def __init__(self, interval_names, in_features, branch_params, fusion_hidden_size=128):
        """
        :param interval_names: list of intervals，例如 ['1m', '5m', '15m', '30m', '1h', '2h', '4h', '8h']
        :param in_features: 每个 candle 的特征数
        :param branch_params: dict，包含 CNN、Transformer 的超参数，例如：\n            cnn_channels, cnn_kernel_size, transformer_d_model, nhead, num_transformer_layers, transformer_dropout\n        :param fusion_hidden_size: 融合后全连接层隐藏单元数
        """
        super(MultiBranchModel, self).__init__()
        self.branches = nn.ModuleDict()
        for interval in interval_names:
            self.branches[interval] = CNNTransformerBranch(
                in_features=in_features,
                cnn_channels=branch_params.get('cnn_channels', [32, 64]),
                cnn_kernel_size=branch_params.get('cnn_kernel_size', 3),
                transformer_d_model=branch_params.get('transformer_d_model', 64),
                nhead=branch_params.get('nhead', 4),
                num_transformer_layers=branch_params.get('num_transformer_layers', 2),
                transformer_dropout=branch_params.get('transformer_dropout', 0.1)
            )
        # 融合各分支输出：每个分支输出维度为 transformer_d_model
        fusion_input_dim = len(interval_names) * branch_params.get('transformer_d_model', 64)
        self.fusion = nn.Sequential(
            nn.Linear(fusion_input_dim, fusion_hidden_size),
            nn.ReLU(),
            nn.Linear(fusion_hidden_size, 1)  # 输出一个标量 Logit
        )
        self.sigmoid = nn.Sigmoid()

    def forward(self, inputs):
        """
        :param inputs: dict，键为 interval 名称，值为对应分支输入 tensor，形状为 (batch, seq_len, in_features)
        """
        branch_outputs = []
        for interval, branch in self.branches.items():
            # 假设 inputs 包含所有 interval 的数据
            x = inputs[interval]  # (batch, seq_len, in_features)
            branch_out = branch(x)  # (batch, transformer_d_model)
            branch_outputs.append(branch_out)
        # 按特征维度串联各分支输出
        fusion_input = torch.cat(branch_outputs, dim=1)  # (batch, total_features)
        out = self.fusion(fusion_input)            # (batch, 1)
        out = self.sigmoid(out)  # 二分类概率输出
        return out

if __name__ == '__main__':
    # 示例：构建模型并用虚拟数据测试前向传播
    interval_names = ["1m", "5m", "15m", "30m", "1h", "2h", "4h", "8h"]
    in_features = 5  # 例如 open, high, low, close, volume
    branch_params = {
        "cnn_channels": [32, 64],
        "cnn_kernel_size": 3,
        "transformer_d_model": 64,
        "nhead": 4,
        "num_transformer_layers": 2,
        "transformer_dropout": 0.1
    }
    model = MultiBranchModel(interval_names, in_features, branch_params)
    print(model)

    # 构造虚拟输入：不同间隔的数据长度不同（例如 1m 数据最多，8h 数据最少）
    dummy_inputs = {}
    batch_size = 2
    seq_lengths = {"1m": 1000, "5m": 200, "15m": 70, "30m": 35, "1h": 18, "2h": 9, "4h": 5, "8h": 3}
    for interval in interval_names:
        dummy_inputs[interval] = torch.randn(batch_size, seq_lengths[interval], in_features)
    
    output = model(dummy_inputs)
    print("Output shape:", output.shape)
