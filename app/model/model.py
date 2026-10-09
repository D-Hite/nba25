import pandas as pd
import torch
from torch_geometric.data import HeteroData
import networkx as nx
import matplotlib.pyplot as plt
import os

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
DATA_DIR = os.path.join(BASE_PATH, 'app', 'model', 'data')

# Load CSVs
player_nodes = pd.read_csv(os.path.join(DATA_DIR, 'player_nodes.csv'))
team_nodes = pd.read_csv(os.path.join(DATA_DIR, 'team_nodes.csv'))
outcome_nodes = pd.read_csv(os.path.join(DATA_DIR, 'outcome_nodes.csv'))
player_team_edges = pd.read_csv(os.path.join(DATA_DIR, 'player_team_edges.csv'))
team_outcome_edges = pd.read_csv(os.path.join(DATA_DIR, 'team_outcome_edges.csv'))

# Convert numeric columns to float
player_features = player_nodes.drop(columns=['node_id','season_id','player_id','team_id','next_game_id','played_flag'])
player_features = player_features.astype(float)

team_features = team_nodes.drop(columns=['node_id','team_id','season_id','next_game_id','team_city'])
team_features = team_features.astype(float)

# Initialize HeteroData
data = HeteroData()
data['player'].x = torch.tensor(player_features.values, dtype=torch.float)
data['team'].x = torch.tensor(team_features.values, dtype=torch.float)
data['outcome'].x = torch.ones((len(outcome_nodes), 1))  # simple placeholder

# Map node ids to integer indices
player_id_map = {nid: i for i, nid in enumerate(player_nodes['node_id'])}
team_id_map = {nid: i for i, nid in enumerate(team_nodes['node_id'])}
outcome_id_map = {nid: i for i, nid in enumerate(outcome_nodes['node_id'])}

# Player -> Team edges
src_idx = [player_id_map[nid] for nid in player_team_edges['source_node_id']]
tgt_idx = [team_id_map[nid] for nid in player_team_edges['target_node_id']]
data['player', 'plays_for', 'team'].edge_index = torch.tensor([src_idx, tgt_idx], dtype=torch.long)

# Team -> Outcome edges
src_idx = [team_id_map[nid] for nid in team_outcome_edges['source_node_id']]
tgt_idx = [outcome_id_map[nid] for nid in team_outcome_edges['target_node_id']]
data['team', 'team_result', 'outcome'].edge_index = torch.tensor([src_idx, tgt_idx], dtype=torch.long)

print(data)


# import torch
# import torch.nn as nn
# import torch.nn.functional as F
# from torch_geometric.nn import HeteroConv, GCNConv, SAGEConv, Linear
# from torch_geometric.loader import NeighborLoader

# # 1. Define the model
# class OutcomePredictor(nn.Module):
#     def __init__(self, hidden_channels, out_channels):
#         super().__init__()
#         self.conv1 = HeteroConv({
#             ('player', 'plays_for', 'team'): SAGEConv((-1, -1), hidden_channels),
#             ('team', 'team_result', 'outcome'): SAGEConv((-1, -1), hidden_channels)
#         }, aggr='sum')

#         # Only include edges where destination nodes actually exist
#         self.conv2 = HeteroConv({
#             ('team', 'team_result', 'outcome'): SAGEConv(hidden_channels, hidden_channels)
#         }, aggr='sum')

#         self.lin = Linear(hidden_channels, out_channels)

#     def forward(self, x_dict, edge_index_dict):
#         x_dict = self.conv1(x_dict, edge_index_dict)
#         x_dict = {k: F.relu(v) for k, v in x_dict.items()}

#         # Apply conv2 only where destination nodes exist
#         x_dict = self.conv2(x_dict, edge_index_dict)
#         x_dict = {k: F.relu(v) for k, v in x_dict.items()}

#         return self.lin(x_dict['outcome'])



# # 2. Prepare labels
# # Assuming outcome_nodes['result'] is 0/1 for loss/win

# # Now create tensor
# y = torch.tensor(outcome_nodes['result'].values, dtype=torch.float).unsqueeze(1)


# # 3. Train/test split
# num_nodes = len(outcome_nodes)
# train_mask = torch.zeros(num_nodes, dtype=torch.bool)
# train_mask[:int(0.8*num_nodes)] = True
# test_mask = ~train_mask

# # 4. Initialize model
# model = OutcomePredictor(hidden_channels=32, out_channels=1)
# optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
# criterion = nn.BCEWithLogitsLoss()  # binary classification

# # 5. Training loop
# data = data  # your heterograph
# for epoch in range(30):
#     model.train()
#     optimizer.zero_grad()
#     out = model(data.x_dict, data.edge_index_dict)
#     loss = criterion(out[train_mask], y[train_mask])
#     loss.backward()
#     optimizer.step()
#     if epoch % 5 == 0:
#         print(f'Epoch {epoch}, Loss: {loss.item():.4f}')

# # 6. Evaluation
# model.eval()
# with torch.no_grad():
#     pred = torch.sigmoid(out) > 0.5
#     acc = (pred[test_mask] == y[test_mask]).sum().item() / test_mask.sum().item()
#     print(f'Test Accuracy: {acc:.4f}')




import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import HeteroConv, SAGEConv, Linear
from sklearn.preprocessing import StandardScaler

# ----------------------------
# 1. Feature Normalization
# ----------------------------
def normalize_features(data):
    for ntype in data.node_types:
        x = data[ntype].x.cpu().numpy()
        scaler = StandardScaler()
        x = scaler.fit_transform(x)
        data[ntype].x = torch.tensor(x, dtype=torch.float)
    return data

# ----------------------------
# 2. Model Definition
# ----------------------------
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import HeteroConv, SAGEConv, Linear

class OutcomePredictor(nn.Module):
    def __init__(self, player_in_dim, team_in_dim, outcome_in_dim,
                 hidden_channels=32, out_channels=1, dropout=0.5):
        super().__init__()

        self.hidden_channels = hidden_channels

        # Initial linear projections to hidden_channels
        self.lin_dict = nn.ModuleDict({
            'player': nn.Linear(player_in_dim, hidden_channels),
            'team': nn.Linear(team_in_dim, hidden_channels),
            'outcome': nn.Linear(outcome_in_dim, hidden_channels)
        })

        # BatchNorm per node type
        self.bn_dict = nn.ModuleDict({
            'player': nn.BatchNorm1d(hidden_channels),
            'team': nn.BatchNorm1d(hidden_channels),
            'outcome': nn.BatchNorm1d(hidden_channels)
        })

        self.dropout = nn.Dropout(dropout)

        # HeteroConv layers
        # We include reverse edges so all node types are updated
        self.conv1 = HeteroConv({
            ('player', 'plays_for', 'team'): SAGEConv(hidden_channels, hidden_channels),
            ('team', 'rev_plays_for', 'player'): SAGEConv(hidden_channels, hidden_channels),
            ('team', 'team_result', 'outcome'): SAGEConv(hidden_channels, hidden_channels),
            ('outcome', 'rev_team_result', 'team'): SAGEConv(hidden_channels, hidden_channels),
        }, aggr='sum')

        self.conv2 = HeteroConv({
            ('player', 'plays_for', 'team'): SAGEConv(hidden_channels, hidden_channels),
            ('team', 'rev_plays_for', 'player'): SAGEConv(hidden_channels, hidden_channels),
            ('team', 'team_result', 'outcome'): SAGEConv(hidden_channels, hidden_channels),
            ('outcome', 'rev_team_result', 'team'): SAGEConv(hidden_channels, hidden_channels),
        }, aggr='sum')

        # Output layer for outcome prediction
        self.lin_out = Linear(hidden_channels, out_channels)

    def forward(self, x_dict, edge_index_dict):
        # Initial projection + BatchNorm + ReLU
        x_dict = {k: F.relu(self.bn_dict[k](self.lin_dict[k](v)))
                  for k, v in x_dict.items()}

        # Layer 1
        x_dict = self.conv1(x_dict, edge_index_dict)
        x_dict = {k: F.relu(self.bn_dict[k](v)) for k, v in x_dict.items()}
        x_dict = {k: self.dropout(v) for k, v in x_dict.items()}

        # Layer 2
        x_dict = self.conv2(x_dict, edge_index_dict)
        x_dict = {k: F.relu(self.bn_dict[k](v)) for k, v in x_dict.items()}
        x_dict = {k: self.dropout(v) for k, v in x_dict.items()}

        # Predict only outcome nodes
        return self.lin_out(x_dict['outcome'])




import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.data import HeteroData
from torch_geometric.loader import NeighborLoader
# from model import OutcomePredictor  # or wherever you saved the class

# --- 1. Prepare HeteroData ---
# Assume `data` is your HeteroData object with nodes and edges already loaded

# Create reverse edges
edge_index_dict = {
    ('player', 'plays_for', 'team'): data['player', 'plays_for', 'team'].edge_index,
    ('team', 'rev_plays_for', 'player'): data['player', 'plays_for', 'team'].edge_index.flip(0),
    ('team', 'team_result', 'outcome'): data['team', 'team_result', 'outcome'].edge_index,
    ('outcome', 'rev_team_result', 'team'): data['team', 'team_result', 'outcome'].edge_index.flip(0),
}

x_dict = {
    'player': data['player'].x,
    'team': data['team'].x,
    'outcome': data['outcome'].x
}

# --- 2. Prepare labels ---
y = torch.tensor(outcome_nodes['result'].values, dtype=torch.float).unsqueeze(1)

num_outcomes = y.size(0)
train_mask = torch.zeros(num_outcomes, dtype=torch.bool)
train_mask[:int(0.8 * num_outcomes)] = True
test_mask = ~train_mask

# --- 3. Initialize model ---
player_in_dim = x_dict['player'].size(1)
team_in_dim = x_dict['team'].size(1)
outcome_in_dim = x_dict['outcome'].size(1)

model = OutcomePredictor(player_in_dim, team_in_dim, outcome_in_dim,
                         hidden_channels=32, out_channels=1, dropout=0.5)

optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
criterion = nn.BCEWithLogitsLoss()

# --- 4. Training loop ---
num_epochs = 30

for epoch in range(num_epochs):
    model.train()
    optimizer.zero_grad()
    
    out = model(x_dict, edge_index_dict)
    loss = criterion(out[train_mask], y[train_mask])
    
    if torch.isnan(loss):
        print(f"Epoch {epoch}: Loss is NaN, skipping gradient step")
        continue
    
    loss.backward()
    optimizer.step()
    
    if epoch % 5 == 0:
        print(f"Epoch {epoch}, Loss: {loss.item():.4f}")

# --- 5. Evaluation ---
model.eval()
with torch.no_grad():
    out = model(x_dict, edge_index_dict)
    pred = torch.sigmoid(out) > 0.5
    acc = (pred[test_mask] == y[test_mask]).sum().item() / test_mask.sum().item()
    print(f"Test Accuracy: {acc:.4f}")
