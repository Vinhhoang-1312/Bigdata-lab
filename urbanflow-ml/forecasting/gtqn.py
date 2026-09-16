"""GTQN-Lite: a compact graph-temporal quantile forecasting network."""

from __future__ import annotations

from dataclasses import dataclass

import torch
from torch import nn
from torch.nn import functional as F


@dataclass(frozen=True)
class GTQNConfig:
    num_zones: int
    history_channels: int = 2
    calendar_channels: int = 7
    hidden_size: int = 32
    zone_embedding_size: int = 16
    temporal_dilations: tuple[int, ...] = (1, 2, 4, 8, 16, 32)
    dropout: float = 0.2
    use_fixed_graph: bool = True
    use_adaptive_graph: bool = True


class GatedTemporalBlock(nn.Module):
    def __init__(self, hidden_size: int, dilation: int, dropout: float) -> None:
        super().__init__()
        self.filter_conv = nn.Conv1d(hidden_size, hidden_size, 3, padding=dilation, dilation=dilation)
        self.gate_conv = nn.Conv1d(hidden_size, hidden_size, 3, padding=dilation, dilation=dilation)
        self.output = nn.Conv1d(hidden_size, hidden_size, 1)
        self.norm = nn.GroupNorm(1, hidden_size)
        self.dropout = nn.Dropout(dropout)

    def forward(self, values: torch.Tensor) -> torch.Tensor:
        gated = torch.tanh(self.filter_conv(values)) * torch.sigmoid(self.gate_conv(values))
        update = self.dropout(self.output(gated))
        return self.norm(values + update)


class GTQNLite(nn.Module):
    def __init__(self, config: GTQNConfig) -> None:
        super().__init__()
        self.config = config
        input_channels = config.history_channels + config.calendar_channels
        self.input_projection = nn.Conv1d(input_channels, config.hidden_size, 1)
        self.temporal_blocks = nn.ModuleList(
            GatedTemporalBlock(config.hidden_size, dilation, config.dropout)
            for dilation in config.temporal_dilations
        )
        self.temporal_attention = nn.Conv1d(config.hidden_size, 1, 1)

        self.source_embedding = nn.Parameter(torch.empty(config.num_zones, config.zone_embedding_size))
        self.target_embedding = nn.Parameter(torch.empty(config.num_zones, config.zone_embedding_size))
        nn.init.xavier_uniform_(self.source_embedding)
        nn.init.xavier_uniform_(self.target_embedding)

        self.self_projection = nn.Linear(config.hidden_size, config.hidden_size)
        self.fixed_projection = nn.Linear(config.hidden_size, config.hidden_size, bias=False)
        self.adaptive_projection = nn.Linear(config.hidden_size, config.hidden_size, bias=False)
        self.graph_gate = nn.Linear(config.hidden_size * 2, config.hidden_size)
        self.graph_norm = nn.LayerNorm(config.hidden_size)
        self.output_head = nn.Sequential(
            nn.Linear(config.hidden_size, config.hidden_size),
            nn.GELU(),
            nn.Dropout(config.dropout),
            nn.Linear(config.hidden_size, 3),
        )

    def adaptive_adjacency(self) -> torch.Tensor:
        logits = F.relu(self.source_embedding @ self.target_embedding.transpose(0, 1))
        return torch.softmax(logits, dim=-1)

    def _temporal_encode(self, history: torch.Tensor, calendar: torch.Tensor) -> torch.Tensor:
        batch, zones, steps, _ = history.shape
        if zones != self.config.num_zones:
            raise ValueError(f"expected {self.config.num_zones} zones, received {zones}")
        if calendar.shape[:2] != (batch, steps):
            raise ValueError("calendar must have shape [batch, history, features]")
        calendar_by_zone = calendar.unsqueeze(1).expand(-1, zones, -1, -1)
        features = torch.cat([history, calendar_by_zone], dim=-1)
        encoded = features.reshape(batch * zones, steps, -1).transpose(1, 2)
        encoded = self.input_projection(encoded)
        for block in self.temporal_blocks:
            encoded = block(encoded)
        attention = torch.softmax(self.temporal_attention(encoded), dim=-1)
        pooled = (encoded * attention).sum(dim=-1)
        return pooled.reshape(batch, zones, self.config.hidden_size)

    def forward(
        self,
        history: torch.Tensor,
        calendar: torch.Tensor,
        fixed_adjacency: torch.Tensor,
    ) -> torch.Tensor:
        encoded = self._temporal_encode(history, calendar)
        self_state = self.self_projection(encoded)
        graph_parts: list[torch.Tensor] = []
        if self.config.use_fixed_graph:
            fixed_context = torch.einsum("ij,bjh->bih", fixed_adjacency, encoded)
            graph_parts.append(self.fixed_projection(fixed_context))
        if self.config.use_adaptive_graph:
            adaptive_context = torch.einsum("ij,bjh->bih", self.adaptive_adjacency(), encoded)
            graph_parts.append(self.adaptive_projection(adaptive_context))
        graph_state = torch.stack(graph_parts).mean(dim=0) if graph_parts else self_state
        gate = torch.sigmoid(self.graph_gate(torch.cat([self_state, graph_state], dim=-1)))
        fused = self.graph_norm(self_state + gate * (graph_state - self_state))

        raw = self.output_head(fused)
        median = raw[..., 0]
        lower = median - F.softplus(raw[..., 1])
        upper = median + F.softplus(raw[..., 2])
        return torch.stack([lower, median, upper], dim=-1)


def pinball_loss(
    predictions: torch.Tensor,
    target: torch.Tensor,
    *,
    quantiles: tuple[float, ...] = (0.1, 0.5, 0.9),
    weights: tuple[float, ...] = (1.0, 2.0, 1.0),
) -> torch.Tensor:
    if predictions.shape[:-1] != target.shape or predictions.shape[-1] != len(quantiles):
        raise ValueError("predictions must have shape target.shape + [number of quantiles]")
    quantile_tensor = predictions.new_tensor(quantiles)
    weight_tensor = predictions.new_tensor(weights)
    error = target.unsqueeze(-1) - predictions
    loss = torch.maximum(quantile_tensor * error, (quantile_tensor - 1.0) * error)
    return (loss * weight_tensor).sum(dim=-1).mean() / weight_tensor.sum()
