import torch
from torch import nn
import torch.nn.functional as F
from torch.nn import BCEWithLogitsLoss

from transformers import AutoModel, AutoConfig

from pdb import set_trace

from src.finetuning.open_book.contrastive.models.loss import SupConLoss


def mean_pooling(model_output, attention_mask):
    token_embeddings = model_output[0] #First element of model_output contains all token embeddings
    input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(input_mask_expanded.sum(1), min=1e-9)

class BaseEncoder(nn.Module):

    def __init__(self, len_tokenizer, model='roberta-base'):
        super().__init__()

        self.transformer = AutoModel.from_pretrained(model)
        self.transformer.resize_token_embeddings(len_tokenizer)

    def forward(self, input_ids, attention_mask):
        
        output = self.transformer(input_ids, attention_mask)

        return output

class ContrastivePretrainModel(nn.Module):

    def __init__(self, len_tokenizer, model='roberta-base', pool=True, proj=128, temperature=0.07, with_proj=True):
        super().__init__()

        self.pool = pool
        self.proj = proj
        self.temperature = temperature
        self.criterion = SupConLoss(self.temperature)

        self.encoder = BaseEncoder(len_tokenizer, model)
        self.config = self.encoder.transformer.config

        self.with_proj = with_proj
        self.transform = nn.Linear(self.config.hidden_size, self.proj)

        
    def forward(self, input_ids, attention_mask, labels, input_ids_right, attention_mask_right):
        
        if self.pool:
            output_left = self.encoder(input_ids, attention_mask)
            output_left = mean_pooling(output_left, attention_mask)

            output_right = self.encoder(input_ids_right, attention_mask_right)
            output_right = mean_pooling(output_right, attention_mask_right)
        else:
            output_left = self.encoder(input_ids, attention_mask)['pooler_output']
            output_right = self.encoder(input_ids_right, attention_mask_right)['pooler_output']
        
        output = torch.cat((output_left.unsqueeze(1), output_right.unsqueeze(1)), 1)

        if self.with_proj:
            output = torch.tanh(self.transform(output))

        output = F.normalize(output, dim=-1)

        loss = self.criterion(output, labels)

        return ((loss,))


class ContrastiveModel(nn.Module):

    def __init__(self, len_tokenizer, model='roberta-base', pool=True, proj=128, temperature=0.07, with_proj=True):
        super().__init__()

        self.pool = pool
        self.proj = proj
        self.temperature = temperature
        self.criterion = SupConLoss(self.temperature)

        self.encoder = BaseEncoder(len_tokenizer, model)
        self.config = self.encoder.transformer.config

        self.with_proj = with_proj
        # Only apply projection if it is requested
        self.transform = nn.Linear(self.config.hidden_size, self.proj)

    def forward(self, input_ids, attention_mask):

        if self.pool:
            output = self.encoder(input_ids, attention_mask)
            output = mean_pooling(output, attention_mask)
        else:
            output = self.encoder(input_ids, attention_mask)['pooler_output']

        if self.with_proj:
            # Only apply projection if it is requested
            output = torch.tanh(self.transform(output))

        output = F.normalize(output, dim=-1)

        # calculating supcon loss would need different structure of batches here, so just setting to 0 since this is only inference model anyway
        loss = 0

        return (loss, output)