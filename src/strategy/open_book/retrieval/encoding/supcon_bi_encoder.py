import logging
import os

import numpy as np
import random
import torch
from transformers import AutoTokenizer, AutoModel

from src.finetuning.open_book.contrastive.models.modeling import ContrastiveModel
from src.strategy.open_book.retrieval.encoding.bi_encoder import BiEncoder


class SupConBiEncoder(BiEncoder):
    def __init__(self, model_name, base_model, with_projection, pooling, normalize, schema_org_class):
        """Initialize Entity Biencoder"""
        super().__init__(schema_org_class)

        # Make results reproducible
        seed = 42
        np.random.seed(seed)
        random.seed(seed)
        torch.manual_seed(seed)
        torch.cuda.manual_seed_all(seed)

        self.pooling = pooling
        self.normalize = normalize

        # Initialize tokenizer and model for BERT if necessary
        if model_name is not None:
            self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
            model_path = '{}/models/open_book/{}'.format(os.environ['DATA_DIR'], model_name)
            if not os.path.isdir(model_path):
                # Try to load model from huggingface - enhance model and save locally
                tokenizer = AutoTokenizer.from_pretrained(model_name)
                model = AutoModel.from_pretrained(model_name)
                # Add special tokens - inspired by Dito - Li et al. 2020
                special_tokens_dict = {'additional_special_tokens': ['[COL]', '[VAL]']}
                num_added_toks = tokenizer.add_special_tokens(special_tokens_dict)
                model.resize_token_embeddings(len(tokenizer))

                # Cache model and tokenizer locally --> reduce number of calls to huggingface
                model.save_pretrained(model_path)
                tokenizer.save_pretrained(model_path)

            self.tokenizer = AutoTokenizer.from_pretrained(model_path)
            # Note: Normalization + pooling happen in the model
            self.model = ContrastiveModel(len_tokenizer=len(self.tokenizer), model=base_model, with_proj=with_projection).to(self.device)
            self.model.load_state_dict(torch.load('{}/pytorch_model.bin'.format(model_path), map_location=torch.device(self.device)))

    def encode_entity(self, entity, excluded_attributes=None):
        """Encode the provided entity"""
        entity_str = self.entity_serializer.convert_to_str_representation(entity, excluded_attributes)

        inputs = self.tokenizer(entity_str, return_tensors='pt', padding=True,
                                truncation=True, max_length=128).to(self.device)

        with torch.no_grad():
            outputs = self.model(input_ids=inputs['input_ids'], attention_mask=inputs['attention_mask'])

        return inputs, outputs[1]

    def encode_entities(self, entities, excluded_attributes=None):
        """Encode the provided entities"""
        entity_strs = [self.entity_serializer.convert_to_str_representation(entity, excluded_attributes)
                       for entity in entities]

        inputs = self.tokenizer(entity_strs, return_tensors='pt', padding=True,
                                truncation=True, max_length=128).to(self.device)

        with torch.no_grad():
            outputs = self.model(input_ids=inputs['input_ids'], attention_mask=inputs['attention_mask'])

        return inputs, outputs[1]

    def encode_entities_and_return_pooled_outputs(self, entity, excluded_attributes=None):
        inputs, outputs = self.encode_entities(entity, excluded_attributes)

        return outputs.squeeze().tolist()
