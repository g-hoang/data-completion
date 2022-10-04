import logging
import math
import os

import click
import pandas as pd
from transformers import Trainer, TrainingArguments, BertTokenizerFast, \
    BertConfig

from src.finetuning.SimilarityDataset import IterableSimilarityDataset
from src.finetuning.bert_for_dpr_sim import BertForDPRSimilarity


@click.command()
@click.option('--schema_org_class')
@click.option('--model_name')
@click.option('--pooling')
def finetune_dpr(schema_org_class, model_name, pooling):
    """Run finetuning using dprs in-batch training"""
    logger = logging.getLogger()

    config = BertConfig.from_pretrained(model_name)
    config.pooling = pooling

    tokenizer = BertTokenizerFast.from_pretrained(model_name)
    model = BertForDPRSimilarity.from_pretrained(model_name, config=config)

    if schema_org_class not in model_name:
        # Add special tokens
        special_tokens_dict = {'additional_special_tokens': ['[COL]', '[VAL]']}
        num_added_toks = tokenizer.add_special_tokens(special_tokens_dict)
        model.resize_token_embeddings(len(tokenizer))
        logger.info('Added special tokens [COL], [VAL]')

    # Read data sets line by line (2 subsequent lines describe the same entity
    logging.info("Read Movie dataset")

    movie_dataset_path = '{}fine-tuning/movie_fine-tuning_dpr.csv'.format(os.environ['DATA_DIR'])

    df_dpr = pd.read_csv(movie_dataset_path, sep=';', encoding='utf-8')
    df_train_dpr = df_dpr[df_dpr['split'] == 'train']
    df_dev_dpr = df_dpr[df_dpr['split'] == 'dev']

    train_encodings = tokenizer(list(df_train_dpr['entity_str'].values), truncation=True, padding=True)
    dev_encodings = tokenizer(list(df_dev_dpr['entity_str'].values), truncation=True, padding=True)

    train_dataset = IterableSimilarityDataset(train_encodings, df_train_dpr['cluster'].values, df_train_dpr['match_index'].values)
    val_dataset = IterableSimilarityDataset(dev_encodings, df_dev_dpr['cluster'].values, df_dev_dpr['match_index'].values)

    num_epochs = 5
    warmup_steps = math.ceil(len(df_train_dpr) * num_epochs * 0.1)
    model_save_path = '{}/models/finetuned_cls_dpr_{}'.format(os.environ['DATA_DIR'], model_name)
    training_args = TrainingArguments(
        output_dir=model_save_path,          # output directory
        num_train_epochs=num_epochs,              # total number of training epochs
        per_device_train_batch_size=8,  # batch size per device during training
        per_device_eval_batch_size=8,   # batch size for evaluation
        warmup_steps=warmup_steps,                # number of warmup steps for learning rate scheduler
        weight_decay=0.01,               # strength of weight decay
        logging_dir='./logs',            # directory for storing logs
        logging_steps=10
    )



    trainer = Trainer(
        model=model,                         # the instantiated 🤗 Transformers model to be trained
        #data_collator=similarity_data_collator,
        args=training_args,                  # training arguments, defined above
        train_dataset=train_dataset,         # training dataset
        eval_dataset=val_dataset             # evaluation dataset
    )

    trainer.train()

if __name__ == "__main__":
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    finetune_dpr()
