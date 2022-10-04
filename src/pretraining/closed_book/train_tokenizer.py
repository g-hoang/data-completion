import logging
import os

import click
from datasets import load_dataset

from t5_tokenizer_model import SentencePieceUnigramTokenizer
from transformers import T5Config

@click.command()
@click.option('--schema_org_class')
def train_tokenizer(schema_org_class):
    vocab_size = 32_000
    input_sentence_size = None

    # Initialize a dataset
    dataset = load_dataset('text', data_files='{}/pretraining/{}_pretraining_train.txt'.format(os.environ['DATA_DIR'],
                                                                                 schema_org_class))

    tokenizer = SentencePieceUnigramTokenizer(unk_token="<unk>", eos_token="</s>", pad_token="<pad>")


    # Build an iterator over this dataset
    def batch_iterator(input_sentence_size=None):
        if input_sentence_size is None:
            input_sentence_size = len(dataset)
        batch_length = 100
        for i in range(0, input_sentence_size, batch_length):
            yield dataset['train'][i: i + batch_length]["text"]


    # Train tokenizer
    tokenizer.train_from_iterator(
        iterator=batch_iterator(input_sentence_size=input_sentence_size),
        vocab_size=vocab_size,
        show_progress=True,
    )

    # Save tokenizer files to disk
    model_path = '{}/models/closed_book/pretrained_{}_t5-v1_1-small'.format(os.environ['DATA_DIR'],schema_org_class)
    tokenizer_path = '{}/tokenizer.json'.format(model_path)
    tokenizer.save(tokenizer_path)

    # Save model to disc
    config = T5Config.from_pretrained("google/t5-v1_1-small", vocab_size=tokenizer.get_vocab_size())
    config.save_pretrained(model_path)

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    train_tokenizer()