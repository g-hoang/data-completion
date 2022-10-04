# Environment

## The table corpus
We use the [`WDC table-corpus`](http://webdatacommons.org/structureddata/schemaorgtables/). The table corpus is a collection of tables that are structured according to the [Schema.org](http://schema.org/) vocabulary.

An example bash script is setup for Movie datasets located at `scripts/setup_datasets.sh`. Ideally, you can configure that the script will download the table corpus to `/data/corpus/` folder.

## Open-book strategy
It requires ElasticSearch up and running. You can direcly start the ElasticSearch service by using the docker-compose file.

After that, you need to indexing the dataset using the bash script `scripts/load_data_into_elastic.sh`. Please maintain the environment variables on your own machine.

## Closed-book strategy
- The sequence to sequence (seq2seq) approach relies on pytorch and transformers library. The specific version defined in `requirement.txt` is set for CUDA 11.6, but feel free to adjust it according to your hardware specifications.
