# Exploring the potential of Seq2Seq models for the data completion task

### Setup

To retrieve the data sets run the following script:

`./scripts/setup_datasets.sh`

Make sure that you have access to a running Elastic Search instance before you experiment with the different Table Augmentation Strategies.
We use `docker-compose.yml` to spin up Elastic Search.
Please consult the [Elastic Search documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html) for further details.

### Index entities

Every entity of the table corpus is index as a separate entity into a single ES index. Command to index entities:

```
./scripts/load_data_into_elastic.sh
```

### Generating training data for seq2seq models
Please give your own configuration in the config files in `./scripts/configs/` and run the following script:
- `es_instance`: Address of VMware running Elastic Search
- `schema_org_class`: the schema.org class of the table corpus
- `training_data_type` can be `linear` or `qa` (question answering)
- `context-attributes` and `target-attributes` are the attributes of the table that you want to use as context and target attributes respectively.

```
./scripts/generate_training_data.sh
```
After running the above script, you will find the training data in the following location:
`./data/fine-tuning/closed_book/{training_data_type}/{schema_org_class}_finetuning_train.json`

### Finetune seq2seq models

Please give further configuration in the bash file `./scripts/finetune_seq2seq.sh`. Important parameters are:

- `$OUTPUT` is the directory here for storing checkpoints
- `$TRAIN_FILE` is the training data file that are generated in the previous step.

### Run experiment (predicting missing values)

- Before running experiment on seq2seq models, please set up a directory containing 3 required files for seq2seq models: `config.json`, `pytorch_model.bin` and `tokenizer.json`.

- A recommended directory: `./data/models/closed-book/{schema_org_class}/{training_data_type}/`

- For running experiment, please run the script `./scripts/run_experiment.sh` with the equivalent config file located inside `./scripts/configs/` directory. The `model_name` parameter should be set as a list of path to directories containing the models

- The default setting for the config file will run all query tables inside schema_org_class. If you want to run a specific query table (e.g., running experiment to compare performance with TURL), please specify the `path-to-query-table` parameter in the config file as the query table's location in string.

- The query table for comparing with TURL is located at: `./data/querytables/movie/gs_querytable_turl_director_500.json`

- Checkpoint are available [here](https://stneuedu-my.sharepoint.com/:f:/g/personal/11120956_st_neu_edu_vn/EhnwSet9lb5IvWbuU_t2spQBCvC3pgoWKd5BtzXfkZJwOQ?e=IXhgZ7)