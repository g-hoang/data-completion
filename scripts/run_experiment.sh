#!/bin/bash

export ES_INSTANCE='wifo5-35.informatik.uni-mannheim.de'
export DATA_DIR=/work/ghoang/table-augmentation-framework/data

# Depends on what experiment you want to run
# Predicting movie query table: --path_to_config=/work/ghoang/table-augmentation-framework/scripts/config/experiment_movie.yml
# Predicting localbusiness query table: --path_to_config=/work/ghoang/table-augmentation-framework/scripts/config/experiment_localbusiness.yml
# Run baseline: --path_to_config=/work/ghoang/table-augmentation-framework/scripts/config/experiment_baseline.yml
# Predicting movie and localbusiness using seq2seq models requires further configuration in the config files

python -m src.strategy.run_strategy --path_to_config=/work/ghoang/table-augmentation-framework/scripts/config/experiment_movie.yml
