#!/bin/bash

export ES_INSTANCE=wifo5-35.informatik.uni-mannheim.de
export DATA_DIR=/work/ghoang/table-augmentation-framework/data/
export CUDA_VISIBLE_DEVICES=0
export SCHEMA_ORG_CLASS=movie

python -m src.strategy.open_book.indexing.index_es_entity --schema_org_class=$SCHEMA_ORG_CLASS --worker=10  &> load_data_into_es_$SCHEMA_ORG_CLASS.log &


