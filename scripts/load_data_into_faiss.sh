#!/bin/bash

export ES_INSTANCE=wifo5-33.informatik.uni-mannheim.de
export DATA_DIR=/work-ceph/alebrink/tableAugmentation/data/
export PYTHONPATH=/home/alebrink/development/table-augmentation-framework
export CUDA_VISIBLE_DEVICES=1
export SCHEMA_ORG_CLASS=movie
export MODEL_NAME=bert-base-uncased

python ./src/strategy/open_book/indexing/index_faiss_entity.py --schema_org_class=$SCHEMA_ORG_CLASS --model_name=$MODEL_NAME


