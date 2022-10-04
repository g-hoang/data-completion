export CUDA_VISIBLE_DEVICES=1
export CONFIG_FILE=/work/ghoang/table-augmentation-framework/scripts/config/experiment_movie.yml  # Generate training data for movie query table
export DATA_DIR=/work/ghoang/table-augmentation-framework/data
export ES_INSTANCE='wifo5-35.informatik.uni-mannheim.de'

python -m src.finetuning.closed_book.generate_finetuning_data --path_to_config=$CONFIG_FILE &> generate_finetuning_data.log &
