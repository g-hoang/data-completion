export CUDA_VISIBLE_DEVICES=0,1
export CUDA_DEVICE_ORDER=PCI_BUS_ID
export OUTPUT=/work/ghoang/finetuning/closed_book/linear/movie/t5_base/ # directory here for storing checkpoints
export TRAIN_FILE=/work/ghoang/table-augmentation-framework/data/fine-tuning/closed_book/linear/movie_finetuning_train.json # training file location
export LOG_NAME=linear_movie_t5_base # name of log file

python -m torch.distributed.launch \
    --nproc_per_node 4 src/finetuning/closed_book/run_table_augmentation.py --per_device_train_batch_size 16 --num_train_epochs 80 \
    --model_name_or_path t5-base --output_dir=$OUTPUT --train_file=$TRAIN_FILE --save_steps=5000 \
    --do_train &> finetune_$LOG_NAME.log &