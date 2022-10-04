# Generating training data for seq2seq model

A seq2seq model takes a sequence as input and produce another sequence as an output. Therefore, for the training data, we define a set of `source` for inputs and their equivalent `target` as outputs. The data are created following this template:

```
{"table_augmentation": {"source": "[COL]name[VAL]The Puppet Masters[COL]feature1[VAL]feature1_value. target:[COL]target_attribute", "target": "[VAL]target_value"}}
```