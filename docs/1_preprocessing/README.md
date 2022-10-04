# Language detection

To make sure the language of the table is English, we use [a pretrained model](https://fasttext.cc/docs/en/language-identification.html) to determine whether the table language is English or not. `lid.176.bin` is selected, and it should be downloaded beforehand and saved into `$DATA_DIR/models/fasttext`.
