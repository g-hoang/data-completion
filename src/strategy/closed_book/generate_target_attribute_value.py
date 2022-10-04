import logging
import os
import torch
import math

from transformers import AutoTokenizer, AutoModelForSeq2SeqLM

from src.model.evidence import Evidence
from src.strategy.open_book.retrieval.retrieval_strategy import RetrievalStrategy


def create_source_sequence(entity, target_attribute, schema_org_class):
    if schema_org_class == 'movie':
        identifying_attributes = ['name', 'director', 'duration', 'datepublished']
    elif schema_org_class == 'localbusiness':
        # Focus on Locality and name for now
        #identifying_attributes = ['addresslocality', 'addressregion', 'addresscountry', 'postalcode', 'name', 'streetaddress']
        identifying_attributes = ['name', 'addresslocality']
    else:
        logging.warning(
            'Identifying attributes are not defined for schema org class {}'.format(schema_org_class))

    # No prefix for now, because of the focus on one task --> table augmentation
    #prefix = "table augmentation: "
    encoded_entity = ''
    for identifying_attribute in identifying_attributes:
        if identifying_attribute != target_attribute \
                and identifying_attribute in entity:
            encoded_entity = "{}[COL]{}[VAL]{}".format(encoded_entity, identifying_attribute,
                                                       entity[identifying_attribute])
    source = "{}. target:[COL]{}".format(encoded_entity, target_attribute)
    return source

def create_natural_question(entity, target_attribute, context_attributes):
    # Generate natural language question
    # Who be director of Harry Potter with duration be PT2H32M and datepublished be 2001-11-16 (for human)
    # When be datepublished of Harry Potter with duration be PT2H32M and director be Chris Columbus (for datetime format)
    # Where be addresslocality of Hyatt Paris Madeleine with telephone be +33155271234 and postalcode be 75008 (For location features)
    # How be duration of Harry Potter with director be Chris Columbus and datepublished be 2001-11-16 (for duration or ratings?)
    # What be telephone of Hyatt Paris Madeleine with addresslocality be Paris and postalcode be 75008 and streetaddress be 24 Boulevard Malesherbes, 75008 (for others)

    prefix = get_question_type(target_attribute)
    encoded_entity = f"{prefix} be {target_attribute}"
    if 'name' in entity: encoded_entity = f"{encoded_entity} of {entity['name']}"
    for id in range(len(context_attributes)):
        context_attribute = context_attributes[id]
        if context_attribute not in [target_attribute, 'name'] and context_attribute in entity:
            encoded_entity = f"{encoded_entity} and {context_attribute} be {entity[context_attribute]}"
    return encoded_entity

def create_source_sequence2(entity, target_attribute, context_attributes, fact=''):
    encoded_entity = ''
    for context_attribute in context_attributes:
        if context_attribute != target_attribute and context_attribute in entity:
            encoded_entity = "{}[COL]{}[VAL]{}".format(encoded_entity, context_attribute,
                                                       entity[context_attribute])
    source = "{}{}. target:[COL]{}".format(encoded_entity, fact, target_attribute)
    return source

def get_question_type(attribute):
    # Get question Type of provided attribute
    attributes_question_pairs = {'datepublished': 'When',
                       'duration': 'How',
                       'director': 'Who'}
    list_of_location_attributes = ['latitude', 'longitude', 'addresscountry', 'addresslocality', 'addressregion', 'postalcode',
                                    'streetaddress' ]

    if attribute in list_of_location_attributes:
        return 'Where'
    elif attribute in attributes_question_pairs:
        return attributes_question_pairs[attribute]
    else:
        return 'What'

class TargetAttributeValueGenerator(RetrievalStrategy):

    def __init__(self, schema_org_class, model_name, training_data_type='origin'):
        super().__init__(schema_org_class, 'generate_entity')

        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        #self.device = 'cpu'
        self.model_name = model_name
        model_path = '{}/models/closed_book/{}'.format(os.environ['DATA_DIR'], model_name)
        self.tokenizer = AutoTokenizer.from_pretrained(model_path)
        self.model = AutoModelForSeq2SeqLM.from_pretrained(model_path).to(self.device)
        self.training_data_type = training_data_type

    # Sequence generator
    def retrieve_evidence(self, query_table, evidence_count, entity_id):
        logger = logging.getLogger()
        evidence_id = 1
        evidences = []

        # Iterate through query table and create entity embeddings (neural representations)
        for row in query_table.table:
            source = ''
            if self.training_data_type == 'origin' or self.training_data_type == 'descriptive':
                source = create_source_sequence2(row, query_table.target_attribute, query_table.context_attributes)
                #print(self.tokenizer.tokenize(source))
            elif self.training_data_type == 'natural_questions':
                source = create_natural_question(row, query_table.target_attribute, query_table.context_attributes)
            else:
                logger.warning('Training data type is not defined')
                continue

            input_ids = self.tokenizer(source, return_tensors='pt').input_ids.to(self.device)
            outputs = self.model.generate(input_ids, return_dict_in_generate=True, output_scores=True, 
                num_return_sequences=5, num_beams=5) # evidence_count can be used here for beam search params?

            for i in range(len(outputs['sequences'])):
                sequence = outputs['sequences'][i]
                sequences_scores = outputs['sequences_scores'][i]
                decoded_value = self.tokenizer.decode(sequence, skip_special_tokens=True)

                if self.training_data_type == 'origin' or self.training_data_type == 'descriptive':
                    decoded_value = decoded_value.replace('[VAL]', '')

                evidence = Evidence(evidence_id, query_table.identifier, row['entityId'], decoded_value,
                            None, None, query_table.target_attribute, None)
                evidence.set_scores('sequence_scores', math.exp(sequences_scores))
                evidences.append(evidence)
            evidence_id += 1

        return evidences

    def re_rank_evidences(self, query_table, evidences):
        """Just return evidences for now"""

        return evidences
