import copy
import logging
import json
import os
import itertools

from src.model.evidence import Evidence


def load_query_table(raw_json):
    # Load and initialize verified evidences
    logger = logging.getLogger()

    verified_evidences = []
    for raw_evidence in raw_json['verifiedEvidences']:
        context = None
        if 'context' in raw_evidence:
            context = raw_evidence['context']

        evidence = Evidence(raw_evidence['id'], raw_evidence['queryTableId'], raw_evidence['entityId'],
                            raw_evidence['value'], raw_evidence['table'], raw_evidence['rowId'],
                            raw_evidence['attribute'], context)

        if 'scale' in raw_evidence:
            evidence.scale = raw_evidence['scale']

        if 'signal' in raw_evidence:
            evidence.verify(raw_evidence['signal'])
            if 'scale' not in raw_evidence:
                evidence.determine_scale(raw_json['table'])

        if 'cornerCase' in raw_evidence:
            evidence.corner_case = raw_evidence['cornerCase']

        if evidence.query_table_id == raw_json['id'] and evidence not in verified_evidences:
            verified_evidences.append(evidence)
        elif evidence in verified_evidences:
            logger.warning('Evidence: {} already contained in query table {}'.format(evidence, raw_json['id']))
        else:
            logger.warning('Evidence: {} does not belong to query table {}'.format(evidence, raw_json['id']))

    return QueryTable(raw_json['id'], raw_json['assemblingStrategy'],
                      raw_json['useCase'], raw_json['category'],
                      raw_json['schemaOrgClass'], raw_json['requirements'],
                      raw_json['contextAttributes'], raw_json['targetAttribute'],
                      raw_json['table'], verified_evidences)


def load_query_table_from_file(path):
    """Load query table from provided path and return new Querytable object"""
    logger = logging.getLogger()

    with open(path) as gsFile:
        logger.info('Load query table from ' + path)
        querytable = load_query_table(json.load(gsFile))
        if type(querytable) is not QueryTable:
            logger.warning('Not able to load query table from {}'.format(path))
        return querytable


def load_query_tables():
    """Load all query tables"""
    query_tables = []
    for query_table_path in get_all_query_table_paths():
        query_tables.append(load_query_table_from_file(query_table_path))

    return query_tables


def load_query_tables_by_class(schema_org_class):
    """Load all query tables of a specific query table"""
    query_tables = []
    for category in get_categories(schema_org_class):
        for query_table_path in get_query_table_paths(schema_org_class, category):
            query_tables.append(load_query_table_from_file(query_table_path))

    return query_tables


def get_schema_org_classes():
    """Get a list of all schema org classes"""
    schema_org_classes = []
    path_to_classes = '{}/querytables/'.format(os.environ['DATA_DIR'])
    if os.path.isdir(path_to_classes):
        schema_org_classes = [schema_org_class for schema_org_class in os.listdir(path_to_classes)
                              if schema_org_class != 'deprecated' and 'test' not in schema_org_class]

    return schema_org_classes


def get_categories(schema_org_class):
    """Get list of categories by schema org"""
    categories = []
    path_to_categories = '{}/querytables/{}/'.format(os.environ['DATA_DIR'], schema_org_class)
    if os.path.isdir(path_to_categories):
        categories = [category for category in os.listdir(path_to_categories) if category != 'deprecated']

    return categories


def get_query_table_paths(schema_org_class, category):
    """Get query table paths"""
    query_table_files = []
    path_to_query_tables = '{}/querytables/{}/{}/'.format(os.environ['DATA_DIR'], schema_org_class, category)

    if os.path.isdir(path_to_query_tables):
        # Filter for json files
        query_table_files = ['{}{}'.format(path_to_query_tables, filename) for filename in os.listdir(path_to_query_tables)
                             if '.json' in filename]

    return query_table_files


def get_all_query_table_paths():
    query_table_paths = []
    for schema_org_class in get_schema_org_classes():
        for category in get_categories(schema_org_class):
            query_table_paths.extend(get_query_table_paths(schema_org_class, category))

    return query_table_paths


def create_context_attribute_permutations(querytable):
    """Create all possible query table permutations based on the context attributes of the provided query table"""
    permutations = []
    for i in range(len(querytable.context_attributes)):
        permutations.extend(itertools.permutations(querytable.context_attributes, i))

    # Remove permutations that do not contain name attribute
    permutations = [permutation for permutation in permutations if 'name' in permutation
                    and permutation != querytable.context_attributes]

    querytables = []
    for permutation in permutations:
        new_querytable = copy.deepcopy(querytable)
        removable_attributes = [attr for attr in new_querytable.context_attributes if attr not in permutation]
        for attr in removable_attributes:
            new_querytable.remove_context_attribute(attr)
        querytables.append(new_querytable)

    return querytables



class QueryTable:

    def __init__(self, identifier, assembling_strategy, use_case, category, schema_org_class, requirements,
                 context_attributes, target_attribute, table, verified_evidences):
        self.identifier = identifier
        self.assembling_strategy = assembling_strategy
        self.use_case = use_case
        self.category = category
        self.schema_org_class = schema_org_class
        self.requirements = requirements
        self.context_attributes = context_attributes
        self.target_attribute = target_attribute
        self.table = table
        self.verified_evidences = verified_evidences

    def __str__(self):
        return self.to_json(with_evidence_context=False)

    def to_json(self, with_evidence_context):
        encoded_evidence = {}
        # Camelcase encoding for keys and fill encoded evidence
        for key in self.__dict__.keys():
            if key == 'identifier':
                encoded_evidence['id'] = self.__dict__['identifier']
            elif key == 'verified_evidences':
                encoded_evidence['verifiedEvidences'] = [evidence.to_json(with_evidence_context) for evidence in
                                                         self.verified_evidences]
            else:
                camel_cased_key = ''.join([key_part.capitalize() for key_part in key.split('_')])
                camel_cased_key = camel_cased_key[0].lower() + camel_cased_key[1:]
                encoded_evidence[camel_cased_key] = self.__dict__[key]

        return encoded_evidence

    def no_known_positive_evidences(self, entity_id):
        """Calculate number of know positive evidences"""
        return sum([1 for evidence in self.verified_evidences
                    if evidence.signal and evidence.entity_id == entity_id])

    def has_verified_evidences(self):
        return len(self.verified_evidences) > 0

    def save(self, with_evidence_context):
        """Save query table on disk"""
        logger = logging.getLogger()
        category = self.category.lower().replace(" ", "_")
        file_name = 'gs_querytable_{}_{}_{}.json'.format(category, self.target_attribute, self.identifier)
        path_to_query_table = '{}/querytables/{}/{}/{}'.format(os.environ['DATA_DIR'], self.schema_org_class,
                                                              category, file_name)

        # Save query table to file
        with open(path_to_query_table, 'w', encoding='utf-8') as f:
            json.dump(self.to_json(with_evidence_context), f, indent=2)
            logger.info('Save query table {}'.format(file_name))

    def calculate_evidence_statistics_of_row(self, entity_id):
        """Export Query Table Statistics per entity"""
        row = [row for row in self.table if row['entityId'] == entity_id].pop()

        evidences = sum([1 for evidence in self.verified_evidences
                         if evidence.entity_id == row['entityId']])
        correct_value_entity = sum([1 for evidence in self.verified_evidences
                                    if evidence.entity_id == row['entityId'] and evidence.scale == 3])
        rel_value_entity = sum([1 for evidence in self.verified_evidences
                                if evidence.entity_id == row['entityId'] and evidence.scale == 2])
        correct_entity = sum([1 for evidence in self.verified_evidences
                              if evidence.entity_id == row['entityId'] and evidence.scale == 1])
        not_correct_entity = sum([1 for evidence in self.verified_evidences
                                  if evidence.entity_id == row['entityId'] and evidence.scale == 0])

        return evidences, correct_value_entity, rel_value_entity, correct_entity, not_correct_entity

    def remove_context_attribute(self, attribute):
        """Remove specified context attribute"""
        logger = logging.getLogger()
        if attribute == 'name':
            raise ValueError('It is not allowed to remove the name attribute from the query table!')

        if attribute not in self.context_attributes:
            raise ValueError('Context attribute {} not found in query table {}!'.format(attribute, self.identifier))

        if attribute in self.context_attributes:
            for row in self.table:
                try:
                    del row[attribute]
                except KeyError as e:
                    logger.warning('Keyerrror {}: Identifier {} - Category {}'.format(attribute, self.identifier, self.category))

            self.context_attributes.remove(attribute)
            logger.debug('Removed context attribute {} from querytable {}!'.format(attribute, self.identifier))

    def add_verified_evidence(self, evidence):
        logger = logging.getLogger()
        if evidence.query_table_id != self.identifier:
            logger.warning('Evidence {} does not belong to query table {}!'.format(evidence.identifier, self.identifier))
        else:
            self.verified_evidences.append(evidence)

    def normalize_query_table_numbering(self):
        # Change numbering of entities in query table
        # i = 1000  # Move entity ids to value range that is not used
        # for row in self.table:
        #     if row['entityId'] != i:
        #         relevant_evidences = [evidence for evidence in self.verified_evidences
        #                               if evidence.entity_id == row['entityId']]
        #         for evidence in relevant_evidences:
        #             evidence.entity_id = i
        #         row['entityId'] = i
        #     i += 1

        i = 0
        for row in self.table:
            if row['entityId'] != i:
                relevant_evidences = [evidence for evidence in self.verified_evidences
                                      if evidence.entity_id == row['entityId']]
                for evidence in relevant_evidences:
                    evidence.entity_id = i
                row['entityId'] = i
            i += 1

        # Change numbering of evidences in query table
        i = 0
        #self.verified_evidences = list(set(self.verified_evidences))
        for evidence in self.verified_evidences:
            if evidence.identifier != i:
                evidence.identifier = i
            i += 1
