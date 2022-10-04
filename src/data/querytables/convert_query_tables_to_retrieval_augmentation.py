import logging
import os

import click

from src.model.evidence_new import RetrievalEvidence, AugmentationEvidence
from src.model.querytable import get_categories, get_query_table_paths, load_query_tables_by_class
from src.model.querytable_new import RetrievalQueryTable, AugmentationQueryTable

@click.command()
@click.option('--schema_org')
def load_and_convert_query_tables(schema_org):
    """Load query tables and convert into retrieval & augmentation query tables"""

    # Load old query tables
    old_query_tables = load_query_tables_by_class(schema_org)
    # Determine new retrieval ids using assembling strategy
    assembling_strategies = set()
    target_attributes = set()
    gt_tables = set()
    for query_table in old_query_tables:
        assembling_strategies.add(query_table.assembling_strategy)
        target_attributes.add(query_table.target_attribute)
        gt_tables.add(query_table.category)

    retrieval_ids = {}
    current_retrieval_id = 100
    for assembling_strategy in assembling_strategies:
        retrieval_ids[assembling_strategy] = current_retrieval_id
        current_retrieval_id += 100

    gt_table_ids = {}
    current_gt_table_id = 100
    for gt_table in gt_tables:
        gt_table_ids[gt_table] = current_gt_table_id
        current_gt_table_id += 100

    target_attribute_ids = {}
    current_target_attribute_id = 1
    for target_attribute in target_attributes:
        target_attribute_ids[target_attribute] = current_target_attribute_id
        current_target_attribute_id += 1

    new_retrieval_query_tables = []
    existing_retrival_querytable_ids = []
    new_augmentation_query_tables = []
    for query_table in old_query_tables:
        if retrieval_ids[query_table.assembling_strategy] not in existing_retrival_querytable_ids:
            retrieval_id = retrieval_ids[query_table.assembling_strategy]
            new_verified_retrieval_evidences = []
            for evidence in query_table.verified_evidences:
                retrival_evidence = RetrievalEvidence(evidence.identifier, retrieval_id, evidence.entity_id,
                                                      evidence.table, evidence.row_id, evidence.context)
                retrival_evidence.scale = evidence.scale
                retrival_evidence.signal = evidence.signal
                retrival_evidence.corner_case = evidence.corner_case
                new_verified_retrieval_evidences.append(retrival_evidence)

            new_retrieval_query_table = RetrievalQueryTable(retrieval_id, 'retrieval', query_table.assembling_strategy,
                          query_table.category,
                          query_table.schema_org_class,
                          query_table.context_attributes,
                          query_table.table.copy(), new_verified_retrieval_evidences)

            new_retrieval_query_tables.append(new_retrieval_query_table)
            existing_retrival_querytable_ids.append(retrieval_ids[query_table.assembling_strategy])

        # Determine augmentations ids using retrieval ids +  target attribute id
        augmentation_id = retrieval_ids[query_table.assembling_strategy] + target_attribute_ids[query_table.target_attribute]
        new_verified_augmentation_evidences = []
        for evidence in query_table.verified_evidences:
            augmentation_evidence = AugmentationEvidence(evidence.identifier, augmentation_id,
                                 evidence.entity_id, evidence.table, evidence.row_id,
                                 evidence.context, evidence.value, evidence.attribute)
            augmentation_evidence.scale = evidence.scale
            augmentation_evidence.signal = evidence.signal
            augmentation_evidence.corner_case = evidence.corner_case
            new_verified_augmentation_evidences.append(augmentation_evidence)

        new_augmentation_query_table = AugmentationQueryTable(augmentation_id, 'augmentation',
                                                              query_table.assembling_strategy,
                                                        query_table.category,
                                                        query_table.schema_org_class,
                                                        query_table.context_attributes,
                                                        query_table.table.copy(), new_verified_augmentation_evidences,
                                                        query_table.target_attribute,
                                                        query_table.use_case)
        new_augmentation_query_tables.append(new_augmentation_query_table)

    # Merge query tables
    new_consolidated_retrieval_query_tables = {}
    new_consolidated_augmentation_query_tables = {}

    for query_table in new_retrieval_query_tables:
        retrieval_id = gt_table_ids[query_table.gt_table]
        if retrieval_id not in new_consolidated_retrieval_query_tables:
            # Update retrieval id of query table
            query_table.identifier = retrieval_id
            for evidence in query_table.verified_evidences:
                evidence.query_table_id = retrieval_id
            new_consolidated_retrieval_query_tables[retrieval_id] = query_table
        else:
            consolidated_query_table = new_consolidated_retrieval_query_tables[retrieval_id]
            consolidated_query_table.append(query_table)

    for query_table in new_augmentation_query_tables:
        augmentation_id = gt_table_ids[query_table.gt_table] + target_attribute_ids[
            query_table.target_attribute]
        if augmentation_id not in new_consolidated_augmentation_query_tables:
            # Update augmentation id of query table
            query_table.identifier = augmentation_id
            for evidence in query_table.verified_evidences:
                evidence.query_table_id = retrieval_id
            new_consolidated_augmentation_query_tables[augmentation_id] = query_table
        else:
            consolidated_query_table = new_consolidated_augmentation_query_tables[augmentation_id]
            consolidated_query_table.append(query_table)


    # Save query tables
    for query_table in new_consolidated_retrieval_query_tables.values():
        query_table.save(with_evidence_context=False)

    for query_table in new_consolidated_augmentation_query_tables.values():
        query_table.save(with_evidence_context=False)

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    logger = logging.getLogger()

    # Load environmental parameters
    path_to_data = os.environ['DATA_DIR']

    query_table_goldstandard = load_and_convert_query_tables()

    logger.info('Corrected query tables!')
