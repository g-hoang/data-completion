import logging
import os
import pickle

import click
import pandas as pd
import py_entitymatching as em

from src.strategy.open_book.ranking.similarity.magellan_re_ranker import determine_path_to_feature_table, \
    determine_path_to_model


@click.command()
@click.option('--schema_org_class')
def finetune_magellan(schema_org_class):
    """Run finetuning for magellan"""
    logger = logging.getLogger()
    logger.info('Selected schema org class {}'.format(schema_org_class))

    dataset_path = '{}/finetuning/open_book/{}_fine-tuning_subset_pairs.csv'.format(os.environ['DATA_DIR'],
                                                                                   schema_org_class)
    df_magellan = pd.read_csv(dataset_path, sep=';', encoding='utf-8')
    df_magellan['_id'] = range(0, len(df_magellan))
    df_magellan['entity1_id'] = range(0, len(df_magellan))
    df_magellan['entity2_id'] = range(0, len(df_magellan))
    # df_train_magellan = df_magellan[df_magellan['split'] == 'train']
    # df_dev_magellan = df_magellan[df_magellan['split'] == 'dev']

    #context_attribute_list = [['id', 'name', 'addresslocality', 'addressregion', 'addresscountry', 'postalcode',
    #                           'streetaddress'], ['id', 'name', 'addresslocality'], ['id', 'name']]
    if schema_org_class == 'localbusiness':
        context_attribute_list = [['id', 'name', 'addresslocality']]
    elif schema_org_class == 'product':
        context_attribute_list = [['id', 'name']]
    else:
        ValueError('Schema Org Class {} is unknown.'.format(schema_org_class))


    for context_attributes in context_attribute_list:

        logger.info('Selected context attributes: {}'.format([', '.join(context_attributes)]))

        # Determine Feature Table
        df_entity1 = df_magellan[['entity1_{}'.format(attr) for attr in context_attributes]]
        df_entity1.columns = [column.replace('entity1_', '') for column in df_entity1.columns]
        df_entity2 = df_magellan[['entity2_{}'.format(attr) for attr in context_attributes]]
        df_entity2.columns = [column.replace('entity2_', '') for column in df_entity2.columns]

        # Set ID
        em.set_key(df_entity1, 'id')
        em.set_key(df_entity2, 'id')
        em.set_key(df_magellan, '_id')

        # Set foreign key relationships
        em.set_ltable(df_magellan, df_entity1)
        em.set_fk_ltable(df_magellan, 'entity1_id')
        em.set_rtable(df_magellan, df_entity2)
        em.set_fk_rtable(df_magellan, 'entity2_id')

        atypes1 = em.get_attr_types(df_entity1)
        atypes2 = em.get_attr_types(df_entity2)

        block_c = em.get_attr_corres(df_entity1, df_entity2)
        block_c['corres'].remove(('id', 'id'))

        tok = em.get_tokenizers_for_matching()
        sim = em.get_sim_funs_for_matching()
        feature_table = em.get_features(df_entity1, df_entity2, atypes1, atypes2, block_c, tok, sim)
        #feature_table.to_csv('{}/magellan/tmp_feature_table.csv'.format(os.environ['DATA_DIR']), sep=';')

        # Save feature table for usage during prediction
        if not os.path.isdir('{}/magellan'.format(os.environ['DATA_DIR'])):
            os.mkdir('{}/magellan'.format(os.environ['DATA_DIR']))

        if 'id' in context_attributes:
            context_attributes.remove('id')

        feature_table_path = determine_path_to_feature_table(schema_org_class, context_attributes)
        em.save_object(feature_table, feature_table_path)
        logger.info('Saved Feature Table')

        # Use train and dev for training and evaluate using cross validation
        df_train_magellan = df_magellan[(df_magellan['split'] == 'train') | (df_magellan['split'] == 'dev')]
        em.set_key(df_train_magellan, '_id')
        df_test_magellan = df_magellan[df_magellan['split'] == 'test']
        em.set_key(df_test_magellan, '_id')

        # Set foreign key relationships
        em.set_ltable(df_train_magellan, df_entity1)
        em.set_fk_ltable(df_train_magellan, 'entity1_id')
        em.set_rtable(df_train_magellan, df_entity2)
        em.set_fk_rtable(df_train_magellan, 'entity2_id')

        df_train_feature_vector = em.extract_feature_vecs(df_train_magellan, feature_table=feature_table,
                                                          attrs_after='score',
                                                          show_progress=True)
        # Fill missing values with 0
        df_train_feature_vector.fillna(value=0, inplace=True)

        if len(df_test_magellan)> 0:
            em.set_ltable(df_test_magellan, df_entity1)
            em.set_fk_ltable(df_test_magellan, 'entity1_id')
            em.set_rtable(df_test_magellan, df_entity2)
            em.set_fk_rtable(df_test_magellan, 'entity2_id')

            df_test_feature_vector = em.extract_feature_vecs(df_test_magellan, feature_table=feature_table,
                                                              attrs_after='score',
                                                              show_progress=True)
            df_test_feature_vector.fillna(value=0, inplace=True)




        #dt = em.DTMatcher(name='DecisionTree', random_state=42)
        #svm = em.SVMMatcher(name='SVM', random_state=42)
        rf = em.RFMatcher(name='RF', random_state=42)
        #lg = em.LogRegMatcher(name='LogReg', random_state=42)
        #ln = em.LinRegMatcher(name='LinReg')

        #models = [dt, rf, lg, ln]
        models = [rf]
        # result = em.select_matcher(models, table=df_train_feature_vector,
        #                            exclude_attrs=['_id', 'entity1_id', 'entity2_id', 'score'],
        #                            k=5,
        #                            target_attr='score', metric_to_select_matcher='f1', random_state=0)

        # print(result['cv_stats'])

        for model in models:
            model.fit(table=df_train_feature_vector,
                      exclude_attrs=['_id', 'entity1_id', 'entity2_id', 'score'],
                      target_attr='score')
            save_model(model, schema_org_class, context_attributes)

            if len(df_test_magellan)> 0:
                predictions = evaluate_matcher(model, df_test_feature_vector)
                if predictions:
                    save_predictions(df_test_magellan, predictions, schema_org_class, model.name, context_attributes)

        # #evaluate_matcher(svm, schema_org_class, df_train_feature_vector, df_dev_feature_vector)
        # evaluate_matcher(rf, df_train_feature_vector, df_test_feature_vector)
        # save_predictions(df_test_magellan, predictions, schema_org_class, rf.name, context_attributes)
        #
        # evaluate_matcher(lg, df_train_feature_vector, df_test_feature_vector)
        # save_predictions(df_test_magellan, predictions, schema_org_class, lg.name, context_attributes)
        #
        # evaluate_matcher(ln, df_train_feature_vector, df_test_feature_vector)
        # save_predictions(df_test_magellan, predictions, schema_org_class, ln.name, context_attributes)


def evaluate_matcher(model, df_test_feature_vector):
    # Predict on test set
    if df_test_feature_vector:
        predictions = model.predict(table=df_test_feature_vector, exclude_attrs=['_id', 'entity1_id', 'entity2_id', 'score'],
                                    append=True, target_attr='predicted', inplace=False, return_probs=True,
                                    probs_attr='proba')

        print('Performance of model {} :'.format(model.name))
        eval_result = em.eval_matches(predictions, 'score', 'predicted')
        em.print_eval_summary(eval_result)
        print('')

        return predictions

    return None


def save_model(model, schema_org_class, context_attributes):
    # Save Model
    filepath_pickle = determine_path_to_model(model.name, schema_org_class, context_attributes)
    pickle.dump(model, open(filepath_pickle, 'wb'))
    logging.info('Model {} here: {}'.format(model.name, filepath_pickle))


def save_predictions(df_test_magellan, predictions, schema_org_class, model_name, context_attributes):
    # Save prediction
    df_test_magellan['pred'] = predictions['predicted']
    df_test_magellan['proba'] = predictions['proba']
    context_attribute_string = '_'.join([attr for attr in context_attributes])
    dataset_path = '{}/finetuning/open_book/magellan_results/{}_fine-tuning_magellan_{}_{}.csv'.format(os.environ['DATA_DIR'],
                                                                                      schema_org_class,
                                                                                      model_name,
                                                                                      context_attribute_string)
    df_test_magellan.to_csv(dataset_path, sep=';', encoding='utf-8')

if __name__ == "__main__":
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    finetune_magellan()
