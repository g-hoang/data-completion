from unittest import TestCase

import yaml

from src.strategy.pipeline_building import validate_configuration, build_pipelines_from_configuration


class Test(TestCase):
    def test_build_pipelines_from_configuration(self):
        # Setup
        path_to_configurations = 'config/test_experiments'
        path_to_config_1 = '{}/test_experiment_1.yml'.format(path_to_configurations)
        with open(path_to_config_1) as f:
            config_1 = yaml.load(f, Loader=yaml.FullLoader)

        # Test 1 - Check correct pipelines are built!
        pipelines = build_pipelines_from_configuration(config_1)
        self.assertEqual(len(pipelines), 7)

        count_gold_standard = 0
        for pipeline in pipelines:
            if pipeline['retrieval_strategy']['name'] == 'query_by_goldstandard':
                count_gold_standard += 1

            pipeline_steps = ['retrieval_strategy', 'similarity_re_ranking_strategy', 'source_re_ranking_strategy',
                              'voting_strategies']
            for step in pipeline_steps:
                self.assertIn(step, pipeline)

        self.assertEqual(count_gold_standard, 1)


    def test_validate_configuration(self):
        # Setup
        path_to_configurations = 'config/test_experiments'
        path_to_config_1 = '{}/test_experiment_1.yml'.format(path_to_configurations)
        with open(path_to_config_1) as f:
            config_1 = yaml.load(f, Loader=yaml.FullLoader)

        path_to_config_2 = '{}/test_experiment_2.yml'.format(path_to_configurations)
        with open(path_to_config_2) as f:
            config_2 = yaml.load(f, Loader=yaml.FullLoader)

        path_to_config_3 = '{}/test_experiment_3.yml'.format(path_to_configurations)
        with open(path_to_config_3) as f:
            config_3 = yaml.load(f, Loader=yaml.FullLoader)

        # Test 1 - Correct configuration
        self.assertTrue(validate_configuration(config_1))

        # Test 2 - Invalid configuration - Missing source-re-ranking-strategies
        with self.assertRaises(ValueError) as context:
            validate_configuration(config_2)
            self.assertEqual(context.exception, ValueError('Configuration for pipelines - source-re-ranking-strategies missing'))

        # Test 3 - Invalid configuration - Missing schema_org_class
        with self.assertRaises(ValueError) as context:
            validate_configuration(config_3)
            self.assertEqual(context.exception, ValueError('Configuration for general - schema_org_class missing'))
